# -*- coding: utf-8 -*-
"""
Pipeline unificado Madesa — Confirma Fácil → CSV → Google Sheets (com DE→PARA)
--------------------------------------------------------------------------------
O que faz:
1) Coleta "ENTREGUES" e "CANCELADOS" via API do Confirma Fácil (últimos N dias).
2) Exporta cada resultado para CSV (ENTREGUES.csv e CANCELADOS.csv).
3) Carrega o Excel de DE→PARA (Transportadora) e aplica o mapeamento.
4) Publica em Google Sheets, aba "ENTREGUES CF" (intervalo B:G).

Uso local:
  python madesa_cf_pipeline.py --run --lookback 30
Menu interativo (sem flags) também funciona localmente.

Em CI (GitHub Actions), rode com --run e defina variáveis de ambiente (secrets).
"""

import os
import csv
import json
import time
import logging
import warnings
import argparse
from pathlib import Path
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# =============================================================================
# CONFIG (podem ser sobrescritas por variáveis de ambiente)
# =============================================================================

# Confirma Fácil
BASE_URL = "https://utilities.confirmafacil.com.br"
LOGIN_URL = f"{BASE_URL}/login/login"
OCORR_URL = f"{BASE_URL}/filter/ocorrencia"

# >>> Recomendo usar sempre variáveis de ambiente (no Actions: Secrets) <<<
CF_EMAIL = os.getenv("CF_EMAIL", "")         # ex.: 'seu.email@madesa.com'
CF_SENHA = os.getenv("CF_SENHA", "")         # ex.: 'sua_senha_aqui'
CF_IDCLIENTE = int(os.getenv("CF_IDCLIENTE", "206"))
CF_IDPRODUTO = int(os.getenv("CF_IDPRODUTO", "1"))

# Códigos de ocorrências
CODES_ENTREGUES = "1,2,37,999"
CODES_CANCELADOS = "25,102,203,303,325,327"

LOOKBACK_DIAS = int(os.getenv("LOOKBACK_DIAS", "30"))

# Requests tuning
PAGE_SIZE = int(os.getenv("PAGE_SIZE", "1000"))
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "10"))
# timeout = (connect_timeout, read_timeout) — aumente o read_timeout
TIMEOUT = (int(os.getenv("CONNECT_TIMEOUT", "5")), int(os.getenv("READ_TIMEOUT", "120")))
TOTAL_RETRIES = int(os.getenv("TOTAL_RETRIES", "5"))
BACKOFF = float(os.getenv("BACKOFF", "1.0"))

# Pastas
OUTPUT_DIR = os.getenv("OUTPUT_DIR", str(Path.cwd() / "out"))
OUTPUT_NAME_ENTREGUES = "ENTREGUES"
OUTPUT_NAME_CANCELADOS = "CANCELADOS"
USE_CSV = True  # se False, exporta XLSX

# Google Sheets
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SHEET_ID = os.getenv("SHEET_ID", "")
SHEET_ID_EXTRA = os.getenv("SHEET_ID_EXTRA", "")
SHEET_RANGE_ENTREGUES = 'ENTREGUES CF!B2:G'
CREDENTIALS_PATH = os.getenv("GOOGLE_CREDENTIALS_PATH", str(Path.home() / "secrets" / "gsa.json"))

# DE→PARA
DEXPARA_XLSX_PATH = os.getenv("DEXPARA_XLSX_PATH", str(Path.cwd() / "data" / "DExPARA.xlsx"))
DEXPARA_SHEET = os.getenv("DEXPARA_SHEET", "TRANSPORTADORA")

# Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
warnings.simplefilter("ignore", UserWarning)


# =============================================================================
# Utils
# =============================================================================

def _fmt(segundos: float) -> str:
    ms = int((segundos - int(segundos)) * 1000)
    h = int(segundos) // 3600
    m = (int(segundos) % 3600) // 60
    s = int(segundos) % 60
    return f"{h:02d}:{m:02d}:{s:02d}.{ms:03d}"

def _norm(s: str) -> str:
    if s is None:
        return ""
    return str(s).strip().upper()

def format_value(value):
    if isinstance(value, datetime):
        return value.strftime('%Y-%m-%d')
    if value is None:
        return ''
    return str(value)

def format_data_iso_to_br(iso_str: str) -> str:
    if not iso_str:
        return ""
    try:
        return datetime.fromisoformat(iso_str).strftime("%d/%m/%Y")
    except ValueError:
        try:
            return datetime.strptime(
                iso_str.replace("T", " ").replace("Z", ""), "%Y-%m-%d %H:%M:%S"
            ).strftime("%d/%m/%Y")
        except Exception:
            return ""


# =============================================================================
# Confirma Fácil
# =============================================================================
def make_session(max_pool=20, total_retries=3, backoff=0.5):
    s = requests.Session()
    retries = Retry(
        total=total_retries,
        connect=total_retries,
        read=total_retries,
        backoff_factor=backoff,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset({"GET", "POST"}),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(pool_connections=max_pool, pool_maxsize=max_pool, max_retries=retries)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    # keep-alive + gzip
    s.headers.update({
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive",
        "User-Agent": "madesa-cf-pipeline/1.0"
    })
    return s

def autentificador(session: requests.Session) -> str:
    if not CF_EMAIL or not CF_SENHA:
        raise RuntimeError("Defina CF_EMAIL e CF_SENHA (variáveis de ambiente).")
    headers = {"Content-Type": "application/json"}
    payload = {
        "email": CF_EMAIL,
        "senha": CF_SENHA,
        "idcliente": CF_IDCLIENTE,
        "idproduto": CF_IDPRODUTO,
    }
    r = session.post(LOGIN_URL, headers=headers, data=json.dumps(payload), timeout=TIMEOUT)
    r.raise_for_status()
    token = r.json().get("resposta", {}).get("token")
    if not token:
        raise RuntimeError("Falha na autenticação: token não retornado")
    return token

def periodo_busca(dias: int):
    hoje = datetime.today()
    di = (hoje - timedelta(days=dias)).strftime("%d-%m-%Y")
    df = hoje.strftime("%d-%m-%Y")
    logging.info(f"Período: {di} até {df}")
    return di, df

def montar_params(di: str, df: str, page: int, codigos: str):
    return {
        "page": page,
        "size": PAGE_SIZE,
        "serie": "1",
        "de": datetime.strptime(di, "%d-%m-%Y").strftime("%Y/%m/%d 00:00:00"),
        "ate": datetime.strptime(df, "%d-%m-%Y").strftime("%Y/%m/%d 23:59:59"),
        "codigoOcorrencia": codigos,
        "tipoData": "OCORRENCIA",
    }

def fetch_page(session: requests.Session, token: str, params: dict):
    headers = {"Authorization": token, "accept": "application/json"}
    try:
        r = session.get(OCORR_URL, headers=headers, params=params, timeout=TIMEOUT)
        r.raise_for_status()
        return r.json().get("respostas", [])
    except requests.exceptions.RequestException as e:
        logging.warning(f"fetch_page falhou para pagina={params.get('page')}: {e}")
        return []

def consultar_ocorrencias(di: str, df: str, token: str, session: requests.Session, codigos: str):
    params0 = montar_params(di, df, page=0, codigos=codigos)
    r0 = session.get(OCORR_URL, headers={"Authorization": token, "accept": "application/json"}, params=params0, timeout=TIMEOUT)
    r0.raise_for_status()
    j0 = r0.json()
    total_pages = int(j0.get("totalPages", 0))
    resultados = j0.get("respostas", []) or []

    if total_pages <= 1:
        return resultados

    futures = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        for p in range(1, total_pages):
            pparams = montar_params(di, df, page=p, codigos=codigos)
            futures.append(ex.submit(fetch_page, session, token, pparams))
        for f in as_completed(futures):
            try:
                resultados.extend(f.result())
            except Exception as e:
                logging.warning(f"Falha ao obter uma página: {e}")
    return resultados

def extrair_dataframe(respostas):
    linhas = []
    for item in respostas:
        emb = item.get("embarque") or {}
        entregas = emb.get("entregas") or []
        data_entrega_raw = entregas[0].get("dataEntrega", "") if entregas else ""
        data_entrega = format_data_iso_to_br(data_entrega_raw)

        linhas.append({
            "NF": emb.get("numero", ""),
            "Transportadora": (emb.get("transportadora") or {}).get("nome", ""),
            "Chave": emb.get("chave", ""),
            "Pedido": (emb.get("pedido") or {}).get("numero", ""),
            "DataEntrega": data_entrega
        })
    df = pd.DataFrame(linhas)
    return df.where(pd.notna(df), '')  # evita 'nan' como string

def exportar_df(df: pd.DataFrame, out_dir: str, nome_base: str, use_csv: bool = True) -> str:
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    if use_csv:
        path = os.path.join(out_dir, f"{nome_base}.csv")
        df.to_csv(path, index=False, encoding="utf-8-sig")
    else:
        path = os.path.join(out_dir, f"{nome_base}.xlsx")
        df.to_excel(path, index=False, engine="openpyxl")
    logging.info(f"Arquivo gerado: {path} ({len(df):,} linhas)")
    return path

# =============================================================================
# DE→PARA
# =============================================================================
def load_transportadora_mapping(xlsx_path: str, sheet_name: str = "TRANSPORTADORA") -> dict:
    try:
        df = pd.read_excel(xlsx_path, sheet_name=sheet_name, usecols="A:B", dtype=str, engine="openpyxl")
        colA = df.columns[0]
        colB = df.columns[1]
        df = df.dropna(subset=[colA])
        mapping = {}
        for _, row in df.iterrows():
            origem = _norm(row[colA])
            destino = "" if pd.isna(row[colB]) else str(row[colB]).strip()
            if origem:
                mapping[origem] = destino
        logging.info(f"DE→PARA carregado: {len(mapping)} mapeamentos.")
        return mapping
    except Exception as e:
        logging.error(f"Falha ao ler DE→PARA em '{xlsx_path}' aba '{sheet_name}': {e}", exc_info=True)
        return {}

def apply_mapping(value: str, mapping: dict) -> str:
    key = _norm(value)
    if key in mapping:
        return mapping[key]
    return value

# =============================================================================
# Google Sheets
# =============================================================================
def gsheets_service():
    if not Path(CREDENTIALS_PATH).exists():
        raise FileNotFoundError(f"Arquivo de credenciais não encontrado: {CREDENTIALS_PATH}")
    creds = Credentials.from_service_account_file(CREDENTIALS_PATH, scopes=SCOPES)
    return build('sheets', 'v4', credentials=creds)

def clear_sheet(service, sheet_id=SHEET_ID):
    try:
        if not sheet_id:
            raise RuntimeError("SHEET_ID não definido.")
        service.spreadsheets().values().clear(spreadsheetId=sheet_id, range=SHEET_RANGE_ENTREGUES).execute()
        logging.info(f"Informações apagadas com sucesso da aba 'ENTREGUES CF' ({sheet_id}).")
    except Exception as e:
        logging.error(f"Erro ao apagar informações: {e}")

def get_last_row(service):
    try:
        result = service.spreadsheets().values().get(spreadsheetId=SHEET_ID, range='ENTREGUES CF!B:B').execute()
        values = result.get('values', [])
        return len(values) + 1
    except Exception as e:
        logging.error(f"Erro ao obter última linha: {e}")
        return 2

def split_into_batches(data, batch_size):
    for i in range(0, len(data), batch_size):
        yield data[i:i + batch_size]

def publicar_rows(service, rows, sheet_id=SHEET_ID, start_row=2, batch_size=10000):
    if not sheet_id:
        raise RuntimeError("SHEET_ID não definido para publicação.")

    pos = start_row
    for batch in split_into_batches(rows, batch_size):
        end_row = pos + len(batch) - 1
        batch_range = f'ENTREGUES CF!B{pos}:G{end_row}'
        body = {'values': batch}
        service.spreadsheets().values().update(
            spreadsheetId=sheet_id,
            range=batch_range,
            valueInputOption='RAW',
            body=body
        ).execute()
        logging.info(f"Lote escrito em {sheet_id}: linhas {pos}–{end_row}.")
        pos = end_row + 1
    return pos

# =============================================================================
# DataFrame → rows B:G com DE→PARA
# =============================================================================
def montar_rows_para_sheet(df: pd.DataFrame, status: str, mapping: dict):
    if df.empty:
        return []

    df = df.copy().where(pd.notna(df), '')
    df["Transportadora"] = df["Transportadora"].map(lambda v: apply_mapping(v, mapping))

    rows = []
    for _, r in df.iterrows():
        rows.append([
            format_value(r.get("Pedido", "")),
            format_value(r.get("NF", "")),
            format_value(r.get("Transportadora", "")),
            format_value(r.get("DataEntrega", "")),
            format_value(r.get("Chave", "")),
            status,
        ])
    return rows

# =============================================================================
# Pipeline
# =============================================================================
def _retry_consultar(di, df, token, session, codigos, attempts=3):
    for attempt in range(1, attempts + 1):
        try:
            logging.info(f"Consultando ocorrências (codigos={codigos}) — tentativa {attempt}/{attempts}")
            return consultar_ocorrencias(di, df, token, session, codigos)
        except requests.exceptions.RequestException as e:
            logging.warning(f"Falha na consulta (tentativa {attempt}/{attempts}): {e}")
            if attempt == attempts:
                logging.error("Número máximo de tentativas atingido. Abortando coleta.")
                raise
            sleep_time = BACKOFF * (2 ** (attempt - 1))
            logging.info(f"Aguardando {sleep_time:.1f}s antes da próxima tentativa...")
            time.sleep(sleep_time)

def coleta_e_publica():
    t0 = time.perf_counter()

    session = make_session(max_pool=40, total_retries=TOTAL_RETRIES, backoff=BACKOFF)
    token = autentificador(session)

    di, df = periodo_busca(LOOKBACK_DIAS)

    # ENTREGUES
    t_ent0 = time.perf_counter()
    resp_ent = _retry_consultar(di, df, token, session, CODES_ENTREGUES, attempts=TOTAL_RETRIES)
    df_ent = extrair_dataframe(resp_ent)
    path_ent = exportar_df(df_ent, OUTPUT_DIR, OUTPUT_NAME_ENTREGUES, USE_CSV)
    t_ent1 = time.perf_counter()

    # CANCELADOS
    t_can0 = time.perf_counter()
    resp_can = _retry_consultar(di, df, token, session, CODES_CANCELADOS, attempts=TOTAL_RETRIES)
    df_can = extrair_dataframe(resp_can)
    path_can = exportar_df(df_can, OUTPUT_DIR, OUTPUT_NAME_CANCELADOS, USE_CSV)
    t_can1 = time.perf_counter()

    mapping = load_transportadora_mapping(DEXPARA_XLSX_PATH, DEXPARA_SHEET)
    service = gsheets_service()

    rows_ent = montar_rows_para_sheet(df_ent, "ENTREGUE", mapping)
    rows_can = montar_rows_para_sheet(df_can, "CANCELADO", mapping)

    next_row = publicar_rows(service, rows_ent, start_row=2, batch_size=10000)
    publicar_rows(service, rows_can, start_row=next_row, batch_size=10000)

    t1 = time.perf_counter()
    logging.info("\n=== TEMPOS ===")
    logging.info(f"ENTREGUES (coleta+export): {_fmt(t_ent1 - t_ent0)}")
    logging.info(f"CANCELADOS (coleta+export): {_fmt(t_can1 - t_can0)}")
    logging.info(f"TOTAL pipeline: {_fmt(t1 - t0)}")
    logging.info(f"Arquivos: {path_ent} | {path_can}")


    # -----------------------------------------------------------------
    # Publica também na nova planilha (se SHEET_ID_EXTRA estiver setado)
    # -----------------------------------------------------------------
    if SHEET_ID_EXTRA:
        logging.info("Publicando também na planilha extra...")
        service_extra = gsheets_service()
        next_row_extra = publicar_rows(service_extra, rows_ent, sheet_id=SHEET_ID_EXTRA, start_row=2, batch_size=10000)
        publicar_rows(service_extra, rows_can, sheet_id=SHEET_ID_EXTRA, start_row=next_row_extra, batch_size=10000)

# =============================================================================
# CLI / Menu
# =============================================================================
def menu_interativo():
    try:
        service = gsheets_service()
    except Exception as e:
        logging.critical(f"Falha ao iniciar Google Sheets: {e}", exc_info=True)
        return

    while True:
        print("\nEscolha uma opção:")
        print("1: Apagar as informações da aba 'ENTREGUES CF'")
        print("2: Coletar CF → Exportar CSVs → Aplicar DE→PARA → Publicar no Sheets")
        print("0: Sair do programa")

        option = input("Digite 1, 2 ou 0: ").strip()

        if option == '1':
            clear_sheet(service)
        elif option == '2':
            try:
                coleta_e_publica()
            except Exception as e:
                logging.critical(f"Erro no pipeline: {e}", exc_info=True)
        elif option == '0':
            logging.info("Encerrando o programa.")
            break
        else:
            logging.warning("Opção inválida. Por favor, escolha 1, 2 ou 0.")


def cli():
    parser = argparse.ArgumentParser(description="Pipeline CF → CSV → DE→PARA → Google Sheets")
    parser.add_argument("--run", action="store_true", help="Executa o pipeline completo (sem menu).")
    parser.add_argument("--clear", action="store_true", help="Apaga a aba 'ENTREGUES CF'.")
    parser.add_argument("--lookback", type=int, default=None, help="Dias para trás (ex.: 30).")
    args = parser.parse_args()

    global LOOKBACK_DIAS
    if args.lookback is not None:
        LOOKBACK_DIAS = args.lookback

    if args.clear:
        service = gsheets_service()
        clear_sheet(service)
        return

    if args.run:
        coleta_e_publica()
        return

    # Sem flags → menu interativo
    menu_interativo()


if __name__ == "__main__":
    cli()
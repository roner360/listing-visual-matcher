
import os
import io
import time
from typing import Optional, Dict, Any, Tuple

import pandas as pd
import requests
import streamlit as st
from bs4 import BeautifulSoup


# -----------------------------
# Config UI
# -----------------------------
st.set_page_config(page_title="CSV Matcher", layout="wide")
st.title("CSV Matcher: Amazon vs Grossista")


# -----------------------------
# Proxy (IPRoyal) via ENV / Secrets
# -----------------------------
def get_proxy_config() -> Optional[Dict[str, str]]:
    """
    Legge proxy da variabili ambiente.
    Esempio:
      PROXY_HOST=...
      PROXY_PORT=...
      PROXY_USER=...
      PROXY_PASS=...
    Restituisce dict compatibile con requests: {"http": "...", "https": "..."}
    """
    host = os.getenv("PROXY_HOST", "").strip()
    port = os.getenv("PROXY_PORT", "").strip()
    user = os.getenv("PROXY_USER", "").strip()
    password = os.getenv("PROXY_PASS", "").strip()

    if not host or not port:
        return None

    if user and password:
        proxy_url = f"http://{user}:{password}@{host}:{port}"
    else:
        proxy_url = f"http://{host}:{port}"

    return {"http": proxy_url, "https": proxy_url}


def get_timeout() -> Tuple[float, float]:
    # (connect timeout, read timeout)
    return (5.0, 12.0)


def build_session() -> requests.Session:
    s = requests.Session()
    # User-Agent “normale” (aiuta a ricevere HTML standard)
    s.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/123.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "it-IT,it;q=0.9,en-US;q=0.8,en;q=0.7",
        }
    )
    return s


SESSION = build_session()
PROXIES = get_proxy_config()


# -----------------------------
# Cache: HTML e immagini
# -----------------------------
@st.cache_data(show_spinner=False, ttl=60 * 60)
def fetch_html(url: str) -> Optional[str]:
    try:
        r = SESSION.get(url, proxies=PROXIES, timeout=get_timeout(), allow_redirects=True)
        if r.status_code >= 400:
            return None
        # Amazon può rispondere gzip/charset variabile → requests gestisce
        return r.text
    except requests.RequestException:
        return None


@st.cache_data(show_spinner=False, ttl=24 * 60 * 60)
def extract_og_image(html: str) -> Optional[str]:
    try:
        soup = BeautifulSoup(html, "html.parser")
        tag = soup.find("meta", property="og:image")
        if tag and tag.get("content"):
            return tag["content"].strip()

        # fallback twitter:image
        tag = soup.find("meta", attrs={"name": "twitter:image"})
        if tag and tag.get("content"):
            return tag["content"].strip()

        return None
    except Exception:
        return None


@st.cache_data(show_spinner=False, ttl=24 * 60 * 60)
def download_image_bytes(url: str) -> Optional[bytes]:
    try:
        r = SESSION.get(url, proxies=PROXIES, timeout=get_timeout(), stream=True)
        if r.status_code >= 400:
            return None

        # limita dimensione per evitare bombe di memoria
        max_bytes = 3_500_000  # ~3.5MB
        data = bytearray()
        for chunk in r.iter_content(chunk_size=64 * 1024):
            if not chunk:
                break
            data.extend(chunk)
            if len(data) > max_bytes:
                break
        return bytes(data)
    except requests.RequestException:
        return None


def get_amazon_image_url(row: pd.Series, amazon_url_col: str, amazon_img_col: Optional[str]) -> Optional[str]:
    """
    Se amazon_img_col è presente e valorizzata: usa quella (consigliato).
    Altrimenti prova a estrarre og:image dall'HTML dell'amazon_url.
    """
    if amazon_img_col and amazon_img_col in row and pd.notna(row[amazon_img_col]) and str(row[amazon_img_col]).strip():
        return str(row[amazon_img_col]).strip()

    url = str(row[amazon_url_col]).strip()
    if not url:
        return None

    html = fetch_html(url)
    if not html:
        return None

    return extract_og_image(html)


# -----------------------------
# Stato: dizionario match
# -----------------------------
if "match_map" not in st.session_state:
    st.session_state.match_map = {}  # type: ignore


def set_match(row_id: int, value: bool):
    st.session_state.match_map[row_id] = value  # type: ignore


def get_match(row_id: int) -> bool:
    return bool(st.session_state.match_map.get(row_id, False))  # type: ignore


# -----------------------------
# Upload CSV + scelta colonne
#ifro
# -----------------------------
uploaded = st.file_uploader("Carica CSV", type=["csv"])

if not uploaded:
    st.info("Carica un CSV per iniziare.")
    st.stop()

# Leggi CSV in modo robusto
try:
    df = pd.read_csv(uploaded)
except Exception:
    uploaded.seek(0)
    df = pd.read_csv(uploaded, sep=";")

df = df.reset_index(drop=True)
st.caption(f"Righe: {len(df):,}  |  Colonne: {len(df.columns)}")

cols = list(df.columns)

with st.sidebar:
    st.header("Impostazioni")
    amazon_url_col = st.selectbox("Colonna URL Amazon", cols, index=0)
    grossista_img_col = st.selectbox("Colonna URL immagine Grossista", cols, index=0)

    amazon_img_col = st.selectbox(
        "Colonna URL immagine Amazon (opzionale, consigliata)",
        ["(nessuna)"] + cols,
        index=0
    )
    amazon_img_col = None if amazon_img_col == "(nessuna)" else amazon_img_col

    page_size = st.selectbox("Righe per pagina", [10, 20, 50, 100], index=1)
    show_cols = st.multiselect(
        "Altre colonne da mostrare",
        [c for c in cols if c not in {amazon_url_col, grossista_img_col, (amazon_img_col or "")}],
        default=[]
    )

    # Rate limit leggero (per non fare troppe richieste a raffica)
    rate_limit_ms = st.slider("Pausa tra righe (ms) per fetch Amazon", 0, 500, 60)


# -----------------------------
# Paginazione
# -----------------------------
total_pages = max(1, (len(df) + page_size - 1) // page_size)

c1, c2, c3, c4 = st.columns([1, 2, 2, 1])
with c1:
    page = st.number_input("Pagina", min_value=1, max_value=total_pages, value=1, step=1)
with c2:
    st.write("")
    st.write(f"Totale pagine: **{total_pages}**")
with c3:
    st.write("")
    st.write(f"Proxy attivo: **{'Sì' if PROXIES else 'No'}**")
with c4:
    st.write("")
    if st.button("Reset match"):
        st.session_state.match_map = {}  # type: ignore
        st.rerun()

start = (page - 1) * page_size
end = min(len(df), start + page_size)
page_df = df.iloc[start:end].copy()

st.divider()

# -----------------------------
# Render righe (solo pagina)
# -----------------------------
for idx, row in page_df.iterrows():
    row_id = int(idx)
    with st.container(border=True):
        top = st.columns([1, 3, 3, 5])

        # Checkbox
        with top[0]:
            current = get_match(row_id)
            new_val = st.checkbox("MATCH", value=current, key=f"match_{row_id}")
            if new_val != current:
                set_match(row_id, new_val)

        # Amazon image
        with top[1]:
            st.caption("Amazon")
            amazon_img_url = get_amazon_image_url(row, amazon_url_col, amazon_img_col)
            if amazon_img_url:
                img_bytes = download_image_bytes(amazon_img_url)
                if img_bytes:
                    st.image(img_bytes, use_column_width=True)
                else:
                    st.warning("Immagine Amazon non scaricabile.")
            else:
                st.warning("Immagine Amazon non trovata.")

        # Grossista image
        with top[2]:
            st.caption("Grossista")
            gross_url = str(row[grossista_img_col]).strip() if pd.notna(row[grossista_img_col]) else ""
            if gross_url:
                img_bytes = download_image_bytes(gross_url)
                if img_bytes:
                    st.image(img_bytes, use_column_width=True)
                else:
                    st.warning("Immagine Grossista non scaricabile.")
            else:
                st.warning("URL immagine grossista vuoto.")

        # Altre colonne
        with top[3]:
            st.caption("Dettagli")
            data = {}
            for c in show_cols:
                v = row.get(c, "")
                if pd.isna(v):
                    v = ""
                data[c] = v
            st.json(data, expanded=False)

        # piccola pausa per non sparare troppe richieste (soprattutto og:image Amazon)
        if rate_limit_ms > 0 and amazon_img_col is None:
            time.sleep(rate_limit_ms / 1000.0)

st.divider()


# -----------------------------
# Export CSV con colonna MATCH
# -----------------------------
st.subheader("Esporta CSV con MATCH")

match_map: Dict[int, bool] = st.session_state.match_map  # type: ignore
out = df.copy()
out["MATCH"] = [bool(match_map.get(i, False)) for i in range(len(out))]

csv_bytes = out.to_csv(index=False).encode("utf-8")

st.download_button(
    "Download CSV (con MATCH)",
    data=csv_bytes,
    file_name="output_with_match.csv",
    mime="text/csv",
)
st.caption("Le righe non controllate rimangono MATCH = False.")

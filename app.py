import os
import time
import re
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
# Helpers
# -----------------------------
def safe_str(x: Any) -> str:
    if x is None:
        return ""
    try:
        if pd.isna(x):
            return ""
    except Exception:
        pass
    return str(x).strip()


def suggest_amazon_img_col(df: pd.DataFrame, cols: list[str]) -> Optional[str]:
    """
    Preselect the column that most often contains m.media-amazon.com
    """
    best_col = None
    best_score = 0.0

    for c in cols:
        s = df[c].dropna().astype(str).head(300)
        if len(s) == 0:
            continue

        score = s.str.contains("m.media-amazon.com", case=False, regex=False).mean()
        if score > best_score:
            best_score = score
            best_col = c

    return best_col if best_score > 0 else None


def suggest_supplier_img_col(df: pd.DataFrame, cols: list[str], amazon_img_suggested: Optional[str]) -> Optional[str]:
    """
    Preselect the first column that strongly looks like an image URL column
    (.jpg/.jpeg/.png/.webp), excluding Amazon CDN columns.
    """
    img_pattern = re.compile(r"\.(jpg|jpeg|png|webp)(\?.*)?$", re.IGNORECASE)

    strong_candidates = []
    fallback_candidates = []

    for c in cols:
        if c == amazon_img_suggested:
            continue

        s = df[c].dropna().astype(str).head(300)
        if len(s) == 0:
            continue

        amazon_like_score = s.str.contains("m.media-amazon.com", case=False, regex=False).mean()
        if amazon_like_score > 0.2:
            continue

        img_score = s.str.contains(img_pattern, regex=True).mean()

        if img_score >= 0.6:
            strong_candidates.append((c, img_score))
        elif img_score > 0:
            fallback_candidates.append((c, img_score))

    if strong_candidates:
        strong_candidates.sort(key=lambda x: x[1], reverse=True)
        return strong_candidates[0][0]

    if fallback_candidates:
        fallback_candidates.sort(key=lambda x: x[1], reverse=True)
        return fallback_candidates[0][0]

    return None


# -----------------------------
# Proxy (IPRoyal) via ENV / Secrets
# -----------------------------
def get_proxy_config() -> Optional[Dict[str, str]]:
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
    return (5.0, 12.0)


def build_session() -> requests.Session:
    s = requests.Session()
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

        max_bytes = 3_500_000
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
    If amazon_img_col exists and has a value: use it.
    Otherwise try extracting og:image from the Amazon page.
    """
    if amazon_img_col and amazon_img_col in row and pd.notna(row[amazon_img_col]) and str(row[amazon_img_col]).strip():
        return str(row[amazon_img_col]).strip()

    url = safe_str(row.get(amazon_url_col, ""))
    if not url:
        return None

    html = fetch_html(url)
    if not html:
        return None

    return extract_og_image(html)


# -----------------------------
# Stato
# -----------------------------
if "match_map" not in st.session_state:
    st.session_state.match_map = {}

if "notes_map" not in st.session_state:
    st.session_state.notes_map = {}

if "page_num" not in st.session_state:
    st.session_state.page_num = 1


def set_match(row_id: int, value: bool):
    st.session_state.match_map[row_id] = value


def get_match(row_id: int) -> bool:
    return bool(st.session_state.match_map.get(row_id, False))


def set_note(row_id: int, value: str):
    st.session_state.notes_map[row_id] = value


def get_note(row_id: int) -> str:
    return str(st.session_state.notes_map.get(row_id, ""))


# -----------------------------
# Upload CSV + scelta colonne
# -----------------------------
uploaded = st.file_uploader("Carica CSV", type=["csv"])

if not uploaded:
    st.info("Carica un CSV per iniziare.")
    st.stop()

try:
    df = pd.read_csv(uploaded)
except Exception:
    uploaded.seek(0)
    df = pd.read_csv(uploaded, sep=";")

df = df.reset_index(drop=True)
st.caption(f"Righe: {len(df):,}  |  Colonne: {len(df.columns)}")

cols = list(df.columns)

# Suggested columns
amazon_img_suggested = suggest_amazon_img_col(df, cols)
supplier_img_suggested = suggest_supplier_img_col(df, cols, amazon_img_suggested)

amazon_img_options = ["(nessuna)"] + cols
amazon_img_index = amazon_img_options.index(amazon_img_suggested) if amazon_img_suggested in amazon_img_options else 0
grossista_index = cols.index(supplier_img_suggested) if supplier_img_suggested in cols else 0

with st.sidebar:
    st.header("Impostazioni")

    amazon_url_col = st.selectbox("Colonna URL Amazon", cols, index=0)

    grossista_img_col = st.selectbox(
        "Colonna URL immagine Grossista",
        cols,
        index=grossista_index
    )

    amazon_img_col = st.selectbox(
        "Colonna URL immagine Amazon (opzionale, consigliata)",
        amazon_img_options,
        index=amazon_img_index
    )
    amazon_img_col = None if amazon_img_col == "(nessuna)" else amazon_img_col

    page_size = st.selectbox("Righe per pagina", [10, 20, 50, 100], index=1)

    show_cols = st.multiselect(
        "Altre colonne da mostrare",
        [c for c in cols if c not in {amazon_url_col, grossista_img_col, (amazon_img_col or "")}],
        default=[]
    )

    rate_limit_ms = st.slider("Pausa tra righe (ms) per fetch Amazon", 0, 500, 60)

    if amazon_img_suggested:
        st.caption(f"Suggerita colonna Amazon image: `{amazon_img_suggested}`")
    if supplier_img_suggested:
        st.caption(f"Suggerita colonna Grossista image: `{supplier_img_suggested}`")


# -----------------------------
# Paginazione - top
# -----------------------------
total_pages = max(1, (len(df) + page_size - 1) // page_size)

if st.session_state.page_num > total_pages:
    st.session_state.page_num = total_pages

c1, c2, c3, c4 = st.columns([1, 2, 2, 1])

with c1:
    top_page = st.number_input(
        "Pagina",
        min_value=1,
        max_value=total_pages,
        value=int(st.session_state.page_num),
        step=1,
        key="page_top"
    )
    if top_page != st.session_state.page_num:
        st.session_state.page_num = int(top_page)
        st.rerun()

with c2:
    st.write("")
    st.write(f"Totale pagine: **{total_pages}**")

with c3:
    st.write("")
    st.write(f"Proxy attivo: **{'Sì' if PROXIES else 'No'}**")

with c4:
    st.write("")
    if st.button("Reset match"):
        st.session_state.match_map = {}
        st.session_state.notes_map = {}
        st.rerun()

page = int(st.session_state.page_num)
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

        # MATCH
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
                    st.image(img_bytes, use_container_width=True)
                else:
                    st.warning("Immagine Amazon non scaricabile.")
            else:
                st.warning("Immagine Amazon non trovata.")

        # Grossista image
        with top[2]:
            st.caption("Grossista")
            gross_url = safe_str(row.get(grossista_img_col, ""))
            if gross_url:
                img_bytes = download_image_bytes(gross_url)
                if img_bytes:
                    st.image(img_bytes, use_container_width=True)
                else:
                    st.warning("Immagine Grossista non scaricabile.")
            else:
                st.warning("URL immagine grossista vuoto.")

        # Details + Notes
        with top[3]:
            st.caption("Dettagli")
            data = {}
            for c in show_cols:
                v = row.get(c, "")
                if pd.isna(v):
                    v = ""
                data[c] = v
            st.json(data, expanded=False)

            note_val = st.text_area(
                "Notes",
                value=get_note(row_id),
                key=f"note_{row_id}",
                height=100
            )
            if note_val != get_note(row_id):
                set_note(row_id, note_val)

        if rate_limit_ms > 0 and amazon_img_col is None:
            time.sleep(rate_limit_ms / 1000.0)

st.divider()

# -----------------------------
# Paginazione - bottom
# -----------------------------
b1, b2, b3, b4 = st.columns([1, 1, 2, 2])

with b1:
    if st.button("⬅️ Prev", disabled=(st.session_state.page_num <= 1), use_container_width=True):
        st.session_state.page_num -= 1
        st.rerun()

with b2:
    if st.button("Next ➡️", disabled=(st.session_state.page_num >= total_pages), use_container_width=True):
        st.session_state.page_num += 1
        st.rerun()

with b3:
    bottom_page = st.number_input(
        "Vai a pagina",
        min_value=1,
        max_value=total_pages,
        value=int(st.session_state.page_num),
        step=1,
        key="page_bottom"
    )
    if bottom_page != st.session_state.page_num:
        st.session_state.page_num = int(bottom_page)
        st.rerun()

with b4:
    st.write("")
    st.write(f"Pagina **{st.session_state.page_num}** di **{total_pages}**")


# -----------------------------
# Export CSV con colonna MATCH + NOTES
# -----------------------------
st.subheader("Esporta CSV con MATCH")

match_map: Dict[int, bool] = st.session_state.match_map
notes_map: Dict[int, str] = st.session_state.notes_map

out = df.copy()
out["MATCH"] = [bool(match_map.get(i, False)) for i in range(len(out))]
out["NOTES"] = [str(notes_map.get(i, "")) for i in range(len(out))]

csv_bytes = out.to_csv(index=False).encode("utf-8")

st.download_button(
    "Download CSV (con MATCH + NOTES)",
    data=csv_bytes,
    file_name="output_with_match_notes.csv",
    mime="text/csv",
)

st.caption("Le righe non controllate rimangono MATCH = False.")

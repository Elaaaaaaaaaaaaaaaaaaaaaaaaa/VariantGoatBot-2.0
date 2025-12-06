# ================================
# VariantGoatbot v2.0 "Italia Pro"
# ================================
# Migliorie:
# - Estrazione 💰 Prezzo e 🗓️ Data (Panini/Star/J-POP)
# - Log a schermo con timestamp + contatore giornaliero
# - Notifica giornaliera "🧠 Sistema attivo" (una sola)
# - Hardening anti-duplicati e gestione errori/ripetizioni
# - Formattazione messaggi migliorata + 🇮🇹 tag nazione
#
# Requisiti:
#   pip install requests beautifulsoup4
#
# Avvio:
#   python variant_goatbot.py
#
# Comandi in chat:
#   /start  /ping  /status  /testnotify
#
# ================================

import time
import json
import os
import threading
import re
from datetime import datetime, date
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from pathlib import Path

# ---------- Watchdog state ----------
last_poll_at = 0  # timestamp ultimo segnale dal polling
polling_alive = False


# ---------- CONFIG ----------
# ⚠️ Incolla qui gli stessi valori che hai già ora (non cambiare!)
TOKEN   = "8228974197:AAF5oNJosi1hHNJa4UazRGj6t2xJWfUKYVA"
CHAT_ID = 651262868


# ---------- HTTP USER-AGENT ----------
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

# ---------- CONFIG PIKA ----------
PIKA_BASE_URL = "https://www.pika.fr"
PIKA_TOUT_URL = f"{PIKA_BASE_URL}/tous-les-mangas/"

PIKA_SEEN_FILE = Path("pika_seen.json")


# ========== PIKA – SEEN STORAGE ==========

PIKA_SEEN_FILE = Path("pika_seen.json")

# ---------- CONFIG GLÉNAT ----------
GLENAT_BASE_URL = "https://www.glenat.com"
GLENAT_SPECIAL_URL = "https://www.glenat.com/editions-speciales"
GLENAT_SEEN_FILE = Path("glenat_seen.json")


# ---------- CONFIG KIOON ----------
KIOON_BASE_URL = "https://www.ki-oon.com"
KIOON_NEWS_URL = "https://www.ki-oon.com/news.html"
KIOON_SEEN_FILE = Path("kioon_seen.json")


# ---------- CONFIG KUROKAWA ----------
KURO_BASE_URL = "https://www.kurokawa.fr"
KURO_NEWS_URL = "https://www.kurokawa.fr/actualites"
KURO_SEEN_FILE = Path("kurokawa_seen.json")

# ---------- CONFIG NORMA ----------
NORMA_BASE_URL = "https://www.normaeditorial.com"
NORMA_NEWS_URL = "https://www.normaeditorial.com/noticias"
NORMA_SEEN_FILE = Path("norma_seen.json")

# ---------- CONFIG IVREA ----------
IVREA_BASE_URL = "https://www.ivrea.es"
IVREA_NEWS_URL = "https://www.ivrea.es/category/novedades/"
IVREA_SEEN_FILE = Path("ivrea_seen.json")

# ---------- CONFIG MILKY WAY ----------
MW_BASE_URL = "https://www.milkywayediciones.com"
MW_HOME_URL = MW_BASE_URL + "/"
MW_SEEN_FILE = Path("milkyway_seen.json")

# ---------- CONFIG PLANETA CÓMIC ----------
PLANETA_BASE_URL = "https://www.planetadelibros.com"
PLANETA_NEWS_URL = "https://www.planetadelibros.com/editorial/planeta-comic/54#novedades"
PLANETA_SEEN_FILE = Path("planeta_seen.json")

# ---------- CARLSEN (Germania) ----------
CARLSEN_MONTH_URL = "https://www.carlsen.de/manga/monatsuebersicht"

# ---------- TOKYOPOP Germania ----------
TOKYOPOP_SPECIAL_URL   = "https://www.tokyopop.de/produkt-kategorie/special-editions/"
TOKYOPOP_LIMITED_URL   = "https://www.tokyopop.de/produkt-kategorie/limited-editions/"
TOKYOPOP_COLLECTOR_URL = "https://www.tokyopop.de/produkt-kategorie/collector-editions/"



def load_pika_seen():
    """
    Legge dal file JSON gli ID Pika già notificati.
    Ritorna un set() di stringhe.
    """
    if PIKA_SEEN_FILE.exists():
        try:
            return set(json.loads(PIKA_SEEN_FILE.read_text("utf-8")))
        except Exception:
            return set()
    return set()


def save_pika_seen(seen_ids):
    """
    Salva sul file JSON gli ID Pika già notificati.
    """
    try:
        PIKA_SEEN_FILE.write_text(json.dumps(sorted(seen_ids)), encoding="utf-8")
    except Exception as e:
        print(f"[PIKA] Errore nel salvataggio dei seen: {e}")


def load_glenat_seen():
    if GLENAT_SEEN_FILE.exists():
        try:
            return set(json.loads(GLENAT_SEEN_FILE.read_text("utf-8")))
        except Exception:
            return set()
    return set()


def save_glenat_seen(seen_ids):
    try:
        GLENAT_SEEN_FILE.write_text(json.dumps(sorted(seen_ids)), encoding="utf-8")
    except Exception as e:
        print(f"[GLENAT] Errore salvataggio seen: {e}")


def load_kioon_seen():
    if KIOON_SEEN_FILE.exists():
        try:
            data = json.loads(KIOON_SEEN_FILE.read_text("utf-8"))
            return set(data)
        except Exception:
            return set()
    return set()


def save_kioon_seen(seen_ids: set[str]):
    try:
        KIOON_SEEN_FILE.write_text(
            json.dumps(sorted(seen_ids), ensure_ascii=False),
            encoding="utf-8"
        )
    except Exception as e:
        print(f"{now_ts()} [KIOON][ERR] save_kioon_seen: {e}", flush=True)


def load_kuro_seen():
    if KURO_SEEN_FILE.exists():
        try:
            data = json.loads(KURO_SEEN_FILE.read_text("utf-8"))
            return set(data)
        except Exception:
            return set()
    return set()


def save_kuro_seen(seen_ids):
    try:
        KURO_SEEN_FILE.write_text(
            json.dumps(sorted(seen_ids), ensure_ascii=False),
            encoding="utf-8"
        )
    except Exception as e:
        print(f"{now_ts()} [KURO][ERR] save_kuro_seen: {e}", flush=True)

def load_norma_seen():
    if NORMA_SEEN_FILE.exists():
        try:
            data = json.loads(NORMA_SEEN_FILE.read_text("utf-8"))
            return set(data)
        except Exception:
            return set()
    return set()


def save_norma_seen(seen_ids):
    try:
        NORMA_SEEN_FILE.write_text(
            json.dumps(sorted(seen_ids), ensure_ascii=False),
            encoding="utf-8"
        )
    except Exception as e:
        print(f"{now_ts()} [NORMA][ERR] save_norma_seen: {e}", flush=True)

def load_ivrea_seen():
    if IVREA_SEEN_FILE.exists():
        try:
            data = json.loads(IVREA_SEEN_FILE.read_text("utf-8"))
            return set(data)
        except Exception:
            return set()
    return set()

def save_ivrea_seen(seen_ids):
    try:
        IVREA_SEEN_FILE.write_text(
            json.dumps(sorted(seen_ids), ensure_ascii=False),
            encoding="utf-8"
        )
    except Exception as e:
        print(f"{now_ts()} [IVREA][ERR] save_ivrea_seen: {e}", flush=True)

def load_mw_seen():
    if MW_SEEN_FILE.exists():
        try:
            data = json.loads(MW_SEEN_FILE.read_text("utf-8"))
            return set(data)
        except Exception:
            return set()
    return set()

def save_mw_seen(seen_ids):
    try:
        MW_SEEN_FILE.write_text(
            json.dumps(sorted(seen_ids), ensure_ascii=False),
            encoding="utf-8"
        )
    except Exception as e:
        print(f"{now_ts()} [MILKY][ERR] save_mw_seen: {e}", flush=True)

def load_planeta_seen():
    if PLANETA_SEEN_FILE.exists():
        try:
            data = json.loads(PLANETA_SEEN_FILE.read_text("utf-8"))
            return set(data)
        except Exception:
            return set()
    return set()


def save_planeta_seen(seen_ids):
    try:
        PLANETA_SEEN_FILE.write_text(
            json.dumps(sorted(seen_ids), ensure_ascii=False),
            encoding="utf-8"
        )
    except Exception as e:
        print(f"{now_ts()} [PLANETA][ERR] save_planeta_seen: {e}", flush=True)

# ------------------------------------------------------------
# TOKYOPOP (Germania) – Edizioni Speciali / Limitate
# ------------------------------------------------------------
def check_tokyopop_de(send):
    import requests
    from bs4 import BeautifulSoup

    BASE = "https://www.tokyopop.de"
    URLS = [
        f"{BASE}/produkt-kategorie/special-editions/",
        f"{BASE}/produkt-kategorie/collector-editions/",
        f"{BASE}/produkt-kategorie/limitierte-ausgaben/",
    ]

    print(f"{now_ts()} [TOKYOPOP-DE] Controllo limited/collector…")

    new_count = 0

    for url in URLS:
        try:
            html = requests.get(url, timeout=15).text
        except Exception as e:
            print(f"{now_ts()} [TOKYOPOP-DE][ERR] {e}")
            continue

        soup = BeautifulSoup(html, "html.parser")

        # Ogni prodotto è dentro <li class="product"> … variabile sul sito, verificherò comunque
        items = soup.select("li.product")

        for item in items:
            a = item.find("a")
            if not a:
                continue

            title = a.get("title") or a.text.strip()
            link = a["href"]

            if link in seen:
                continue

            seen.add(link)
            new_count += 1

            msg = f"🇩🇪 **Tokyopop DE – Edizione Speciale**\n📘 *{title}*\n🔗 {link}"
            send(msg)

    print(f"{now_ts()} [TOKYOPOP-DE] Completato. Nuove notifiche: {new_count}")

# ========== PIKA – KEYWORDS & ID ==========

PIKA_SPECIAL_KEYWORDS = [
    "collector",
    "édition collector",
    "edition collector",
    "édition limitée",
    "edition limitée",
    "edition limitee",
    "tirage limité",
    "tirage limite",
    "grimoire",
    "édition spéciale",
    "edition spéciale",
    "edition speciale",
    "exclusif",
    "exclusive",
    "variant",
]

GLENAT_SPECIAL_KEYWORDS = [
    "édition collector",
    "edition collector",
    "édition limitée",
    "edition limitée",
    "limited",
    "tirage limité",
    "tirage limite",
    "collector",
    "coffret",
    "variant",
]

KURO_KEYWORDS = [
    "collector",
    "édition limitée",
    "edition limitée",
    "édition spéciale",
    "edition spéciale",
    "coffret",
    "pack",
    "intégrale",
    "tirage limité",
    "limited",
]

KIOON_KEYWORDS = [
    "collector",
    "coffret",
    "coffrets",
    "édition collector",
    "edition collector",
    "édition spéciale",
    "edition speciale",
    "édition limitée",
    "limited",
    "all stars",
]

NORMA_KEYWORDS = [
    "edición especial",
    "edición limitada",
    "edicion especial",
    "edicion limitada",
    "cofre",
    "cofres",
    "integral",
    "integrales",
    "pack",
    "coleccionista",
    "deluxe",
]

IVREA_KEYWORDS = [
    "edición especial",
    "edición limitada",
    "edicion especial",
    "edicion limitada",
    "cofre",
    "cofres",
    "box",
    "pack",
    "coleccionista",
    "deluxe",
    "especial",
    "regalo",
]

MW_KEYWORDS = [
    "edición especial",
    "edición limitada",
    "edicion especial",
    "edicion limitada",
    "grimorio",
    "cofre",
    "box",
    "pack",
    "deluxe",
    "coleccionista",
]

PLANETA_KEYWORDS = [
    "edición especial",
    "edicion especial",
    "edición limitada",
    "edicion limitada",
    "cofre",
    "pack",
    "integral",
    "deluxe",
    "coleccionista",
    "estuche",
]

CARLSEN_KEYWORDS = [
    "sonderausgabe",
    "limitierte", "limitiert",
    "edition",
    "schuber",
    "vorzugsausgabe",
    "deluxe",
]


def fetch_kioon_news(max_pages: int = 3):
    """
    Legge la sezione news di Ki-oon e restituisce gli articoli
    che sembrano riguardare coffret / collector / special.
    """
    session = requests.Session()
    results = []

    headers = {
        "User-Agent": USER_AGENT,
        "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.7,it;q=0.6",
    }

    for page in range(max_pages):
        if page == 0:
            url = KIOON_NEWS_URL
        else:
            url = f"{KIOON_NEWS_URL}?page={page}"

        try:
            resp = session.get(url, headers=headers, timeout=20)
            resp.raise_for_status()
        except Exception as e:
            print(f"{now_ts()} [KIOON][ERR] fetch page {page}: {e}", flush=True)
            continue

        soup = BeautifulSoup(resp.text, "html.parser")

        # Prendiamo tutti i link che portano a /news/xxxx-qualcosa.html
        for a in soup.find_all("a", href=True):
            href = a["href"]
            title = a.get_text(strip=True)
            if not title:
                continue

            # teniamo solo gli articoli news
            if "/news/" not in href:
                continue

            low = title.lower()
            if not any(kw in low for kw in KIOON_KEYWORDS):
                continue

            # costruiamo URL assoluto
            if href.startswith("http"):
                full_url = href
            else:
                full_url = KIOON_BASE_URL.rstrip("/") + "/" + href.lstrip("/")

            results.append(
                {
                    "title": title,
                    "url": full_url,
                }
            )

    return results

def check_kioon(send_func):
    """
    Controlla le news Ki-oon per articoli che parlano di coffret/collector
    e notifica solo quelli non ancora visti.
    """
    print(f"{now_ts()} [KIOON] Controllo news coffrets/collector…", flush=True)

    seen = load_kioon_seen()
    new_seen = set(seen)
    new_count = 0

    try:
        items = fetch_kioon_news()
    except Exception as e:
        print(f"{now_ts()} [KIOON][ERR] fetch_kioon_news: {e}", flush=True)
        return

    for item in items:
        key = item["url"]  # usiamo l'URL come ID
        if key in seen:
            continue

        title = item["title"]
        link = item["url"]

        msg = (
            "🇫🇷 *Ki-oon – Coffret / édition spéciale annunciata!*\n"
            f"📣 *Titolo news:* {title}\n"
            f"🔗 {link}"
        )

        send_func(msg, parse_mode="Markdown")
        new_seen.add(key)
        new_count += 1
        print(f"{now_ts()} [KIOON] 🔔 New: {title}", flush=True)

    if new_seen != seen:
        save_kioon_seen(new_seen)

    print(f"{now_ts()} [KIOON] Completato. Nuove notifiche: {new_count}", flush=True)



def is_pika_special_edition(label: str) -> bool:
    """
    Ritorna True se nel testo ci sono parole tipo collector/limited/variant ecc.
    """
    text = label.lower()
    return any(kw in text for kw in PIKA_SPECIAL_KEYWORDS)


def make_pika_id(item: dict) -> str:
    """
    Crea un ID stabile per evitare doppie notifiche (es. titolo+data).
    """
    return f"{item.get('title','')}|{item.get('date_str','')}"

# ========== PIKA – FETCH ==========

PIKA_BASE_URL = "https://www.pika.fr"
PIKA_TOUT_URL = f"{PIKA_BASE_URL}/tous-les-mangas/"


def parse_pika_date(date_str: str):
    """
    Pika usa formato gg/mm/aaaa, es. 15/04/2026.
    Ritorna datetime.date oppure None se non riesce a parsare.
    """
    try:
        return datetime.strptime(date_str, "%d/%m/%Y").date()
    except Exception:
        return None


def fetch_pika_upcoming_special_editions(max_pages: int = 3):
    """
    Scansiona le prime N pagine 'Tous les mangas' e ritorna
    una lista di dict con le sole édition collector/limited/variant.
    """
    session = requests.Session()
    results = []

    for page in range(max_pages):
        url = PIKA_TOUT_URL
        if page > 0:
            url = f"{PIKA_TOUT_URL}?page={page}"

        resp = session.get(url, timeout=15)
        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "html.parser")

        # Estraggo tutti i link, filtrando quelli che iniziano con "À paraître"
        for a in soup.find_all("a", href=True):
            label = a.get_text(strip=True)
            if not label.startswith("À paraître"):
                continue

            parts = label.split()
            if len(parts) < 3:
                continue

            raw_date = parts[-1]
            pub_date = parse_pika_date(raw_date)

            # Tutto tranne 'À' iniziale e la data finale lo teniamo come "title"
            title_text = " ".join(parts[1:-1]).strip()

            if not is_pika_special_edition(title_text):
                # Se non contiene collector/limited/variant, skippa
                continue

            full_url = urljoin(PIKA_BASE_URL, a["href"])

            item = {
                "raw_label": label,
                "title": title_text,
                "date_str": raw_date,
                "date": pub_date,
                "url": full_url,
                "source": "pika_tous_les_mangas",
            }
            results.append(item)

    return results


def fetch_glenat_special_editions(max_pages=3):
    session = requests.Session()
    results = []

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept": "*/*",
        "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
        "X-Requested-With": "XMLHttpRequest",
    }

    for page in range(max_pages):
        payload = {
            "view_name": "catalogue",
            "view_display_id": "block_1",
            "view_args": "",
            "view_path": "editions-speciales",
            "page": str(page),
            "field_collection_tags": "2646",   # TAG vero delle edizioni speciali
            "_wrapper_format": "drupal_ajax",
        }

        try:
            resp = session.post(
                "https://www.glenat.com/views/ajax",
                data=payload,
                headers=headers,
                timeout=15
            )
            resp.raise_for_status()
        except Exception as e:
            print(f"[GLENAT] ajax fetch error on page {page}: {e}")
            continue

        # la risposta è JSON, con dentro “data” => html della pagina filtrata
        try:
            json_data = resp.json()
        except:
            print("[GLENAT] invalid JSON")
            continue

        html = ""
        for part in json_data:
            if "data" in part:
                html += part["data"]

        soup = BeautifulSoup(html, "html.parser")

        for a in soup.find_all("a", class_="product-link", href=True):
            title = a.get_text(strip=True)
            link = urljoin(GLENAT_BASE_URL, a["href"])

            if not any(kw in title.lower() for kw in GLENAT_SPECIAL_KEYWORDS):
                continue

            results.append({
                "title": title,
                "url": link,
                "date_str": "",
                "date": None,
            })

    return results



def make_glenat_id(item):
    return f"{item['title']}|{item['url']}"


def format_glenat_message(item):
    return (
        "🇫🇷 *Glénat – Édition spéciale trouvée!*\n"
        f"📚 {item['title']}\n"
        f"🔗 {item['url']}"
    )

def check_glenat(tg_send):
    print("[GLENAT] Controllo éditions spéciales...")

    seen = load_glenat_seen()
    new_seen = set(seen)

    try:
        items = fetch_glenat_special_editions(max_pages=2)
    except Exception as e:
        print(f"[GLENAT] Fetch error {e}")
        return

    for item in items:
        gid = make_glenat_id(item)
        if gid in seen:
            continue

        msg = format_glenat_message(item)
        tg_send(msg, parse_mode="Markdown")
        new_seen.add(gid)

        print(f"[GLENAT] Notified {item['title']}")

    if new_seen != seen:
        save_glenat_seen(new_seen)

    print(f"[GLENAT] Completato. Nuove notifiche: {len(new_seen) - len(seen)}")


def fetch_kuro_news(max_pages=3):
    """
    Legge le news ufficiali di Kurokawa e prende solo articoli
    che contengono collector/coffret/limited editions.
    """
    session = requests.Session()
    results = []

    headers = {
        "User-Agent": USER_AGENT,
        "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8,it;q=0.7",
    }

    for page in range(max_pages):
        if page == 0:
            url = KURO_NEWS_URL
        else:
            url = f"{KURO_NEWS_URL}?page={page}"

        try:
            resp = session.get(url, headers=headers, timeout=20)
            resp.raise_for_status()
        except Exception as e:
            print(f"{now_ts()} [KURO][ERR] fetch page {page}: {e}", flush=True)
            continue

        soup = BeautifulSoup(resp.text, "html.parser")

        # Articoli news con link
        for a in soup.find_all("a", href=True):
            title = a.get_text(strip=True)
            href = a["href"]
            if not title:
                continue

            # filtriamo solo articoli
            if "/actualites/" not in href:
                continue

            low = title.lower()
            if not any(kw in low for kw in KURO_KEYWORDS):
                continue

            if href.startswith("http"):
                full_url = href
            else:
                full_url = KURO_BASE_URL.rstrip("/") + "/" + href.lstrip("/")

            results.append({
                "title": title,
                "url": full_url,
            })

    return results

def check_kuro(send_func):
    """
    Controlla articoli Kurokawa per collector/coffret
    e invia solo quelli nuovi.
    """
    print(f"{now_ts()} [KURO] Controllo news collector/coffrets…", flush=True)

    seen = load_kuro_seen()
    new_seen = set(seen)
    new_count = 0

    try:
        items = fetch_kuro_news()
    except Exception as e:
        print(f"{now_ts()} [KURO][ERR] fetch_kuro_news: {e}", flush=True)
        return

    for item in items:
        key = item["url"]
        if key in seen:
            continue

        title = item["title"]
        link = item["url"]

        msg = (
            "🇫🇷 *Kurokawa – Nouvelle édition spéciale / collector !*\n"
            f"📚 *Titolo:* {title}\n"
            f"🔗 {link}"
        )

        send_func(msg, parse_mode="Markdown")
        new_seen.add(key)
        new_count += 1

        print(f"{now_ts()} [KURO] 🔔 New collector: {title}")

    if new_seen != seen:
        save_kuro_seen(new_seen)

    print(f"{now_ts()} [KURO] Completato. Nuove notifiche: {new_count}", flush=True)

def fetch_norma_news(max_pages=3):
    """
    Legge la sezione noticias di Norma Editorial e cattura solo articoli
    relativi a ediciones especiales, cofres, integrales, pack, ecc.
    """
    session = requests.Session()
    results = []

    headers = {
        "User-Agent": USER_AGENT,
        "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
    }

    for page in range(max_pages):
        if page == 0:
            url = NORMA_NEWS_URL
        else:
            url = f"{NORMA_NEWS_URL}?page={page}"

        try:
            resp = session.get(url, headers=headers, timeout=20)
            resp.raise_for_status()
        except Exception as e:
            print(f"{now_ts()} [NORMA][ERR] fetch page {page}: {e}", flush=True)
            continue

        soup = BeautifulSoup(resp.text, "html.parser")

        # articoli news
        for a in soup.find_all("a", href=True):
            title = a.get_text(strip=True)
            href = a["href"]
            if not title:
                continue

            # prendiamo solo le news vere
            if "/noticias/" not in href:
                continue

            low = title.lower()
            if not any(kw in low for kw in NORMA_KEYWORDS):
                continue

            # costruiamo URL completo
            if href.startswith("http"):
                full_url = href
            else:
                full_url = NORMA_BASE_URL.rstrip("/") + "/" + href.lstrip("/")

            results.append({"title": title, "url": full_url})

    return results

def check_norma(send_func):
    """
    Controlla le news Norma e invia solo le collector/limited nuove.
    """
    print(f"{now_ts()} [NORMA] Controllo noticias collector…", flush=True)

    seen = load_norma_seen()
    new_seen = set(seen)
    new_count = 0

    try:
        items = fetch_norma_news()
    except Exception as e:
        print(f"{now_ts()} [NORMA][ERR] fetch_norma_news: {e}", flush=True)
        return

    for item in items:
        key = item["url"]
        if key in seen:
            continue

        title = item["title"]
        link = item["url"]

        msg = (
            "🇪🇸 *Norma Editorial – Nueva edición especial / cofre / pack!*\n"
            f"📚 *Titolo:* {title}\n"
            f"🔗 {link}"
        )

        send_func(msg, parse_mode="Markdown")
        new_seen.add(key)
        new_count += 1

        print(f"{now_ts()} [NORMA] 🔔 New collector: {title}")

    if new_seen != seen:
        save_norma_seen(new_seen)

    print(f"{now_ts()} [NORMA] Completato. Nuove notifiche: {new_count}", flush=True)

def fetch_ivrea_news(max_pages=3):
    """
    Legge la sezione Novedades di Ivrea e filtra solo collector/cofres/packs.
    """
    session = requests.Session()
    results = []

    headers = {
        "User-Agent": USER_AGENT,
        "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
    }

    for page in range(max_pages):
        if page == 0:
            url = IVREA_NEWS_URL
        else:
            url = f"{IVREA_NEWS_URL}page/{page+1}/"

        try:
            resp = session.get(url, headers=headers, timeout=20)
            resp.raise_for_status()
        except Exception as e:
            print(f"{now_ts()} [IVREA][ERR] fetch page {page}: {e}", flush=True)
            continue

        soup = BeautifulSoup(resp.text, "html.parser")

        # Ogni uscita è un articolo <article> con <h2> → titolo
        for art in soup.find_all("article"):
            h2 = art.find("h2")
            if not h2:
                continue

            title = h2.get_text(strip=True)
            link_tag = h2.find("a")
            if not link_tag:
                continue

            link = link_tag["href"].strip()

            # filtriamo solo collector/cofres/packs
            low = title.lower()
            if not any(kw in low for kw in IVREA_KEYWORDS):
                continue

            results.append({"title": title, "url": link})

    return results

def check_ivrea(send_func):
    print(f"{now_ts()} [IVREA] Controllo novedades collector…", flush=True)

    seen = load_ivrea_seen()
    new_seen = set(seen)
    new_count = 0

    try:
        items = fetch_ivrea_news()
    except Exception as e:
        print(f"{now_ts()} [IVREA][ERR] fetch_ivrea_news: {e}", flush=True)
        return

    for item in items:
        key = item["url"]
        if key in seen:
            continue

        title = item["title"]
        link = item["url"]

        msg = (
            "🇪🇸 *Ivrea España – Nueva edición especial / cofre / pack!*\n"
            f"📚 *Titolo:* {title}\n"
            f"🔗 {link}"
        )

        send_func(msg, parse_mode="Markdown")
        new_seen.add(key)
        new_count += 1

        print(f"{now_ts()} [IVREA] 🔔 New collector: {title}")

    if new_seen != seen:
        save_ivrea_seen(new_seen)

    print(f"{now_ts()} [IVREA] Completato. Nuove notifiche: {new_count}", flush=True)

def fetch_mw_new_releases():
    """
    Legge la home di Milky Way Ediciones e prende le 'Novedades recientes'
    che sembrano edizioni speciali (grimorio, deluxe, cofre, pack, ecc.).
    """
    session = requests.Session()
    headers = {
        "User-Agent": USER_AGENT,
        "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
    }

    try:
        resp = session.get(MW_HOME_URL, headers=headers, timeout=20)
        resp.raise_for_status()
    except Exception as e:
        print(f"{now_ts()} [MILKY][ERR] fetch home: {e}", flush=True)
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    results = []

    # sulla home le novità recenti sono in <h3> con dentro un <a>
    for h3 in soup.find_all("h3"):
        a = h3.find("a", href=True)
        if not a:
            continue

        title = h3.get_text(strip=True)
        if not title:
            continue

        low = title.lower()
        if not any(kw in low for kw in MW_KEYWORDS):
            continue

        href = a["href"].strip()
        if href.startswith("http"):
            url = href
        else:
            url = MW_BASE_URL.rstrip("/") + "/" + href.lstrip("/")

        results.append({"title": title, "url": url})

    return results

def check_milkyway(send_func):
    """
    Controlla le 'Novedades recientes' di Milky Way Ediciones
    e notifica solo le edizioni speciali non ancora viste.
    """
    print(f"{now_ts()} [MILKY] Controllo novedades recientes collector…", flush=True)

    seen = load_mw_seen()
    new_seen = set(seen)
    new_count = 0

    try:
        items = fetch_mw_new_releases()
    except Exception as e:
        print(f"{now_ts()} [MILKY][ERR] fetch_mw_new_releases: {e}", flush=True)
        return

    for item in items:
        key = item["url"]
        if key in seen:
            continue

        title = item["title"]
        link = item["url"]

        msg = (
            "🇪🇸 *Milky Way Ediciones – Nueva edición especial / grimorio / box!*\n"
            f"📚 *Titolo:* {title}\n"
            f"🔗 {link}"
        )

        send_func(msg, parse_mode="Markdown")
        new_seen.add(key)
        new_count += 1

        print(f"{now_ts()} [MILKY] 🔔 New collector: {title}")

    if new_seen != seen:
        save_mw_seen(new_seen)

    print(f"{now_ts()} [MILKY] Completato. Nuove notifiche: {new_count}", flush=True)


def fetch_planeta_news():
    """
    Legge la pagina editoriale 'Planeta Cómic' e prende le novità
    che sembrano edizioni speciali (cofre, pack, deluxe, ecc.).
    """
    session = requests.Session()
    headers = {
        "User-Agent": USER_AGENT,
        "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
    }

    try:
        resp = session.get(PLANETA_NEWS_URL, headers=headers, timeout=20)
        resp.raise_for_status()
    except Exception as e:
        print(f"{now_ts()} [PLANETA][ERR] fetch novedades: {e}", flush=True)
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    results = []

    # sulla pagina editoriale i titoli stanno in blocchi tipo <div class="book">
    for item in soup.find_all("div", class_="book"):
        a = item.find("a", href=True)
        h3 = item.find("h3")
        if not a or not h3:
            continue

        title = h3.get_text(strip=True)
        if not title:
            continue

        low = title.lower()
        # 🔥 SOLO edizioni speciali / cofanetti / pack / deluxe
        if not any(kw in low for kw in PLANETA_KEYWORDS):
            continue

        href = a["href"].strip()
        if href.startswith("http"):
            url_full = href
        else:
            url_full = PLANETA_BASE_URL.rstrip("/") + "/" + href.lstrip("/")

        results.append({"title": title, "url": url_full})

    return results

def check_planeta(send_func):
    print(f"{now_ts()} [PLANETA] Controllo novedades collector…", flush=True)

    seen = load_planeta_seen()
    new_seen = set(seen)
    new_count = 0

    try:
        items = fetch_planeta_news()
    except Exception as e:
        print(f"{now_ts()} [PLANETA][ERR] fetch_planeta_news: {e}", flush=True)
        return

    for item in items:
        key = item["url"]
        if key in seen:
            continue

        title = item["title"]
        link = item["url"]

        msg = (
            "🇪🇸 *Planeta Cómic – Nueva edición especial / cofre / deluxe!*\n"
            f"📚 *Titolo:* {title}\n"
            f"🔗 {link}"
        )

        send_func(msg, parse_mode="Markdown")
        new_seen.add(key)
        new_count += 1

        print(f"{now_ts()} [PLANETA] 🔔 New collector: {title}")

    if new_seen != seen:
        save_planeta_seen(new_seen)

    print(f"{now_ts()} [PLANETA] Completato. Nuove notifiche: {new_count}", flush=True)


def fetch_carlsen_month():
    """
    Legge la pagina 'Monatsübersicht' di Carlsen Manga! e
    filtra solo i titoli che sembrano special / limited
    usando alcune keyword nel testo della card.
    """

    print(f"{now_ts()} [CARLSEN] fetch Monatsübersicht…", flush=True)

    try:
        r = requests.get(
            CARLSEN_MONTH_URL,
            headers={"User-Agent": USER_AGENT},
            timeout=25,
        )
        if not r.ok:
            print(f"{now_ts()} [CARLSEN][ERR] HTTP {r.status_code} su Monatsübersicht", flush=True)
            return []
    except Exception as e:
        print(f"{now_ts()} [CARLSEN][ERR] richiesta fallita: {e}", flush=True)
        return []

    soup = BeautifulSoup(r.text, "html.parser")
    results = []

    # sulla pagina ogni print-neuheit è in un link tipo:
    #  "Taschenbuch Nausicaä ... im Schuber 98,00 €"
    for a in soup.find_all("a", href=True):
        text = " ".join(a.stripped_strings)
        if "€" not in text:
            continue

        lower = text.lower()
        if not any(kw in lower for kw in CARLSEN_KEYWORDS):
            # non sembra una special / limited
            continue

        # prova a separare formato + titolo + prezzo
        # es: "Taschenbuch Nausicaä ... im Schuber 98,00 €"
        before_euro = text.rsplit("€", 1)[0].strip()
        parts = before_euro.split(" ", 1)
        if len(parts) == 2:
            _, title = parts   # togli "Taschenbuch/Softcover/Hardcover"
        else:
            title = before_euro

        # prezzo = ultima "parola" prima del simbolo €
        price = text.split("€")[0].split()[-1] + " €"

        link = urljoin("https://www.carlsen.de", a["href"])
        date_info = "Neuheiten des Monats (Carlsen Monatsübersicht)"

        results.append((
            "CARLSEN",   # src
            title,
            link,
            price,
            date_info,
        ))

    return results

def check_carlsen(send_func):
    """
    Controlla le Neuheiten Carlsen e manda solo le special/limited
    non ancora viste (basato sul set globale 'seen').
    """
    print(f"{now_ts()} [CARLSEN] Controllo special/limited in corso…", flush=True)

    global seen
    new_count = 0

    items = fetch_carlsen_month()

    for (src, title, link, price, date_info) in items:
        key = f"CARLSEN|{title}|{link}"

        # se l'abbiamo già visto, saltiamo
        if key in seen:
            continue

        # segna come visto
        seen.add(key)

        msg = format_msg(src, "🇩🇪", title, link, price, date_info)
        send_func(msg, parse_mode="Markdown")

        # contatore giornaliero
        if "CARLSEN" not in state["daily_counts"]:
            state["daily_counts"]["CARLSEN"] = 0
        state["daily_counts"]["CARLSEN"] += 1

        new_count += 1
        print(f"{now_ts()} [CARLSEN] 🔔 Notified → {title}", flush=True)

    if new_count > 0:
        save_seen()

    print(f"{now_ts()} [CARLSEN] Completato. Nuove notifiche: {new_count}", flush=True)
    return new_count


# ========== PIKA – CHECK & MESSAGE ==========

def format_pika_message(item: dict) -> str:
    """
    Messaggio da mandare su Telegram.
    Adatta lo stile come preferisci.
    """
    title = item.get("title", "Titre inconnu")
    date_str = item.get("date_str", "?")
    url = item.get("url", "")

    return (
        "🇫🇷 *Pika – Édition speciale trovata!*\n"
        f"📚 {title}\n"
        f"📅 Uscita prevista: {date_str}\n"
        f"🔗 {url}"
    )


def check_pika(send_telegram):
    """
    Funzione da chiamare nel ciclo principale del bot.
    `send_telegram` deve essere una funzione che invia il messaggio al tuo canale.
    """
    print("[PIKA] Controllo éditions spéciales in corso...")

    seen = load_pika_seen()
    new_seen = set(seen)

    try:
        items = fetch_pika_upcoming_special_editions(max_pages=3)
    except Exception as e:
        print(f"[PIKA] Errore nel fetch: {e}")
        return

    # Ordina per data se disponibile
    items_sorted = sorted(
        items,
        key=lambda it: (it.get("date") is None, it.get("date") or datetime(9999, 12, 31).date()),
    )

    for item in items_sorted:
        pid = make_pika_id(item)
        if pid in seen:
            continue

        msg = format_pika_message(item)
        send_telegram(msg)
        new_seen.add(pid)

    if new_seen != seen:
        save_pika_seen(new_seen)

    print(f"[PIKA] Completato. Nuove edizioni notificate: {len(new_seen) - len(seen)}")


# ---------- KANA (via 9e-store) ----------

SEEN_KANA_FILE = Path("seen_kana.txt")


def load_seen_kana():
    if SEEN_KANA_FILE.exists():
        txt = SEEN_KANA_FILE.read_text("utf-8", errors="ignore")
        return set(line.strip() for line in txt.splitlines() if line.strip())
    return set()


def save_seen_kana(seen):
    try:
        SEEN_KANA_FILE.write_text("\n".join(sorted(seen)), encoding="utf-8")
    except Exception as e:
        print(f"{now_ts()} [KANA][ERR] save_seen_kana: {e}", flush=True)


def fetch_kana_html():
    url = "https://9e-store.fr/collectors-kana"
    headers = {
        "User-Agent": USER_AGENT,  # se l'hai già definito per gli altri scraper
        "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8,it;q=0.7",
    }
    r = requests.get(url, headers=headers, timeout=25)
    r.raise_for_status()
    return r.text, url


def parse_kana_collectors(html, base_url):
    """
    Parser super tollerante: prende tutti i prodotti della pagina collectors-kana
    e tiene quelli dove compaiono parole tipo 'collector' / 'coffret'.
    """
    soup = BeautifulSoup(html, "html.parser")
    items = []

    seen_hrefs = set()

    for a in soup.find_all("a", href=True):
        href = a["href"]
        text = " ".join(a.get_text(strip=True).split())
        if not text:
            continue

        low = (text + " " + href).lower()
        if "collector" not in low and "coffret" not in low:
            continue

        # prova a pescare un prezzo vicino (qualunque testo con il simbolo €)
        card = a.find_parent(["article", "div", "li"])
        price = ""
        if card:
            price_node = card.find(
                string=lambda s: isinstance(s, str) and "€" in s
            )
            if price_node:
                price = " ".join(price_node.strip().split())

        if href in seen_hrefs:
            continue
        seen_hrefs.add(href)

        if href.startswith("http"):
            full_url = href
        else:
            full_url = base_url.rstrip("/") + "/" + href.lstrip("/")

        items.append((text, full_url, price))

    return items


def check_kana(send_func):
    """
    Controlla i collectors KANA su 9e-store.
    Notifica solo link nuovi usando un file seen_kana.txt.
    """
    print(f"{now_ts()} [KANA] Controllo collectors Kana (9e-store)…", flush=True)

    html, base_url = fetch_kana_html()
    products = parse_kana_collectors(html, "https://9e-store.fr")

    seen = load_seen_kana()
    new_count = 0

    for title, link, price in products:
        key = link  # usiamo l'URL come chiave univoca

        if key in seen:
            continue

        # format messaggio
        price_part = f"\n💶 *Prezzo:* {price}" if price else ""
        msg = (
            "🇫🇷 *Kana (9e-store) – Collector / Coffret*\n"
            f"📚 *Titolo:* {title}{price_part}\n"
            f"🔗 {link}"
        )

        send_func(msg, parse_mode="Markdown")
        seen.add(key)
        new_count += 1
        print(f"{now_ts()} [KANA] 🔔 New collector: {title}", flush=True)

    save_seen_kana(seen)
    print(f"{now_ts()} [KANA] Completato. Nuove notifiche: {new_count}", flush=True)



# ----------------------------

API = f"https://api.telegram.org/bot{TOKEN}"

from urllib.parse import urlsplit, urlunsplit

def normalize_url(u: str) -> str:
    """Rimuove query e slash finali per evitare duplicati falsi."""
    if not u:
        return u
    try:
        s = urlsplit(u)
        scheme = "https"
        path = s.path.rstrip("/")
        return urlunsplit((scheme, s.netloc.lower(), path, "", ""))
    except Exception:
        return u


def tg_selfcheck_token():
    try:
        r = requests.get(f"{API}/getMe", timeout=15)
        j = r.json()
        if not j.get("ok"):
            print(f"{now_ts()} [FATAL] Telegram getMe ok=false → {j}")
            print(f"{now_ts()} [HINT] Token errato/revocato/copincollato male. "
                  f"Verifica in browser: https://api.telegram.org/bot<token>/getMe deve dare ok:true")
            return False
        print(f"{now_ts()} [OK] Telegram getMe: @{j['result']['username']}")
        return True
    except Exception as e:
        print(f"{now_ts()} [ERR] selfcheck getMe: {e}")
        return False

# parole chiave per titoli/prodotti (ampliate)
KEYWORDS = [
    r"\bvariant\b", r"variant cover", r"limited", r"limited edition",
    r"edizione\s*limitata", r"tiratura", r"collector", r"esclusiv[ao]",
    r"cover\s*B", r"cover\s*C", r"variant\s*silver", r"variant\s*gold"
]
KW_RE = re.compile("|".join(KEYWORDS), re.I)

# Sorgenti Italia / Francia (🇮🇹 / 🇫🇷)
SOURCES = {
    "PANINI": {
        "flag": "🇮🇹",
        "list_url": "https://www.panini.it/shp_ita_it/figurine-panini/ultime-uscite.html",
        "domain": "https://www.panini.it",
    },
    "PANINI_FR": {
        "flag": "🇫🇷",
        # lista generale manga Francia (non ultime uscite, ma filtriamo con KEYWORDS)
        "list_url": "https://www.panini.fr/shp_fra_fr/comics-mangas-magazines/mangas.html",
        "domain": "https://www.panini.fr",
    },
    "STAR": {
        "flag": "🇮🇹",
        "list_url": "https://www.starcomics.com/catalogo-fumetti",
        "domain": "https://www.starcomics.com",
    },
    "J-POP": {
        "flag": "🇮🇹",
        "list_url": "https://j-pop.it/it/catalog/category/view/id/696/s/fumetti/?product_list_dir=asc&product_list_order=name",
        "domain": "https://j-pop.it",
    },
}

STORE_FILE = "seen.json"
STATE_FILE = "state.json"  # last_heartbeat_date, counters, etc.

# Stato in memoria
seen = set()
state = {
    "last_heartbeat_date": None,
    "last_heartbeat_ts": None,
    "daily_counts": {
        "PANINI": 0,
        "PANINI_FR": 0,
        "STAR": 0,
        "J-POP": 0,
        "CARLSEN": 0,
        "TOKYOPOP_DE": 0,
    },
}




# ---------- Auto-Discovery config ----------
AUTODISCOVERY_FILE = "autodiscovery.json"

HOME_URLS = {
    "PANINI":    "https://www.panini.it/",
    "PANINI_FR": "https://www.panini.fr/",
    "STAR":      "https://www.starcomics.com/",
    "J-POP":     "https://j-pop.it/",
}


# parole chiave per trovare la lista prodotti partendo dalla home
DISCOVERY_KEYWORDS = {
    "PANINI": ["novita", "novità", "nuove-uscite", "uscite", "preordini", "fumetti", "manga", "catalogo"],
    "PANINI_FR": ["nouveautes", "nouveautés", "sorties", "precommande", "précommande", "manga", "catalogue"],
    "STAR":   ["novita", "novità", "uscite", "prossime-uscite", "preordini", "titoli", "shop", "catalogo"],
    "J-POP":  ["novita", "novità", "catalogo", "fumetti", "manga", "preorder", "preordini", "uscite"],
}



# firme (CSS selectors) che identificano una pagina-lista "valida" per ciascuna fonte
LIST_SIGNATURES = {
    "PANINI":    ["a.product-item-link"],
    "PANINI_FR": ["a.product-item-link"],
    "STAR":      ["div.card-body a[href]", "a[href*='/titoli-']"],
    "J-POP":     ["a.product-item-link", "a.product-item__title"],
}


# ---------- Robust headers + get_soup (retry + redirects) ----------
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "it-IT,it;q=0.9,en-US;q=0.8,en;q=0.7",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "DNT": "1",
}

def get_soup(url, timeout=30):
    """Robust GET with small retries and redirects allowed."""
    last_resp = None
    for _ in range(2):
        try:
            r = requests.get(url, timeout=timeout, headers=HEADERS, allow_redirects=True)
            last_resp = r
            if 200 <= r.status_code < 300:
                return BeautifulSoup(r.text, "html.parser")
        except Exception:
            # continue to retry once
            pass
        time.sleep(1.0)
    # if we get here, raise last response error if exists (to be handled by caller)
    if last_resp is not None:
        last_resp.raise_for_status()
    # fallback: raise a generic exception
    raise RuntimeError(f"Failed to GET {url}")



# --------- Utility ---------
def now_ts():
    return datetime.now().strftime("[%H:%M:%S]")
print(f"{now_ts()} [BOOT] running file = {os.path.abspath(__file__)}")
print(f"{now_ts()} [BOOT] token_tail = {TOKEN[-8:]}  chat_id = {CHAT_ID}")

def today_str():
    return date.today().isoformat()

def load_seen():
    global seen
    if os.path.exists(STORE_FILE):
        try:
            seen = set(json.load(open(STORE_FILE, "r", encoding="utf-8")))
        except Exception:
            seen = set()

def save_seen():
    try:
        json.dump(sorted(list(seen)), open(STORE_FILE, "w", encoding="utf-8"))
    except Exception:
        pass

def load_state():
    global state
    if os.path.exists(STATE_FILE):
        try:
            state.update(json.load(open(STATE_FILE, "r", encoding="utf-8")))
        except Exception:
            pass

def save_state():
    try:
        json.dump(state, open(STATE_FILE, "w", encoding="utf-8"))
    except Exception:
        pass

def reset_daily_if_needed():
    # azzera contatori se è cambiato il giorno
    if state.get("last_heartbeat_date") != today_str():
        state["daily_counts"] = {
            "PANINI": 0,
            "PANINI_FR": 0,
            "STAR": 0,
            "J-POP": 0,
            "CARLSEN": 0,
            "TOKYOPOP_DE": 0,   # <-- QUI underscore, uguale allo state iniziale
        }
        save_state()



# ---------- Auto-Discovery helpers ----------

def load_autodiscovery():
    if os.path.exists(AUTODISCOVERY_FILE):
        try:
            return json.load(open(AUTODISCOVERY_FILE, "r", encoding="utf-8"))
        except Exception:
            return {}
    return {}

def save_autodiscovery(cache):
    try:
        json.dump(cache, open(AUTODISCOVERY_FILE, "w", encoding="utf-8"),
                  indent=2, ensure_ascii=False)
    except Exception:
        pass

def page_matches_signature(src_key, soup):
    sigs = LIST_SIGNATURES.get(src_key, [])
    for css in sigs:
        if soup.select_one(css):
            return True
    return False


def discover_list_url(src_key):
    """Prova a scoprire automaticamente la pagina 'lista prodotti' partendo dalla home."""

    # 🔒 STAR: niente auto-discovery, usa sempre l'URL fisso da SOURCES
    if src_key == "STAR":
        print(f"{now_ts()} [Auto-Discovery] ⭐ STAR disabilitato → uso list_url fisso: {SOURCES['STAR']['list_url']}")
        return SOURCES["STAR"]["list_url"]

    try:
        home = HOME_URLS[src_key]
        kws = DISCOVERY_KEYWORDS.get(src_key, [])
        cache = load_autodiscovery()

        # 1️⃣ verifica cache
        cached = cache.get(src_key)
        if cached:
            try:
                soup = get_soup(cached, timeout=40)
                if page_matches_signature(src_key, soup):
                    print(f"{now_ts()} [Auto-Discovery] ✅ {src_key} URL ok (cache) → {cached}")
                    SOURCES[src_key]["list_url"] = cached
                    return cached
            except Exception:
                print(f"{now_ts()} [Auto-Discovery] ⚠️ {src_key} cache fallita, riscan...")

        # 2️⃣ scandaglia homepage
        home_soup = get_soup(home, timeout=40)
        links = set()
        for a in home_soup.select("a[href]"):
            href = (a.get("href") or "").strip()
            if not href:
                continue
            if href.startswith("//"):
                href = "https:" + href
            elif href.startswith("/"):
                href = home.rstrip("/") + href
            domain = HOME_URLS[src_key].split("//")[1].split("/")[0]
            if domain in href:
                links.add(href)

        # 3️⃣ ranking per keyword
        ranked = []
        weights = {
            "novita": 3, "novità": 3, "uscite": 3, "prossime-uscite": 3,
            "preordini": 3, "preorder": 3,
            "fumetti": 1, "manga": 1, "catalogo": 0
        }
        for url in links:
            low = url.lower()
            score = 0
            for k, w in weights.items():
                if k in low:
                    score += w
            ranked.append((score, url))

        ranked.sort(reverse=True)

        # 4️⃣ prova i migliori candidati
        for score, url in ranked[:50]:
            try:
                soup = get_soup(url, timeout=40)
                if page_matches_signature(src_key, soup):
                    print(f"{now_ts()} [Auto-Discovery] 🔄 {src_key} trovato → {url}")
                    SOURCES[src_key]["list_url"] = url
                    cache[src_key] = url
                    save_autodiscovery(cache)
                    print(f"{now_ts()} [Auto-Discovery] 💾 salvato → {AUTODISCOVERY_FILE}")
                    return url
            except Exception:
                continue

        print(f"{now_ts()} [Auto-Discovery] ❌ {src_key} nessun candidato valido.")
        return SOURCES[src_key]["list_url"]
    except Exception as e:
        print(f"{now_ts()} [Auto-Discovery][ERR] {src_key}: {e}")
        return SOURCES[src_key]["list_url"]



def tg_send(text, parse_mode=None, disable_web_page_preview=True):
    data = {"chat_id": CHAT_ID, "text": text, "disable_web_page_preview": disable_web_page_preview}
    if parse_mode:
        data["parse_mode"] = parse_mode
    try:
        requests.post(f"{API}/sendMessage", data=data, timeout=20)
    except Exception:
        # non bloccare mai il loop per errori di rete
        pass

def tg_heartbeat_once_per_day(force=False):
    """Invia un heartbeat se:
       - è forzato, oppure
       - è cambiato il giorno, oppure
       - sono passate >= 24 ore dall'ultimo heartbeat.
    """
    try:
        now = datetime.now()
        today = today_str()
        last_date = state.get("last_heartbeat_date")
        last_ts   = state.get("last_heartbeat_ts")

        need = False
        if force:
            need = True
        elif last_date != today:
            need = True
        elif last_ts:
            try:
                last_dt = datetime.fromisoformat(last_ts)
                if (now - last_dt).total_seconds() >= 24*3600:
                    need = True
            except Exception:
                need = True  # se il parse fallisce, invia

        if need:
            msg = f"🧠 Sistema attivo – Ultima scansione completata {now.strftime('%H:%M')}"
            tg_send(msg)
            state["last_heartbeat_date"] = today
            state["last_heartbeat_ts"] = now.isoformat(timespec="seconds")
            save_state()
    except Exception:
        pass


def extract_price_text(text):
    # cattura formati tipo €7,90 / 7,90 € / € 7.90 / $11.99 / ¥980
    m = re.search(r"((€|\$|¥|R\$)\s?\d{1,3}([.,]\d{2})?)|(\d{1,3}([.,]\d{2})?\s?(€|\$|¥))", text)
    return m.group(0).strip() if m else None

def extract_date_text(text):
    # cerca pattern comuni in IT (estendibile)
    # es: "Uscita: 12 Novembre 2025", "Disponibile dal 10/11/2025"
    patterns = [
        r"(Uscita|In uscita|Disponibile dal|Disponibile da|Release|Data)\s*[:\-]?\s*([0-3]?\d[\/\-\s][0-1]?\d[\/\-\s]\d{4}|[0-3]?\d\s+\w+\s+\d{4})",
        r"(uscita il|in uscita il)\s*([0-3]?\d[\/\-\s][0-1]?\d[\/\-\s]\d{4}|[0-3]?\d\s+\w+\s+\d{4})",
    ]
    for pat in patterns:
        m = re.search(pat, text, re.I)
        if m:
            return m.group(0)
    return None

def extract_availability(text, soup=None, src_key=None):
    t = (text or "").lower()

    # segnali testuali generici
    if any(x in t for x in ["non disponibile", "esaurito", "sold out", "esaurita"]):
        return "SOLDOUT"
    if any(x in t for x in ["disponibile", "in stock", "disponibilità immediata"]):
        return "AVAILABLE"
    if any(x in t for x in ["preordine", "pre-ordine", "preorder", "in uscita", "uscita il", "prossima uscita", "disponibile dal"]):
        return "PREORDER"

    # segnali CSS per sito specifico
    try:
        if soup is not None:
            if src_key == "STAR":
                if soup.select_one(".badge-esaurito, .product-status--unavailable, .soldout"):
                    return "SOLDOUT"
                if soup.select_one(".product-status--available, .available, .in-stock"):
                    return "AVAILABLE"
                if soup.select_one(".product-status--preorder, .preorder"):
                    return "PREORDER"
            elif src_key == "PANINI":
                if soup.select_one(".stock.unavailable, .out-of-stock"):
                    return "SOLDOUT"
            elif src_key == "J-POP":
                if soup.select_one(".out-of-stock, .product-unavailable"):
                    return "SOLDOUT"
    except Exception:
        pass

    return "UNKNOWN"


# ---------- Site parsers (list pages) ----------

def list_panini(src_key):
    """
    Lista Panini Italia / Francia.
    src_key deve essere "PANINI" oppure "PANINI_FR".
    """
    url = SOURCES[src_key]["list_url"]
    domain = SOURCES[src_key]["domain"]

    soup = get_soup(url)
    items = []

    # metto più selettori perché Panini cambia spesso i temi
    for a in soup.select("a.product-item-link, a.product-item__link, a.product-item__title"):
        title = a.get_text(strip=True)
        link = a.get("href") or ""
        if not link:
            continue

        # link relativo → completo
        if not link.startswith("http"):
            link = domain.rstrip("/") + link

        # filtro con parole chiave (variant, limited, collector, ecc.)
        if KW_RE.search(title):
            items.append((title, link))

    return items


def list_star():
    url = SOURCES["STAR"]["list_url"]
    soup = get_soup(url)
    items = []

    # layout desktop
    for a in soup.select("div.card-body a[href]"):
        title = a.get_text(strip=True)
        href = a.get("href", "")
        if href and not href.startswith("http"):
            href = SOURCES["STAR"]["domain"] + href
        if href and KW_RE.search(title):
            items.append((title, href))

    # (fallback) eventuale layout mobile/alternativo
    if not items:
        for a in soup.select("a[href*='/titoli-']"):
            title = a.get_text(strip=True)
            href = a.get("href", "")
            if href and not href.startswith("http"):
                href = SOURCES["STAR"]["domain"] + href
            if href and KW_RE.search(title):
                items.append((title, href))

    return items


def list_jpop():
    url = SOURCES["J-POP"]["list_url"]
    soup = get_soup(url)
    items = []

    for a in soup.select("a.product-item-link, a.product-item__title"):
        title = a.get_text(strip=True)
        link = a.get("href", "")
        if not link:
            continue
        if not link.startswith("http"):
            link = SOURCES["J-POP"]["domain"] + link
        if KW_RE.search(title):
            items.append((title, link))

    return items


# ---------- Detail page enrichment ----------
def enrich_detail(src_key, title, link):
    """Scarica la pagina prodotto e prova ad estrarre prezzo, data e disponibilità."""
    price = None
    date_info = None
    avail = "UNKNOWN"  # default di sicurezza
    try:
        soup = get_soup(link, timeout=40)
        full_text = soup.get_text(" ", strip=True)

        # --- prezzo per sito ---
        if src_key.startswith("PANINI"):
            el = soup.select_one("span.price")
            if el:
                price = extract_price_text(el.get_text(" ", strip=True))
        elif src_key == "STAR":
            el = soup.select_one(".price, .prezzo, strong")
            if el:
                price = extract_price_text(el.get_text(" ", strip=True))
        elif src_key == "J-POP":
            el = soup.select_one("span.price, .product-info__price, .price__value")
            if el:
                price = extract_price_text(el.get_text(" ", strip=True))

        # fallback prezzo sul testo intero
        if not price:
            price = extract_price_text(full_text)

        # --- data uscita/disponibilità testuale ---
        date_info = extract_date_text(full_text)

        # --- disponibilità (usa testo + segnali CSS specifici) ---
        avail = extract_availability(full_text, soup=soup, src_key=src_key)

    except Exception:
        pass

    return price, date_info, avail



# ---------- Scan orchestration ----------
def format_msg(src_key, flag, title, link, price=None, date_info=None):
    parts = []
    parts.append(f"📌 [{flag} {src_key}]")
    parts.append(f"📚 *{title}*")
    if price:
        parts.append(f"💰 Prezzo: {price}")
    if date_info:
        parts.append(f"🗓️ {date_info}")
    parts.append(f"🔗 {link}")
    return "\n".join(parts)
MAX_AGE_DAYS = 60       # massimo "vecchia" consentita
ALLOW_SOLD_OUT = False   # se False, non notificare esauriti

def parse_date_from_text(date_info):
    if not date_info:
        return None
    # dd/mm/yyyy
    try:
        m = re.search(r"([0-3]?\d)[\/\-\s]([0-1]?\d)[\/\-\s](\d{4})", date_info)
        if m:
            d, mo, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
            return datetime(y, mo, d)
    except Exception:
        pass
    # 12 Novembre 2025
    try:
        mesi = {
            "gennaio":1,"febbraio":2,"marzo":3,"aprile":4,"maggio":5,"giugno":6,
            "luglio":7,"agosto":8,"settembre":9,"ottobre":10,"novembre":11,"dicembre":12
        }
        m = re.search(r"([0-3]?\d)\s+([A-Za-zàèéìòù]+)\s+(\d{4})", date_info, re.I)
        if m:
            d = int(m.group(1)); mo =  mesi.get(m.group(2).lower(), 0); y = int(m.group(3))
            if mo:
                return datetime(y, mo, d)
    except Exception:
        pass
    return None

MAX_AGE_DAYS = 120
ALLOW_SOLD_OUT = False

def should_notify(avail, date_info):
    if avail == "SOLDOUT" and not ALLOW_SOLD_OUT:
        return False

    dt = parse_date_from_text(date_info)

    if avail == "PREORDER":
        if not dt:
            return True
        # notifica preorder solo se entro 6 mesi
        return (dt - datetime.now()).days <= 180

    if dt and (datetime.now() - dt).days > MAX_AGE_DAYS:
        return False

    return True



def scan_source(src_key):
    """
    Ritorna lista di (src_key, title, link, price, date_info) NUOVI (non visti).
    Vale per: PANINI, PANINI_FR, STAR, J-POP.
    """
    out = []
    try:
        if src_key in ("PANINI", "PANINI_FR"):
            items = list_panini(src_key)
        elif src_key == "STAR":
            items = list_star()
        elif src_key == "J-POP":
            items = list_jpop()
        else:
            items = []
    except Exception as e:
        print(f"{now_ts()} [WARN] {src_key} fetch error: {e}")
        return out

    new_count = 0
    for title, link in items:
        key = f"{src_key}|{link}"
        if key in seen:
            continue

        # arricchisci con prezzo/data/availability
        price, date_info, avail = enrich_detail(src_key, title, link)

        if should_notify(avail, date_info):
            out.append((src_key, title, link, price, date_info))
            seen.add(key)
            new_count += 1
        else:
            # silenzia ma segna come visto per non ripeterlo ad ogni ciclo
            seen.add(key)

    if new_count:
        save_seen()

    print(f"{now_ts()} Scan {src_key}: {len(items)} hits, {new_count} new")
    return out


def scan_all_and_notify():
    reset_daily_if_needed()
    total_new = 0

    # --- Fonti gestite tramite SOURCES (Italia/FR/J-POP) ---
    for src_key in ["PANINI", "PANINI_FR", "STAR", "J-POP"]:
        flag = SOURCES[src_key]["flag"]
        for (src, title, link, price, date_info) in scan_source(src_key):
            msg = format_msg(src, flag, title, link, price, date_info)
            tg_send(msg, parse_mode="Markdown")
            state["daily_counts"][src_key] += 1
            total_new += 1
            print(f"{now_ts()} 🔔 Notified → {src}: {title}")

    # --- PIKA Francia ---
    try:
        check_pika(tg_send)
    except Exception as e:
        print(f"{now_ts()} [PIKA][ERR] {e}", flush=True)

    # --- Ki-oon Francia (news coffrets / collector) ---
    try:
        check_kioon(tg_send)
    except Exception as e:
        print(f"{now_ts()} [KIOON][ERR] {e}", flush=True)

   # --- KUROKAWA Francia ---
    try:
        check_kuro(tg_send)
    except Exception as e:
        print(f"{now_ts()} [KURO][ERR] {e}", flush=True)

    # --- KANA (9e-store collectors) ---
    try:
        check_kana(tg_send)
    except Exception as e:
        print(f"{now_ts()} [KANA][ERR] {e}", flush=True)

    # --- NORMA Editorial (Spagna) ---
    try:
        check_norma(tg_send)
    except Exception as e:
        print(f"{now_ts()} [NORMA][ERR] {e}", flush=True)

    # --- IVREA España ---
    try:
        check_ivrea(tg_send)
    except Exception as e:
        print(f"{now_ts()} [IVREA][ERR] {e}", flush=True)

    # --- Milky Way Ediciones (Spagna) ---
    try:
        check_milkyway(tg_send)
    except Exception as e:
        print(f"{now_ts()} [MILKY][ERR] {e}", flush=True)

    # --- Planeta Cómic (Spagna) ---
    try:
        check_planeta(tg_send)
    except Exception as e:
        print(f"{now_ts()} [PLANETA][ERR] {e}", flush=True)

    # --- GERMANIA: Carlsen Manga (special/limited) ---
    try:
        total_new += check_carlsen(tg_send)
    except Exception as e:
        print(f"{now_ts()} [CARLSEN][ERR] {e}", flush=True)
    # --- TOKYOPOP Germania ---
    try:
        check_tokyopop_de(tg_send)
    except Exception as e:
        print(f"{now_ts()} [TOKYOPOP-DE][ERR] {e}", flush=True)


    # --- report finale per le fonti del loop principale ---
    if total_new == 0:
        print(f"{now_ts()} No new variants.")
    else:
        save_state()

    counts = state["daily_counts"]
    print(
        f"{now_ts()} Today: "
        f"PANINI {counts['PANINI']} | "
        f"PANINI_FR {counts['PANINI_FR']} | "
        f"STAR {counts['STAR']} | "
        f"J-POP {counts['J-POP']} | "
        f"CARLSEN {counts['CARLSEN']} | "
        f"TOKYOPOP_DE {counts['TOKYOPOP_DE']}"
    )




# ---------- Telegram commands ----------
def get_updates(offset=None, timeout=25):
    params = {"timeout": timeout}
    if offset is not None:
        params["offset"] = offset

    try:
        r = requests.get(f"{API}/getUpdates", params=params, timeout=timeout + 5)
        if not r.ok:
            print(f"{now_ts()} [POLL][WARN] HTTP {r.status_code} on getUpdates", flush=True)
            return []

        data = r.json()
        result = data.get("result", [])

        # ci assicuriamo di restituire SEMPRE una lista
        if isinstance(result, list):
            return result

        print(f"{now_ts()} [POLL][WARN] unexpected result type {type(result)} → forcing empty list", flush=True)
        return []

    except Exception as e:
        print(f"{now_ts()} [POLL][ERR] get_updates exception {e}", flush=True)
        return []



def handle_command(text):
    """
    Gestione dei comandi Telegram.
    Comandi disponibili:
    /ping        → test rapido
    /heartbeat   → aggiorna heartbeat e conferma che il bot è vivo
    /status      → stato attuale + contatori giornalieri
    /help        → lista comandi
    """

    t = text.strip().lower()

    # --- /ping ---
    if t == "/ping":
        tg_send("🏓 Pong!")
        return

    # --- /heartbeat ---
    elif t == "/heartbeat":
        state["last_heartbeat_ts"] = time.time()
        state["last_heartbeat_date"] = today_str()
        save_state()
        tg_send("❤️ Heartbeat ricevuto. Tutto ok!")
        return

# --- /status ---
# --- /status ---
elif t == "/status":
    counts = state["daily_counts"]

    msg = "📊 *Stato del bot*\n"
    msg += "------------------------\n"
    msg += "🟢 Attivo e in esecuzione\n"
    msg += "🌍 Fonti monitorate:\n"

    # 🇮🇹 ITALIA
    msg += "\n🇮🇹 *Italia*\n"
    msg += "• Panini IT\n"
    msg += "• Star Comics\n"
    msg += "• J-POP\n"
    msg += "• Norma (IT import)\n"
    msg += "• Ivrea IT\n"

    # 🇫🇷 FRANCIA
    msg += "\n🇫🇷 *Francia*\n"
    msg += "• Panini FR\n"
    msg += "• Pika\n"
    msg += "• Kana\n"
    msg += "• Ki-oon\n"
    msg += "• Kurokawa\n"

    # 🇩🇪 GERMANIA
    msg += "\n🇩🇪 *Germania*\n"
    msg += "• Panini DE\n"
    msg += "• Carlsen\n"
    msg += "• Tokyopop DE\n"

    # 🇪🇸 SPAGNA
    msg += "\n🇪🇸 *Spagna*\n"
    msg += "• Planeta Comic\n"
    msg += "• MilkyWay ES\n"
    msg += "• Panini ES\n"

    msg += "\n------------------------\n"
    msg += "📅 *Oggi:*\n"
    msg += f"🇮🇹 Panini IT: {counts['PANINI']}\n"
    msg += f"🇫🇷 Panini FR: {counts['PANINI_FR']}\n"
    msg += f"⭐ Star Comics: {counts['STAR']}\n"
    msg += f"🇯🇵 J-POP: {counts['J-POP']}\n"

    # Puoi aggiungere anche questi se hai i contatori:
    # msg += f"🇩🇪 Panini DE: {counts['PANINI_DE']}\n"
    # msg += f"🇪🇸 Panini ES: {counts['PANINI_ES']}\n"
    # msg += f"🇪🇸 MilkyWay ES: {counts['MILKYWAY']}\n"
    # msg += f"🇪🇸 Planeta: {counts['PLANETA']}\n"
    # msg += f"🇩🇪 Carlsen: {counts['CARLSEN']}\n"
    # msg += f"🇩🇪 Tokyopop DE: {counts['TOKYOPOP_DE']}\n"

    tg_send(msg, parse_mode="Markdown")
    return


    # --- /help ---
    elif t == "/help":
        tg_send(
            "🛠 *Comandi disponibili*\n"
            "------------------------\n"
            "• /ping → Test rapido\n"
            "• /heartbeat → Conferma attività\n"
            "• /status → Stato bot + contatori\n"
            "• /help → Questo menu\n",
            parse_mode="Markdown"
        )
        return

    # --- Comando sconosciuto ---
    else:
        tg_send("❓ Comando non riconosciuto. Scrivi /help per la lista completa.")
        return



def polling_loop():
    global last_poll_at, polling_alive
    polling_alive = True
    last_poll_at = time.time()
    print(f"{now_ts()} [DBG] polling thread started", flush=True)
    offset = None
    while True:
        try:
            updates = get_updates(offset=offset)
            if not isinstance(updates, list):
                print(f"{now_ts()} [POLL][WARN] get_updates() returned {type(updates)} → skip", flush=True)
                time.sleep(2)
                continue

            last_poll_at = time.time()
            for u in updates:
                offset = u.get("update_id", 0) + 1

                msg = u.get("message") or u.get("edited_message")
                if not msg:
                    continue

                chat = msg.get("chat", {}).get("id")
                if str(chat) != str(CHAT_ID):
                    continue

                text = (msg.get("text") or "").strip()
                if text.startswith("/"):
                    handle_command(text)

        except Exception as e:
            print(f"{now_ts()} [POLL][ERR] {e}", flush=True)
            time.sleep(2)



def monitor_loop():
    load_seen()
    load_state()

    # ---------- Auto-Discovery attiva ----------
    try:
        print(f"{now_ts()} [BOOT] avvio auto-discovery...", flush=True)
        for src_key in ["PANINI", "STAR", "J-POP"]:
            discover_list_url(src_key)
        print(f"{now_ts()} [BOOT] auto-discovery completata.", flush=True)
    except Exception as e:
        print(f"{now_ts()} [BOOT][ERR] auto-discovery: {e}", flush=True)

    tg_send("🛰️ Monitor avviato (Panini/Star/J-POP).", parse_mode="Markdown")
    print(f"{now_ts()} [MON] Monitor started (v2.0 Italia Pro).", flush=True)

    while True:
        try:
            print(f"{now_ts()} [MON] avvio scansione…", flush=True)

            # qui dentro fa tutto: PANINI, PANINI_FR, STAR, J-POP + PIKA
            scan_all_and_notify()

            # heartbeat giornaliero
            tg_heartbeat_once_per_day()

            print(f"{now_ts()} [MON] scansione completata.", flush=True)

            # alive tick ogni 3 minuti (180s circa)
            now_int = int(time.time())
            if now_int % 180 == 0:
                print(f"{now_ts()} [MON] alive tick…", flush=True)

            # pausa tra una scansione e l'altra
            time.sleep(180)

        except Exception as e:
            print(f"{now_ts()} [MON][ERR] loop: {e}", flush=True)
            # in caso di errore, aspetta un po' e poi riprova
            time.sleep(30)




def polling_loop():
    """Thread che ascolta i comandi Telegram (/ping, /status, …)."""
    print(f"{now_ts()} [DBG] polling thread started", flush=True)
    offset = None
    while True:
        try:
            updates = get_updates(offset=offset)
            if not isinstance(updates, list):
                print(f"{now_ts()} [POLL][WARN] get_updates() returned {type(updates)} -> skip", flush=True)
                time.sleep(2)
                continue

            for u in updates:
                offset = u.get("update_id", 0) + 1

                msg = u.get("message") or u.get("edited_message")
                if not msg:
                    continue

                chat = msg.get("chat", {}).get("id")
                if str(chat) != str(CHAT_ID):
                    # ignora chat non tue
                    continue

                text = msg.get("text", "")
                if text.startswith("/"):
                    handle_command(text)

        except Exception as e:
            print(f"{now_ts()} [POLL][ERR] {e}", flush=True)
            time.sleep(2)


if __name__ == "__main__":
    print(f"{now_ts()} [BOOT] running file = {os.path.abspath(__file__)}", flush=True)
    print(f"{now_ts()} [BOOT] token_tail = {TOKEN[-8:]}   chat_id = {CHAT_ID}", flush=True)

    # avvia thread di polling comandi
    t = threading.Thread(target=polling_loop, daemon=True)
    t.start()

    # avvia monitor principale (non termina mai)
    monitor_loop()

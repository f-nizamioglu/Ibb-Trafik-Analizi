import logging
import os
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from config import ensure_cli_logging

logger = logging.getLogger(__name__)

# Ana veri seti sayfasi
DATASET_URL = "https://data.ibb.gov.tr/dataset/saatlik-trafik-yogunluk-veri-seti"
# Project convention: same folder name as `CSV_DIR` in `config` (path is relative to CWD).
DOWNLOAD_DIR = "ibb_trafik_verileri"

INDEX_PAGE_REQUEST_TIMEOUT_S = 30
FILE_DOWNLOAD_TIMEOUT_S = 60
DOWNLOAD_STREAM_CHUNK_BYTES = 8192

# Bot korumasina takilmamak icin tarayici basligi ekliyoruz
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}

def download_files_smart():
    ensure_cli_logging()
    if not os.path.exists(DOWNLOAD_DIR):
        os.makedirs(DOWNLOAD_DIR)
        logger.info("'%s' klasoru hazir.\n", DOWNLOAD_DIR)

    logger.info("Sayfa analiz ediliyor: %s ...", DATASET_URL)

    try:
        response = requests.get(
            DATASET_URL, headers=HEADERS, timeout=INDEX_PAGE_REQUEST_TIMEOUT_S, verify=True
        )
        response.raise_for_status()
    except Exception as e:
        logger.warning("Sayfaya erisilemedi: %s", e)
        return

    soup = BeautifulSoup(response.text, 'html.parser')

    # Icerisinde '/download/' gecen butun linkleri (href) topla
    download_links = []
    for a in soup.find_all('a', href=True):
        href = a['href']
        if '/download/' in href or href.endswith('.csv'):
            # Goreceli linkleri (relative url) tam linke cevir
            full_url = urljoin(DATASET_URL, href)
            download_links.append(full_url)

    # Ayni butondan 2 tane varsa (mobil/masaustu gorunumu) listeyi tekillestir
    download_links = list(set(download_links))

    if not download_links:
        logger.warning("Sayfada indirme linki bulunamadi. Sitenin yapisi incelenmeli.")
        return

    logger.info(
        "\nSistemde toplam %s adet CSV baglantisi tespit edildi. Indirme basliyor...\n",
        len(download_links),
    )

    for i, link in enumerate(download_links, 1):
        # Linkin son kismini (ornek: ocak_2020.csv) dosya adi olarak al
        filename = link.split('/')[-1]

        # URL parametreleri (?v=1 vs) varsa temizle
        if '?' in filename:
            filename = filename.split('?')[0]

        if not filename.endswith('.csv'):
            filename += '.csv'

        filepath = os.path.join(DOWNLOAD_DIR, filename)

        # Dosya zaten inmişse atla (script koparsa kaldigi yerden devam etmesi icin)
        if os.path.exists(filepath):
            logger.info("[%s/%s] Zaten mevcut: %s", i, len(download_links), filename)
            continue

        logger.info("[%s/%s] Indiriliyor: %s", i, len(download_links), filename)

        try:
            # stream=True ile buyuk dosyalari RAM'i sismeden parca parca indiriyoruz
            file_resp = requests.get(
                link, headers=HEADERS, stream=True, timeout=FILE_DOWNLOAD_TIMEOUT_S, verify=True
            )
            file_resp.raise_for_status()

            with open(filepath, 'wb') as f:
                for chunk in file_resp.iter_content(chunk_size=DOWNLOAD_STREAM_CHUNK_BYTES):
                    if chunk:
                        f.write(chunk)
        except Exception as e:
            logger.error("-> HATA! %s indirilemedi: %s", filename, e)

    logger.info("\nTum indirme islemleri tamamlandi!")

if __name__ == "__main__":
    download_files_smart()

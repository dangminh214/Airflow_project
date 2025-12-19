import json
import boto3
import requests
from bs4 import BeautifulSoup
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import time
import random
import xml.etree.ElementTree as ET


def create_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS market_news (
            id SERIAL PRIMARY KEY,
            title TEXT,
            url TEXT,
            scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
    conn.commit()


# def scrape_yahoo_finance_news(max_retries=3):
#     url = "https://finance.yahoo.com/news"

#     headers = {
#         "User-Agent": (
#             "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
#             "AppleWebKit/537.36 (KHTML, like Gecko) "
#             "Chrome/120.0.0.0 Safari/537.36"
#         ),
#         "Accept-Language": "en-US,en;q=0.9",
#         "Accept": "text/html",
#         "Connection": "keep-alive",
#     }

#     session = requests.Session()

#     retry_strategy = Retry(
#         total=3,
#         backoff_factor=2,
#         status_forcelist=[429, 500, 502, 503, 504],
#         allowed_methods=["GET"],
#     )

#     adapter = HTTPAdapter(max_retries=retry_strategy)
#     session.mount("https://", adapter)

#     for attempt in range(1, max_retries + 1):
#         try:
#             response = session.get(
#                 url,
#                 headers=headers,
#                 timeout=(5, 15),  # connect, read
#             )
#             response.raise_for_status()

#             soup = BeautifulSoup(response.text, "html.parser")

#             articles = []
#             for item in soup.select("h3 a[href]"):
#                 title = item.get_text(strip=True)
#                 href = item.get("href")

#                 if not href.startswith("http"):
#                     href = "https://finance.yahoo.com" + href # type: ignore

#                 articles.append((title, href))

#             if not articles:
#                 raise ValueError("No articles parsed")

#             return articles

#         except Exception as e:
#             print(f"[Attempt {attempt}] Scrape failed: {e}")
#             time.sleep(random.uniform(3, 7))

#     print("Scraping failed after retries — returning empty list")
#     return []


def scrape_yahoo_finance_news():
    url = "https://finance.yahoo.com/rss/topstories"

    response = requests.get(url, timeout=10)
    response.raise_for_status()

    root = ET.fromstring(response.content)

    records = []
    for item in root.findall(".//item"):
        title = item.findtext("title")
        link = item.findtext("link")

        if title and link:
            records.append((title, link))

    return records


def load_to_postgres(records, pg_config):
    conn = psycopg2.connect(**pg_config)

    create_table(conn)

    with conn.cursor() as cur:
        execute_batch(
            cur,
            """
            INSERT INTO market_news (title, url)
            VALUES (%s, %s)
            ON CONFLICT DO NOTHING
            """,
            records,
        )

    conn.commit()
    conn.close()


def upload_to_minio(records):
    s3 = boto3.client(
        "s3",
        endpoint_url="http://host.docker.internal:9000",
        aws_access_key_id="admin",
        aws_secret_access_key="strongpassword123",
        region_name="eu-central-1",
    )

    bucket_name = "finance-raw"

    try:
        s3.head_bucket(Bucket=bucket_name)
    except:
        s3.create_bucket(Bucket=bucket_name)

    payload = [{"title": t, "url": u} for t, u in records]

    object_key = f"yahoo_news/{datetime.utcnow().isoformat()}.json"

    s3.put_object(
        Bucket=bucket_name,
        Key=object_key,
        Body=json.dumps(payload),
        ContentType="application/json",
    )

    print(f"Uploaded {len(payload)} records to MinIO → {object_key}")


def run():
    records = scrape_yahoo_finance_news()
    upload_to_minio(records)

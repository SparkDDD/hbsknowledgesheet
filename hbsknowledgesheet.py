import os
import json
import logging
import requests
from datetime import datetime
import gspread
from google.oauth2.service_account import Credentials

# Google Sheets Setup
SHEET_ID = "1bg0uvjRTU1ZA6kMNXTomCSqjj_1JOhotinEkcc8hlyw"
SHEET_NAME = "HBS"
SERVICE_ACCOUNT_JSON = os.environ.get("GOOGLE_SHEET_SERVICE_ACCOUNT_JSON")

# Google Sheets Columns (ordered as per user's header)
COLUMNS = [
    "Title", "Publication Date", "Author", "faculty", "Summary",
    "Article URL", "ImageFile URL", "Category", "New Category",
    "Object ID", "timestamp"
]

# Valid categories
all_valid_categories = {
    "Accounting", "Advertising", "AI at Work", "Artificial Intelligence", "Strategy", "Leadership", "Not Defined"
}

# Logging Setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s — %(levelname)s — %(message)s")
logger = logging.getLogger(__name__)

# Google Sheets Client Init
def init_sheet():
    if not SERVICE_ACCOUNT_JSON:
        raise ValueError("Environment variable 'GOOGLE_SHEET_SERVICE_ACCOUNT_JSON' is not set.")
    try:
        service_account_info = json.loads(SERVICE_ACCOUNT_JSON)
    except json.JSONDecodeError:
        raise ValueError("Invalid JSON in 'GOOGLE_SHEET_SERVICE_ACCOUNT_JSON'.")

    scope = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = Credentials.from_service_account_info(service_account_info, scopes=scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_key(SHEET_ID).worksheet(SHEET_NAME)
    logger.info("✅ Connected to Google Sheet")
    return sheet

def get_existing_object_ids(sheet):
    records = sheet.get_all_records()
    existing_ids = {row["Object ID"] for row in records if row.get("Object ID")}
    logger.info(f"📦 Loaded {len(existing_ids)} existing Object IDs from sheet")
    return existing_ids

def normalize_categories(topics):
    if not topics:
        return ["Not Defined"], []
    cleaned = [t for t in topics if isinstance(t, str)]
    allowed = [t for t in cleaned if t in all_valid_categories]
    new = [t for t in cleaned if t not in all_valid_categories]
    return allowed or ["Not Defined"], new

def build_article_row(hit):
    obj_id = hit.get("id")
    url = hit.get("url", "")
    date_str = hit.get("sortDate") or hit.get("display", {}).get("date")
    try:
        date_iso = datetime.fromisoformat(date_str.replace("Z", "")).date().isoformat() if date_str else ""
    except Exception:
        date_iso = ""

    authors = hit.get("author") or []
    if not authors:
        byline = hit.get("display", {}).get("byline", [])
        authors = [item.get("label", "") for item in byline] if isinstance(byline, list) else []
    author_str = ", ".join(authors)

    faculty = hit.get("faculty", [])
    faculty_str = ", ".join(faculty) if isinstance(faculty, list) else ""

    thumb = hit.get("display", {}).get("thumbnail", {}).get("src", "")
    image_url = "https:" + thumb if thumb.startswith("//") else thumb

    allowed_cats, new_cats = normalize_categories(hit.get("topic", []))
    current_timestamp = datetime.now().isoformat()

    return [
        hit.get("title", ""),
        date_iso,
        author_str,
        faculty_str,
        hit.get("description", ""),
        url,
        image_url,
        ", ".join(allowed_cats),
        ", ".join(new_cats) if new_cats else "",
        obj_id,
        current_timestamp,
    ]

def fetch_and_upload():
    logger.info("🚀 Starting HBS API scraper for Google Sheets...")
    try:
        sheet = init_sheet()
    except Exception as e:
        logger.error(f"❌ Initialization failed: {e}")
        return

    existing_ids = get_existing_object_ids(sheet)

    page_size = 10
    max_articles = 50
    articles_checked = 0
    batch_to_append = []

    for offset in range(0, max_articles, page_size):
        logger.info(f"🌐 Fetching page {offset // page_size + 1}")
        url = f"https://www.library.hbs.edu/api/search/query?from={offset}&size={page_size}&index=modern&facets=industry%2Cfaculty%2Cunit&filters=(subset:working-knowledge+AND+contentType:Article)&sort=sortDate:desc"

        try:
            response = requests.get(url)
            response.raise_for_status()
            hits = response.json().get("hits", [])
            if not hits:
                logger.info("✅ No more articles found.")
                break

            for hit in hits:
                if hit.get("id") not in existing_ids:
                    batch_to_append.append(build_article_row(hit))
                    articles_checked += 1

            if articles_checked >= max_articles:
                logger.info(f"✅ Checked max articles: {max_articles}")
                break

        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Fetch failed: {e}")
            break

    if batch_to_append:
        try:
            sheet.append_rows(batch_to_append, value_input_option="RAW")
            logger.info(f"✅ Uploaded {len(batch_to_append)} new articles.")
        except Exception as e:
            logger.error(f"❌ Error uploading batch: {e}")
    else:
        logger.info("ℹ️ No new articles to upload.")

    logger.info(f"✅ Finished checking {articles_checked} articles.")

if __name__ == "__main__":
    fetch_and_upload()

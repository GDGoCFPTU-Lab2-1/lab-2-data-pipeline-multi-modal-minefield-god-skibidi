import pandas as pd
from dateutil import parser as dateutil_parser
import re

# ==========================================
# ROLE 2: ETL/ELT BUILDER
# ==========================================
# Task: Process sales records, handling type traps and duplicates.

_WORD_TO_NUMBER = {
    "zero": 0, "one": 1, "two": 2, "three": 3, "four": 4,
    "five": 5, "six": 6, "seven": 7, "eight": 8, "nine": 9,
    "ten": 10, "eleven": 11, "twelve": 12,
    "twenty": 20, "thirty": 30, "fifty": 50, "hundred": 100,
    "thousand": 1000,
}

def _clean_price(raw_price) -> float | None:
    """Convert messy price values to float or None."""
    if pd.isna(raw_price):
        return None

    s = str(raw_price).strip().lower()

    # Reject non-numeric words like "null", "n/a", "liên hệ" 
    reject_patterns = ["null", "n/a", "liên hệ", "lien he", "contact"]
    for pat in reject_patterns:
        if pat in s:
            return None

    # Handle word-based numbers ("five dollars" -> 5.0)
    for word, val in _WORD_TO_NUMBER.items():
        if word in s:
            return float(val)

    # Strip currency symbols and commas
    s = re.sub(r"[\$,]", "", s)

    try:
        val = float(s)
        # Reject negative prices
        if val < 0:
            return None
        return val
    except ValueError:
        return None


def _normalize_date(raw_date) -> str | None:
    """Parse any date format to YYYY-MM-DD string."""
    if pd.isna(raw_date):
        return None
    try:
        return dateutil_parser.parse(str(raw_date), dayfirst=False).strftime("%Y-%m-%d")
    except Exception:
        try:
            return dateutil_parser.parse(str(raw_date), dayfirst=True).strftime("%Y-%m-%d")
        except Exception:
            return None


def process_sales_csv(file_path):
    df = pd.read_csv(file_path)

    # --- Remove duplicate rows based on 'id' (keep first occurrence) ---
    before = len(df)
    df = df.drop_duplicates(subset=["id"], keep="first")
    print(f"[CSV] Removed {before - len(df)} duplicate rows.")

    results = []
    for _, row in df.iterrows():
        price = _clean_price(row.get("price"))
        date  = _normalize_date(row.get("date_of_sale"))

        product_name  = str(row.get("product_name", "Unknown")).strip()
        category      = str(row.get("category", "Unknown")).strip()
        currency      = str(row.get("currency", "VND")).strip()
        seller_id     = str(row.get("seller_id", "Unknown")).strip()
        stock_qty     = row.get("stock_quantity")

        # Clean display values for content string to avoid "None" or "nan"
        disp_price = f"{price}" if price is not None else "N/A"
        disp_date = date if date is not None else "N/A"
        disp_stock = int(stock_qty) if pd.notna(stock_qty) else "N/A"

        content = (
            f"Product: {product_name} | Category: {category} | "
            f"Price: {disp_price} {currency} | Date of Sale: {disp_date} | "
            f"Seller: {seller_id} | Stock: {disp_stock}"
        )

        doc = {
            "document_id": f"csv-{str(row.get('id'))}",
            "source_type": "CSV",
            "content": content,
            "author": seller_id,
            "timestamp": date,
            "source_metadata": {
                "record_id": str(row.get("id")),
                "product_name": product_name,
                "category": category,
                "price": price,
                "price_raw": str(row.get("price")),
                "currency": currency,
                "date_of_sale": date,
                "seller_id": seller_id,
                "stock_quantity": None if pd.isna(stock_qty) else int(stock_qty),
            },
        }
        results.append(doc)

    print(f"[CSV] Processed {len(results)} records.")
    return results


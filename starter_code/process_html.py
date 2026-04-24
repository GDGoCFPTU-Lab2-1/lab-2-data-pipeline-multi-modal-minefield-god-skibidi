from bs4 import BeautifulSoup
import re

# ==========================================
# ROLE 2: ETL/ELT BUILDER
# ==========================================
# Task: Extract product data from the HTML table, ignoring boilerplate.

def _clean_html_price(raw: str) -> float | None:
    """Convert Vietnamese price strings to float. Returns None if unparseable."""
    s = raw.strip().lower()
    if s in ("n/a", "liên hệ", "lien he", "contact", ""):
        return None
    # Remove 'vnd', spaces, and commas
    s = re.sub(r"[vnd\s,]", "", s, flags=re.IGNORECASE)
    try:
        return float(s)
    except ValueError:
        return None


def parse_html_catalog(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        soup = BeautifulSoup(f, "html.parser")

    # --- Target only the main-catalog table, ignore nav/footer/ads ---
    table = soup.find("table", {"id": "main-catalog"})
    if not table:
        print("[HTML] Error: Could not find table#main-catalog")
        return []

    headers_raw = [th.get_text(strip=True) for th in table.find("thead").find_all("th")]
    # Expected: Mã SP, Tên sản phẩm, Danh mục, Giá niêm yết, Tồn kho, Đánh giá

    results = []
    rows = table.find("tbody").find_all("tr")
    for row in rows:
        cells = [td.get_text(strip=True) for td in row.find_all("td")]
        if len(cells) < 6:
            continue

        product_id   = cells[0]
        product_name = cells[1]
        category     = cells[2]
        raw_price    = cells[3]
        raw_stock    = cells[4]
        raw_rating   = cells[5]

        price = _clean_html_price(raw_price)

        # Reject negative stock
        try:
            stock = int(raw_stock)
            if stock < 0:
                stock = None
        except ValueError:
            stock = None

        # Normalize rating
        rating = raw_rating if raw_rating and raw_rating.lower() != "không có đánh giá" else None

        content = (
            f"Product: {product_name} | Category: {category} | "
            f"Price: {price} VND | Stock: {stock} | Rating: {rating}"
        )

        doc = {
            "source_type": "HTML",
            "content": content,
            "author": "VinShop",
            "source_metadata": {
                "product_id": product_id,
                "product_name": product_name,
                "category": category,
                "price_vnd": price,
                "price_raw": raw_price,
                "stock_quantity": stock,
                "rating": rating,
            },
        }
        results.append(doc)

    print(f"[HTML] Extracted {len(results)} products from catalog.")
    return results


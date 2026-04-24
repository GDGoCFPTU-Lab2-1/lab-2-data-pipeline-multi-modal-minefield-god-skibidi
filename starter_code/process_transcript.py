import re

# ==========================================
# ROLE 2: ETL/ELT BUILDER
# ==========================================
# Task: Clean the transcript text and extract key information.

# Vietnamese number words -> integer mapping (for common amounts)
_VN_NUMBER_MAP = {
    "không": 0, "một": 1, "hai": 2, "ba": 3, "bốn": 4, "năm": 5,
    "sáu": 6, "bảy": 7, "tám": 8, "chín": 9, "mười": 10,
    "trăm": 100, "nghìn": 1_000, "ngàn": 1_000,
    "triệu": 1_000_000, "tỷ": 1_000_000_000,
}

def _parse_vn_number(text: str) -> int | None:
    """
    Parse simple Vietnamese number phrases like 'năm trăm nghìn' = 500,000.
    Handles: X trăm nghìn, X triệu, etc.
    """
    words = text.lower().split()
    result = 0
    current = 0
    try:
        for word in words:
            if word in _VN_NUMBER_MAP:
                n = _VN_NUMBER_MAP[word]
                if n >= 100:
                    if current == 0:
                        current = 1
                    current *= n
                    if n >= 1_000:
                        result += current
                        current = 0
                else:
                    current += n
        result += current
        return result if result > 0 else None
    except Exception:
        return None


def clean_transcript(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        text = f.read()

    # --- Remove timestamps like [00:05:12] ---
    text_clean = re.sub(r"\[\d{2}:\d{2}:\d{2}\]", "", text)

    # --- Remove noise tokens: [Music], [inaudible], [Laughter], [Music starts/ends] ---
    text_clean = re.sub(r"\[(?:Music[^\]]*|inaudible|Laughter|noise[^\]]*)\]", "", text_clean, flags=re.IGNORECASE)

    # --- Remove speaker labels like [Speaker 1]: ---
    text_clean = re.sub(r"\[Speaker \d+\]:\s*", "", text_clean, flags=re.IGNORECASE)

    # --- Strip extra whitespace ---
    lines = [line.strip() for line in text_clean.splitlines() if line.strip()]
    cleaned_text = " ".join(lines)

    # --- Extract VinAI Pro price from Vietnamese words ---
    # Strategy 1: Match VN word numbers before VND
    # e.g., "năm trăm nghìn VND" -> 500000
    vn_number_words = r"(?:một|hai|ba|bốn|năm|sáu|bảy|tám|chín|mười|trăm|nghìn|ngàn|triệu|tỷ)"
    price_match = re.search(
        rf"((?:{vn_number_words}\s*){{1,10}})\s*vnd",
        cleaned_text,
        re.IGNORECASE,
    )
    extracted_price = None
    price_phrase = None
    if price_match:
        price_phrase = price_match.group(1).strip()
        extracted_price = _parse_vn_number(price_phrase)

    # Strategy 2: Fallback — find explicit numeric value like "500,000 VND"
    if extracted_price is None:
        num_match = re.search(r"([\d,\.]+)\s*vnd", cleaned_text, re.IGNORECASE)
        if num_match:
            try:
                extracted_price = int(num_match.group(1).replace(",", "").replace(".", ""))
            except ValueError:
                pass

    print(f"[Transcript] Cleaned text length: {len(cleaned_text)} chars | Extracted price: {extracted_price} VND")

    return {
        "source_type": "Video",
        "content": cleaned_text,
        "author": "Speaker 1",
        "source_metadata": {
            "file_path": file_path,
            "detected_price_vnd": extracted_price,
            "price_phrase_vn": price_phrase,
            "original_line_count": text.count("\n"),
            "cleaned_line_count": len(lines),
        },
    }


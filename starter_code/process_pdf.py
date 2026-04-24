import google.generativeai as genai
import os
import time
import re

# ==========================================
# ROLE 2: ETL/ELT BUILDER
# ==========================================
# Task: Use Gemini API to extract structured data from lecture_notes.pdf
# Implements Exponential Backoff for 429 rate-limit errors.

def extract_pdf_data(file_path):
    # --- FILE CHECK ---
    if not os.path.exists(file_path):
        print(f"[PDF] Error: File not found at {file_path}")
        return None

    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        print("[PDF] Error: GEMINI_API_KEY environment variable not set.")
        return None

    genai.configure(api_key=api_key)
    model = genai.GenerativeModel("gemini-1.5-flash")

    prompt = (
        "You are a document analysis assistant. Read the attached PDF carefully and extract:\n"
        "1. Title: The main title of the document.\n"
        "2. Author: The author(s) of the document.\n"
        "3. Main Topics: A comma-separated list of the key topics covered.\n"
        "4. Summary: A 3-sentence summary of the document.\n"
        "5. Tables: Describe any tables found (title and column headers).\n\n"
        "Respond STRICTLY in this plain-text format (no markdown):\n"
        "Title: <value>\n"
        "Author: <value>\n"
        "Main Topics: <value>\n"
        "Summary: <value>\n"
        "Tables: <value>"
    )

    max_retries = 5
    wait_time = 2  # seconds, doubles each retry

    for attempt in range(max_retries):
        try:
            print(f"[PDF] Uploading file to Gemini (attempt {attempt + 1})...")
            pdf_file = genai.upload_file(path=file_path)
            response = model.generate_content([pdf_file, prompt])
            raw_text = response.text.strip()

            # --- Parse response ---
            def _extract(key, text):
                pattern = rf"^{key}:\s*(.+)$"
                match = re.search(pattern, text, re.MULTILINE | re.IGNORECASE)
                return match.group(1).strip() if match else "Unknown"

            title   = _extract("Title", raw_text)
            author  = _extract("Author", raw_text)
            topics  = _extract("Main Topics", raw_text)
            summary = _extract("Summary", raw_text)
            tables  = _extract("Tables", raw_text)

            full_content = (
                f"Title: {title}\n"
                f"Author: {author}\n"
                f"Main Topics: {topics}\n"
                f"Summary: {summary}\n"
                f"Tables: {tables}"
            )

            print(f"[PDF] Successfully extracted data: '{title}'")
            return {
                "source_type": "PDF",
                "content": full_content,
                "author": author,
                "source_metadata": {
                    "file_path": file_path,
                    "title": title,
                    "main_topics": topics,
                    "tables": tables,
                    "raw_gemini_response": raw_text,
                },
            }

        except Exception as e:
            err_str = str(e)
            if "429" in err_str or "quota" in err_str.lower():
                print(f"[PDF] Rate limit hit (429). Waiting {wait_time}s before retry...")
                time.sleep(wait_time)
                wait_time *= 2
            else:
                print(f"[PDF] Unexpected error: {e}")
                return None

    print("[PDF] Max retries exceeded. Skipping PDF.")
    return None


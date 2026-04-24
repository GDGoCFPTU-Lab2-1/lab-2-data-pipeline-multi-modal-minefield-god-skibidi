import json
import time
import os
import sys

# Robust path handling
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_DATA_DIR = os.path.join(os.path.dirname(SCRIPT_DIR), "raw_data")

# Ensure starter_code directory is importable
sys.path.insert(0, SCRIPT_DIR)

# Import role-specific modules
from schema import UnifiedDocument
from process_pdf import extract_pdf_data
from process_transcript import clean_transcript
from process_html import parse_html_catalog
from process_csv import process_sales_csv
from process_legacy_code import extract_logic_from_code
from quality_check import run_quality_gate

# ==========================================
# ROLE 4: DEVOPS & INTEGRATION SPECIALIST
# ==========================================
# Orchestrates the DAG and saves the final validated Knowledge Base as JSON (v2 schema).

def _build_document(raw_dict: dict) -> UnifiedDocument | None:
    """Safely wrap a raw processor dict into a validated UnifiedDocument."""
    if not raw_dict:
        return None
    try:
        return UnifiedDocument(**{k: v for k, v in raw_dict.items() if k in UnifiedDocument.model_fields})
    except Exception as e:
        print(f"[Orchestrator] Schema validation error: {e}")
        return None


def _process_and_add(raw, label: str, final_kb: list):
    """
    Handles single dict or list of dicts from a processor.
    Validates schema and runs quality gates before adding.
    """
    if raw is None:
        print(f"[Orchestrator] {label}: No data returned, skipping.")
        return

    items = raw if isinstance(raw, list) else [raw]

    passed = 0
    for item in items:
        if not item:
            continue
        doc = _build_document(item)
        if doc is None:
            continue
        
        # Check quality before migration (quality gate handles both v1 and v2)
        if run_quality_gate(doc.model_dump()):
            # MIGRATE TO v2 SCHEMA: 'content' -> 'body_text', 'author' -> 'created_by'
            doc_v2 = doc.to_v2()
            final_kb.append(doc_v2)
            passed += 1

    print(f"[Orchestrator] {label}: {passed}/{len(items)} documents passed quality gate.")


def main():
    start_time = time.time()
    final_kb = []

    # --- FILE PATH SETUP ---
    pdf_path   = os.path.join(RAW_DATA_DIR, "lecture_notes.pdf")
    trans_path = os.path.join(RAW_DATA_DIR, "demo_transcript.txt")
    html_path  = os.path.join(RAW_DATA_DIR, "product_catalog.html")
    csv_path   = os.path.join(RAW_DATA_DIR, "sales_records.csv")
    code_path  = os.path.join(RAW_DATA_DIR, "legacy_pipeline.py")
    output_path = os.path.join(os.path.dirname(SCRIPT_DIR), "processed_knowledge_base.json")

    print("=" * 60)
    print("  DATA PIPELINE — Starting Ingestion")
    print("=" * 60)

    # --- Node 1: PDF (Gemini API extraction) ---
    print("\n[1/5] Processing PDF...")
    t0 = time.time()
    pdf_data = extract_pdf_data(pdf_path)
    _process_and_add(pdf_data, "PDF", final_kb)
    print(f"      Done in {time.time() - t0:.2f}s")

    # --- Node 2: Transcript (noise removal + price extraction) ---
    print("\n[2/5] Processing Transcript...")
    t0 = time.time()
    transcript_data = clean_transcript(trans_path)
    _process_and_add(transcript_data, "Transcript", final_kb)
    print(f"      Done in {time.time() - t0:.2f}s")

    # --- Node 3: HTML catalog (boilerplate stripping) ---
    print("\n[3/5] Processing HTML Catalog...")
    t0 = time.time()
    html_data = parse_html_catalog(html_path)
    _process_and_add(html_data, "HTML", final_kb)
    print(f"      Done in {time.time() - t0:.2f}s")

    # --- Node 4: CSV sales records (deduplication + normalization) ---
    print("\n[4/5] Processing CSV Sales Records...")
    t0 = time.time()
    csv_data = process_sales_csv(csv_path)
    _process_and_add(csv_data, "CSV", final_kb)
    print(f"      Done in {time.time() - t0:.2f}s")

    # --- Node 5: Legacy code (AST docstring + discrepancy detection) ---
    print("\n[5/5] Processing Legacy Code...")
    t0 = time.time()
    code_data = extract_logic_from_code(code_path)
    _process_and_add(code_data, "Code", final_kb)
    print(f"      Done in {time.time() - t0:.2f}s")

    # --- Save final Knowledge Base ---
    print(f"\n[Orchestrator] Saving {len(final_kb)} documents to: {output_path}")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(final_kb, f, ensure_ascii=False, indent=2, default=str)
    print(f"[Orchestrator] ✅ Saved successfully.")

    # --- SLA Report ---
    end_time = time.time()
    elapsed = end_time - start_time
    sla_limit = 120  # seconds
    sla_status = "✅ WITHIN SLA" if elapsed <= sla_limit else "⚠️  SLA BREACH"

    print("\n" + "=" * 60)
    print(f"  PIPELINE COMPLETE — {sla_status}")
    print(f"  Total time       : {elapsed:.2f}s (SLA limit: {sla_limit}s)")
    print(f"  Documents stored : {len(final_kb)}")
    print(f"  Schema version   : v2")
    print(f"  Output path      : {output_path}")
    print("=" * 60)


if __name__ == "__main__":
    main()

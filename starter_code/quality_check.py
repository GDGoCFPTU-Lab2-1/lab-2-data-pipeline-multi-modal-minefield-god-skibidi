# ==========================================
# ROLE 3: OBSERVABILITY & QA ENGINEER
# ==========================================
# Task: Implement quality gates to reject corrupt data or logic discrepancies.

import re

# Strings that indicate corrupt or error data
_TOXIC_PATTERNS = [
    "null pointer exception",
    "traceback",
    "error:",
    "exception:",
    "undefined",
    "stacktrace",
    "fatal error",
    "segmentation fault",
    "access denied",
    "permission denied",
]

_MIN_CONTENT_LENGTH = 20  # characters


def run_quality_gate(document_dict: dict) -> bool:
    """
    Runs a series of semantic quality gates on a document dictionary.
    Returns True if the document passes all gates (is valid), False otherwise.
    """
    doc_id = document_dict.get("document_id", "<no-id>")
    source_type = document_dict.get("source_type", "")
    # Support both v1 'content' and v2 'body_text'
    content = document_dict.get("body_text") or document_dict.get("content") or ""

    # --- Gate 1: Required fields ---
    if not source_type:
        print(f"[QA] FAIL [{doc_id}]: Missing 'source_type'.")
        return False

    # --- Gate 2: Minimum content length ---
    if len(content.strip()) < _MIN_CONTENT_LENGTH:
        print(f"[QA] FAIL [{doc_id}]: Content too short ({len(content.strip())} chars < {_MIN_CONTENT_LENGTH}).")
        return False

    # --- Gate 3: Toxic/error string rejection ---
    content_lower = content.lower()
    
    # Check standard toxic patterns
    for toxic in _TOXIC_PATTERNS:
        if toxic in content_lower:
            print(f"[QA] FAIL [{doc_id}]: Toxic pattern found: '{toxic}'.")
            return False
            
    # Special check for 'nan' as a whole word only (to avoid 'Da Nang' etc.)
    if re.search(r"\bnan\b", content_lower):
        print(f"[QA] FAIL [{doc_id}]: Toxic pattern found: 'nan'.")
        return False

    # --- Gate 4: Discrepancy warning ---
    metadata = document_dict.get("source_metadata", {})
    discrepancies = metadata.get("discrepancies", [])
    if discrepancies:
        for d in discrepancies:
            print(f"[QA] WARN [{doc_id}]: Logic discrepancy detected — {d}")

    print(f"[QA] PASS [{doc_id}] ({source_type}) — content length: {len(content)} chars")
    return True

import ast
import re

# ==========================================
# ROLE 2: ETL/ELT BUILDER
# ==========================================
# Task: Extract docstrings and comments from legacy Python code,
#       and flag any business rule discrepancies.

def extract_logic_from_code(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        source_code = f.read()

    # --- Parse source into AST ---
    try:
        tree = ast.parse(source_code)
    except SyntaxError as e:
        print(f"[Code] Syntax error in {file_path}: {e}")
        return None

    # --- Extract docstrings from all functions and the module ---
    extracted_rules = []
    func_docs = {}

    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Module, ast.ClassDef)):
            docstring = ast.get_docstring(node)
            if docstring:
                name = getattr(node, "name", "__module__")
                func_docs[name] = docstring.strip()
                extracted_rules.append(f"[{name}]: {docstring.strip()}")

    # --- Use regex to find inline business rule comments ---
    business_rule_comments = re.findall(
        r"#\s*((?:Business Logic Rule|WARNING|IMPORTANT|Note)[^\n]+)", 
        source_code, 
        re.IGNORECASE
    )

    # --- Detect discrepancies: comment says 8% but code uses 10% ---
    discrepancies = []
    
    # Look for tax calculation discrepancy
    tax_comment_match = re.search(r"#.*?calculates VAT at (\d+)%", source_code, re.IGNORECASE)
    tax_code_match = re.search(r"tax_rate\s*=\s*([\d.]+)", source_code)
    
    if tax_comment_match and tax_code_match:
        comment_rate = float(tax_comment_match.group(1)) / 100
        code_rate = float(tax_code_match.group(1))
        if abs(comment_rate - code_rate) > 0.001:
            discrepancy_msg = (
                f"DISCREPANCY DETECTED in legacy_tax_calc: "
                f"Comment says VAT={int(comment_rate*100)}%, "
                f"but code uses tax_rate={code_rate} ({int(code_rate*100)}%)"
            )
            discrepancies.append(discrepancy_msg)
            print(f"[Code] ⚠️  {discrepancy_msg}")

    all_rules_text = "\n\n".join(extracted_rules)
    if business_rule_comments:
        all_rules_text += "\n\nInline Comments:\n" + "\n".join(f"- {c}" for c in business_rule_comments)
    if discrepancies:
        all_rules_text += "\n\nDiscrepancies Found:\n" + "\n".join(f"- {d}" for d in discrepancies)

    print(f"[Code] Extracted {len(func_docs)} docstrings, {len(business_rule_comments)} inline rules, {len(discrepancies)} discrepancies.")

    return {
        "source_type": "Code",
        "content": all_rules_text,
        "author": "Senior Dev (retired)",
        "source_metadata": {
            "file_path": file_path,
            "functions_documented": list(func_docs.keys()),
            "business_rule_comments": business_rule_comments,
            "discrepancies": discrepancies,
            "function_docstrings": func_docs,
        },
    }


import json
import os

def run_forensic_test():
    print("=== STARTING FORENSIC DEBRIEF ===")
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    base_path = os.path.join(os.path.dirname(script_dir), "processed_knowledge_base.json")
    
    if not os.path.exists(base_path):
        # Fallback to CWD
        base_path = "processed_knowledge_base.json"
        if not os.path.exists(base_path):
            print("Error: processed_knowledge_base.json not found. Pipeline failed.")
            return
            
    with open(base_path, "r", encoding='utf-8') as f:

        data = json.load(f)
    
    score = 0
    total = 3
    
    # Q1: Check for duplicate avoidance
    ids = [d['document_id'] for d in data if 'csv-' in d['document_id']]
    if len(ids) == len(set(ids)):
        print("[PASS] No duplicate IDs in CSV processing.")
        score += 1
    else:
        print("[FAIL] Duplicate IDs detected in the Knowledge Base.")
        
    # Q2: Check for price extraction from transcript
    transcript_doc = next((d for d in data if d['source_type'] == 'Video'), None)
    if transcript_doc and transcript_doc.get('source_metadata', {}).get('detected_price_vnd') == 500000:
        print("[PASS] Correct price extracted from Vietnamese audio transcript.")
        score += 1
    else:
        print("[FAIL] Failed to extract the price mentioned in the audio transcript.")
        
    # Q3: Check for quality gate effectiveness
    # Support both v1 ('content') and v2 ('body_text')
    corrupt_check = any(
        "Null pointer exception" in (d.get('content') or d.get('body_text') or "") 
        for d in data
    )
    if not corrupt_check:
        print("[PASS] Quality gate successfully rejected corrupt content.")
        score += 1
    else:
        print("[FAIL] Quality gate failed: Corrupt data found in Knowledge Base.")
        
    print(f"\nFinal Forensic Score: {score}/{total}")

if __name__ == "__main__":
    run_forensic_test()

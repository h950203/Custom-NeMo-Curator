import json

def txt_to_jsonl(input_path, output_path, prefix="doc_"):
    with open(input_path, "r", encoding="utf-8") as f_in, \
         open(output_path, "w", encoding="utf-8") as f_out:
        
        for idx, line in enumerate(f_in, start=1):
            line = line.strip()
            if not line:
                continue  # 빈 줄은 스킵
            
            doc_id = f"{prefix}{idx:03d}"  # doc_001 형식
            obj = {
                "id": doc_id,
                "text": line
            }
            
            f_out.write(json.dumps(obj, ensure_ascii=False) + "\n")


# 실행 예시
input_txt = input("텍스트 경로를 입력해주세요: ")
txt_to_jsonl(input_txt, "output.jsonl")

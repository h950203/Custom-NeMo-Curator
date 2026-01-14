import json

def txt_to_jsonl(input_file, output_file):
    """
    텍스트 파일을 JSONL 형식으로 변환합니다.
    
    Args:
        input_file (str): 입력 텍스트 파일 경로
        output_file (str): 출력 JSONL 파일 경로
    """
    with open(input_file, 'r', encoding='utf-8') as infile, \
         open(output_file, 'w', encoding='utf-8') as outfile:
        
        doc_id = 1
        
        for line in infile:
            # 빈 줄 건너뛰기
            line = line.strip()
            if not line:
                continue
            
            # JSON 객체 생성
            doc = {
                "id": f"doc_{doc_id:03d}",
                "text": line
            }
            
            # JSONL 형식으로 작성 (한 줄에 하나의 JSON 객체)
            outfile.write(json.dumps(doc, ensure_ascii=False) + '\n')
            doc_id += 1
    
    print(f"변환 완료: {doc_id - 1}개의 문서가 {output_file}에 저장되었습니다.")

# 사용 예시
if __name__ == "__main__":
    import os
    
    input_file = input("파일 경로를 입력하세요: ")  # 입력 파일명
    
    # 확장자를 .jsonl로 변경
    base_path = os.path.splitext(input_file)[0]
    output_file = base_path + ".jsonl"  # 출력 파일명
    
    txt_to_jsonl(input_file, output_file)
    
    # 결과 확인 (처음 3개 출력)
    print("\n변환된 데이터 샘플:")
    with open(output_file, 'r', encoding='utf-8') as f:
        for i, line in enumerate(f):
            if i >= 3:
                break
            print(json.loads(line))

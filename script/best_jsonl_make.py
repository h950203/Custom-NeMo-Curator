import json

def convert_jsonl_format(input_file, output_file):
    """
    JSONL 형식 변환 함수
    
    Args:
        input_file: 입력 JSONL 파일 경로
        output_file: 출력 JSONL 파일 경로
    """
    with open(input_file, 'r', encoding='utf-8') as infile, \
         open(output_file, 'w', encoding='utf-8') as outfile:
        
        for line in infile:
            # 빈 줄 건너뛰기
            if not line.strip():
                continue
            
            # JSON 파싱
            input_data = json.loads(line)
            
            # 새로운 형식으로 변환
            output_data = {
                "instruction": "###Instruction:\nTranslate the following text from English to Korean using the provided glossary. If domain is not common, this sentence is about a specific domain.\n\n###Domain: common\n###Glossary:\nNone",
                "src": "Korean",
                "input": input_data["text"],
                "tgt": "Korean",
                "output": input_data["text"]
            }
            
            # JSON 형식으로 출력 파일에 쓰기
            outfile.write(json.dumps(output_data, ensure_ascii=False) + '\n')

# 사용 예시
if __name__ == "__main__":
    input_file = "123.jsonl"
    output_file = "1111.jsonl"
    
    convert_jsonl_format(input_file, output_file)
    print(f"변환 완료: {input_file} → {output_file}")

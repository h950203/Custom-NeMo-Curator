import json
import os

def make_jsonl_from_text(input_file, output_file):
    """
    텍스트 파일을 읽어 지정된 JSONL 형식으로 변환합니다.
    
    Args:
        input_file (str): 입력 텍스트 파일 경로
        output_file (str): 출력 JSONL 파일 경로
    """
    try:
        with open(input_file, 'r', encoding='utf-8') as infile, \
             open(output_file, 'w', encoding='utf-8') as outfile:
            
            count = 0
            for line in infile:
                # 빈 줄 건너뛰기
                line = line.strip()
                if not line:
                    continue
                
                # script/best_jsonl_make.py 참조 형식
                output_data = {
                    "instruction": "###Instruction:\nTranslate the following text from English to Korean using the provided glossary. If domain is not common, this sentence is about a specific domain.\n\n###Domain: common\n###Glossary:\nNone",
                    "src": "Korean",
                    "input": line,
                    "tgt": "Korean",
                    "output": line
                }
                
                # JSONL 형식으로 작성
                outfile.write(json.dumps(output_data, ensure_ascii=False) + '\n')
                count += 1
        
        print(f"변환 완료: {count}개의 라인이 {output_file}에 저장되었습니다.")

    except FileNotFoundError:
        print(f"오류: 입력 파일 '{input_file}'을(를) 찾을 수 없습니다.")
    except Exception as e:
        print(f"오류 발생: {e}")


if __name__ == "__main__":
    # 사용자로부터 입력 파일 경로 받기
    input_path = input("텍스트 파일 경로를 입력하세요: ")
    
    if os.path.exists(input_path):
        # 출력 파일 경로 설정 (입력 파일명에서 확장자만 .jsonl로 변경)
        base = os.path.splitext(input_path)[0]
        output_path = base + ".jsonl"
        
        # 함수 호출
        make_jsonl_from_text(input_path, output_path)
        
        # 변환된 파일의 처음 몇 줄을 출력하여 확인
        print("\n--- 변환된 JSONL 파일 샘플 ---")
        try:
            with open(output_path, 'r', encoding='utf-8') as f:
                for i, line in enumerate(f):
                    if i >= 3:
                        break
                    print(line.strip())
        except FileNotFoundError:
            pass # make_jsonl_from_text에서 이미 오류를 처리함
            
    else:
        print(f"오류: 파일이 존재하지 않습니다 - '{input_path}'")

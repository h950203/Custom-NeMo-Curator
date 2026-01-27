import json
import os

def extract_fields(input_file, fields_to_keep, output_suffix="_extracted"):
    """
    JSONL 파일에서 특정 필드들만 추출하여 새 파일로 저장
    
    Args:
        input_file: 입력 JSONL 파일 경로
        fields_to_keep: 유지할 필드명 리스트
        output_suffix: 출력 파일 접미사
    """
    # 출력 파일명 생성
    base_name = os.path.splitext(input_file)[0]
    output_file = f"{base_name}{output_suffix}.jsonl"
    
    extracted_data = []
    total_count = 0
    
    try:
        # JSONL 파일 읽기
        with open(input_file, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                total_count += 1
                try:
                    data = json.loads(line.strip())
                    
                    # 지정된 필드만 추출
                    filtered_data = {}
                    for field in fields_to_keep:
                        if field in data:
                            filtered_data[field] = data[field]
                    
                    extracted_data.append(filtered_data)
                    
                except json.JSONDecodeError as e:
                    print(f"JSON 파싱 오류 (라인 {line_num}): {e}")
                    continue
        
        # 추출된 데이터를 새 JSONL 파일로 저장
        with open(output_file, 'w', encoding='utf-8') as f:
            for data in extracted_data:
                f.write(json.dumps(data, ensure_ascii=False) + '\n')
        
        print(f"\n처리 완료!")
        print(f"전체 행 수: {total_count}")
        print(f"추출된 행 수: {len(extracted_data)}")
        print(f"유지된 필드: {', '.join(fields_to_keep)}")
        print(f"출력 파일: {output_file}")
        
        # 샘플 출력 (첫 번째 행)
        if extracted_data:
            print(f"\n샘플 데이터 (첫 번째 행):")
            print(json.dumps(extracted_data[0], ensure_ascii=False, indent=2))
        
        return output_file
        
    except FileNotFoundError:
        print(f"파일을 찾을 수 없습니다: {input_file}")
        return None
    except Exception as e:
        print(f"오류 발생: {e}")
        import traceback
        traceback.print_exc()
        return None


# 메인 실행 부분
if __name__ == "__main__":
    print("=== JSONL 필드 추출 프로그램 ===\n")
    
    # 파일 경로 입력
    input_file = input("입력 파일 경로를 입력하세요 (예: data.jsonl): ").strip()
    
    # 파일 존재 확인
    if not os.path.exists(input_file):
        print(f"오류: '{input_file}' 파일이 존재하지 않습니다.")
        exit(1)
    
    # 기본 필드 설정
    default_fields = ["instruction", "src", "input", "tgt", "output"]
    
    print(f"\n기본 필드: {', '.join(default_fields)}")
    use_default = input("기본 필드를 사용하시겠습니까? (y/n): ").strip().lower()
    
    if use_default == 'y':
        fields_to_keep = default_fields
    else:
        print("\n유지할 필드명을 쉼표로 구분하여 입력하세요.")
        print("예: instruction,src,input,tgt,output")
        fields_input = input("필드명: ").strip()
        fields_to_keep = [f.strip() for f in fields_input.split(',')]
    
    # 출력 파일 접미사
    output_suffix = input("\n출력 파일 접미사 (기본값: _extracted): ").strip()
    if not output_suffix:
        output_suffix = "_extracted"
    
    # 필드 추출 실행
    print(f"\n'{input_file}' 파일에서 필드를 추출합니다...\n")
    extract_fields(input_file, fields_to_keep, output_suffix)
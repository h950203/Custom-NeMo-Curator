import json
import os

def filter_jsonl(input_file, field_name, threshold_value, output_suffix=None):
    """
    JSONL 파일을 읽어서 특정 필드의 값이 threshold 이상인 행만 필터링
    
    Args:
        input_file: 입력 JSONL 파일 경로
        field_name: 필터링할 필드 이름
        threshold_value: 임계값 (이 값 이상인 행만 유지)
        output_suffix: 출력 파일 접미사 (None이면 자동 생성)
    """
    # 출력 파일명 생성
    base_name = os.path.splitext(input_file)[0]
    if output_suffix is None:
        output_suffix = f"_{field_name}_{threshold_value}"
    output_file = f"{base_name}{output_suffix}.jsonl"
    
    filtered_data = []
    total_count = 0
    
    # JSONL 파일 읽기 및 필터링
    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            for line in f:
                total_count += 1
                try:
                    data = json.loads(line.strip())
                    
                    # 특정 필드가 존재하고, 값이 threshold 이상인 경우만 유지
                    if field_name in data and data[field_name] >= threshold_value:
                        filtered_data.append(data)
                        
                except json.JSONDecodeError as e:
                    print(f"JSON 파싱 오류 (라인 {total_count}): {e}")
                    continue
        
        # 필터링된 데이터를 새 JSONL 파일로 저장
        with open(output_file, 'w', encoding='utf-8') as f:
            for data in filtered_data:
                f.write(json.dumps(data, ensure_ascii=False) + '\n')
        
        print(f"\n처리 완료!")
        print(f"전체 행 수: {total_count}")
        print(f"필터링 후 행 수: {len(filtered_data)}")
        print(f"제거된 행 수: {total_count - len(filtered_data)}")
        print(f"출력 파일: {output_file}")
        
        return output_file
        
    except FileNotFoundError:
        print(f"파일을 찾을 수 없습니다: {input_file}")
        return None
    except Exception as e:
        print(f"오류 발생: {e}")
        return None


# 메인 실행 부분
if __name__ == "__main__":
    print("=== JSONL 파일 필터링 프로그램 ===\n")
    
    # 파일 경로 입력
    input_file = input("입력 파일 경로를 입력하세요 (예: data.jsonl): ").strip()
    
    # 파일 존재 확인
    if not os.path.exists(input_file):
        print(f"오류: '{input_file}' 파일이 존재하지 않습니다.")
        exit(1)
    
    # 필드명 입력
    field_name = input("필터링할 필드명을 입력하세요 (예: score): ").strip()
    
    # 임계값 입력
    while True:
        try:
            threshold_input = input("임계값을 입력하세요 (이 값 이상만 유지): ").strip()
            
            # 정수 또는 실수로 변환 시도
            if '.' in threshold_input:
                threshold_value = float(threshold_input)
            else:
                threshold_value = int(threshold_input)
            break
        except ValueError:
            print("올바른 숫자를 입력해주세요.")
    
    # 필터링 실행
    print(f"\n'{input_file}' 파일에서 '{field_name}' >= {threshold_value} 인 행을 필터링합니다...\n")
    filter_jsonl(input_file, field_name, threshold_value)
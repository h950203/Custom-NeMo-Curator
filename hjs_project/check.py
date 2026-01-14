import enchant
import re
from typing import List

def is_valid_token(token: str, dictionary) -> bool:
    """
    토큰이 유효한 단어인지 판별
    - 숫자만: False (123, 456)
    - 문자+숫자: False (D1, C9, B11)
    - 단일 문자: False (A, B, C) 단, a, I 제외
    - 순수 알파벳: dictionary 체크
    """
    # 숫자만 포함
    if token.isdigit():
        return False
    
    # 문자+숫자 또는 숫자+문자 조합
    if re.search(r'\d', token):
        return False
    
    # 단일 문자 (a, I 제외)
    if len(token) == 1 and token.lower() not in {'a', 'i'}:
        return False
    
    # 순수 알파벳이면 dictionary 체크
    if token.isalpha():
        return dictionary.check(token)
    
    return False


def filter_txt_file(input_file: str, output_file: str, threshold: float = 0.6):
    """
    txt 파일에서 유효한 단어 비율이 낮은 문장을 제거
    
    Args:
        input_file: 입력 txt 파일 경로
        output_file: 출력 txt 파일 경로
        threshold: 유효한 단어 최소 비율 (기본값: 0.6 = 60%)
    """
    # PyEnchant 사전 초기화
    dictionary = enchant.Dict("en_US")
    
    # 파일 읽기
    with open(input_file, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    filtered_lines = []
    removed_count = 0
    log_entries = []
    
    print(f"총 {len(lines)}개 문장 처리 중...")
    print("=" * 80)
    
    for idx, line in enumerate(lines, 1):
        line = line.strip()
        
        # 빈 줄은 그대로 유지
        if not line:
            filtered_lines.append(line)
            log_entries.append(f"[줄 {idx}] [빈 줄] 유지\n")
            continue
        
        # 단어 토큰화 (모든 단어 포함 - 숫자+문자 조합도 포함)
        words = re.findall(r'\b\w+\b', line)
        
        # 단어가 없으면 제거
        if len(words) == 0:
            removed_count += 1
            print(f"[제거 {removed_count}] 단어 없음: {line[:60]}...")
            log_entries.append(f"[줄 {idx}] [0.0%] (0/0) ❌ 제거 - {line}\n")
            continue
        
        # 유효한 단어 개수 세기 (새로운 로직 사용)
        valid_count = sum(1 for word in words if is_valid_token(word, dictionary))
        valid_ratio = valid_count / len(words)
        
        # 임계값 이상이면 유지, 미만이면 제거
        if valid_ratio >= threshold:
            filtered_lines.append(line)
            log_entries.append(f"[줄 {idx}] [{valid_ratio:.1%}] ({valid_count}/{len(words)}) ✅ 유지 - {line}\n")
        else:
            removed_count += 1
            print(f"[제거 {removed_count}] 유효 단어 비율: {valid_ratio:.1%} ({valid_count}/{len(words)})")
            print(f"  문장: {line[:60]}{'...' if len(line) > 60 else ''}")
            log_entries.append(f"[줄 {idx}] [{valid_ratio:.1%}] ({valid_count}/{len(words)}) ❌ 제거 - {line}\n")
    
    # 결과 파일 저장
    with open(output_file, 'w', encoding='utf-8') as f:
        for line in filtered_lines:
            f.write(line + '\n')
    
    # 로그 파일 저장
    log_file = output_file.replace('.txt', '_log.txt')
    with open(log_file, 'w', encoding='utf-8') as f:
        f.write(f"파일 필터링 로그\n")
        f.write(f"입력 파일: {input_file}\n")
        f.write(f"출력 파일: {output_file}\n")
        f.write(f"임계값: {threshold:.0%}\n")
        f.write(f"=" * 80 + "\n\n")
        f.writelines(log_entries)
        f.write(f"\n" + "=" * 80 + "\n")
        f.write(f"총 문장: {len(lines)}개\n")
        f.write(f"유지: {len(filtered_lines)}개\n")
        f.write(f"제거: {removed_count}개\n")
    
    print("=" * 80)
    print(f"✅ 처리 완료!")
    print(f"  원본 문장: {len(lines)}개")
    print(f"  유지된 문장: {len(filtered_lines)}개")
    print(f"  제거된 문장: {removed_count}개")
    print(f"  결과 저장: {output_file}")
    print(f"  로그 저장: {log_file}")


def analyze_txt_file(input_file: str, threshold: float = 0.6):
    """
    txt 파일 분석 (저장하지 않고 분석만)
    
    Args:
        input_file: 입력 txt 파일 경로
        threshold: 유효한 단어 최소 비율
    """
    dictionary = enchant.Dict("en_US")
    
    with open(input_file, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    print(f"\n파일 분석: {input_file}")
    print(f"임계값: {threshold:.0%} (이상만 유지)")
    print("=" * 80)
    
    keep_lines = []
    remove_lines = []
    
    for line in lines:
        line = line.strip()
        if not line:
            continue
        
        # 모든 단어 토큰 추출 (숫자+문자 조합 포함)
        words = re.findall(r'\b\w+\b', line)
        if len(words) == 0:
            remove_lines.append((line, 0.0, 0, 0))
            continue
        
        # 유효한 단어 개수 세기
        valid_count = sum(1 for word in words if is_valid_token(word, dictionary))
        valid_ratio = valid_count / len(words)
        
        if valid_ratio >= threshold:
            keep_lines.append((line, valid_ratio, valid_count, len(words)))
        else:
            remove_lines.append((line, valid_ratio, valid_count, len(words)))
    
    # 유지될 문장
    print(f"\n✅ 유지될 문장 ({len(keep_lines)}개):")
    for i, (line, ratio, valid, total) in enumerate(keep_lines[:5], 1):
        print(f"{i}. [{ratio:.1%}] {line[:60]}{'...' if len(line) > 60 else ''}")
    if len(keep_lines) > 5:
        print(f"   ... 외 {len(keep_lines) - 5}개")
    
    # 제거될 문장
    print(f"\n❌ 제거될 문장 ({len(remove_lines)}개):")
    for i, (line, ratio, valid, total) in enumerate(remove_lines[:5], 1):
        print(f"{i}. [{ratio:.1%}] ({valid}/{total}) {line[:50]}{'...' if len(line) > 50 else ''}")
    if len(remove_lines) > 5:
        print(f"   ... 외 {len(remove_lines) - 5}개")
    
    print("=" * 80)


if __name__ == "__main__":
    import sys
    
    # 명령줄 인자로 파일명 받기
    if len(sys.argv) > 1:
        input_file = sys.argv[1]
        output_file = sys.argv[2] if len(sys.argv) > 2 else "output.txt"
        threshold = float(sys.argv[3]) if len(sys.argv) > 3 else 0.6
    else:
        # 기본값
        input_file = "input.txt"
        output_file = "output.txt"
        threshold = 0.6
    
    # 사용 예시 1: 분석만 하기
    print("📊 파일 분석 모드")
    try:
        analyze_txt_file(input_file, threshold=threshold)
    except FileNotFoundError:
        print(f"⚠ {input_file} 파일을 찾을 수 없습니다.")
        print("\n테스트 파일을 생성합니다...")
        
        # 테스트 파일 생성
        test_content = """The ship's hull structure includes various plating sections.
L K J Side Bilge Sheerstrake plating H G F E D1 D2 D3 D4 D5
Modern shipbuilding techniques have evolved significantly.
A B C D E F G H I J K L M N O P
Section D1 D2 D3 connects to the main assembly.
This is a completely normal sentence with meaningful content.
X Y Z 123 456 A1 B2 C3
Drawing shows components labeled C1 through C9 with specifications."""
        
        with open(input_file, 'w', encoding='utf-8') as f:
            f.write(test_content)
        
        print(f"✅ {input_file} 파일 생성 완료\n")
        analyze_txt_file(input_file, threshold=threshold)
    
    # 사용 예시 2: 실제 필터링 및 저장
    print("\n\n🔧 파일 필터링 및 저장")
    try:
        filter_txt_file(input_file, output_file, threshold=threshold)
    except FileNotFoundError:
        print(f"⚠ {input_file} 파일을 찾을 수 없습니다.")
        sys.exit(1)

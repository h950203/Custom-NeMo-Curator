#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
JSONL 파일에서 input 또는 output 문장을 추출하는 스크립트
"""

import json
import sys
from pathlib import Path


def extract_sentences(jsonl_file_path, choice):
    """
    JSONL 파일에서 문장을 추출합니다.
    
    Args:
        jsonl_file_path: JSONL 파일 경로
        choice: 1 (input) 또는 2 (output)
    
    Returns:
        추출된 문장들의 리스트
    """
    field = 'input' if choice == 1 else 'output'
    sentences = []
    
    try:
        with open(jsonl_file_path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                
                try:
                    data = json.loads(line)
                    if field in data:
                        sentences.append(data[field])
                    else:
                        print(f"경고: {line_num}번째 줄에 '{field}' 필드가 없습니다.", file=sys.stderr)
                except json.JSONDecodeError as e:
                    print(f"경고: {line_num}번째 줄 JSON 파싱 오류: {e}", file=sys.stderr)
                    
    except FileNotFoundError:
        print(f"오류: 파일을 찾을 수 없습니다: {jsonl_file_path}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"오류: 파일 읽기 중 예외 발생: {e}", file=sys.stderr)
        sys.exit(1)
    
    return sentences


def save_to_txt(sentences, output_file_path):
    """
    문장들을 TXT 파일로 저장합니다.
    
    Args:
        sentences: 문장 리스트
        output_file_path: 출력 파일 경로
    """
    try:
        with open(output_file_path, 'w', encoding='utf-8') as f:
            for sentence in sentences:
                f.write(sentence + '\n')
        print(f"\n성공: {len(sentences)}개의 문장을 '{output_file_path}'에 저장했습니다.")
    except Exception as e:
        print(f"오류: 파일 저장 중 예외 발생: {e}", file=sys.stderr)
        sys.exit(1)


def main():
    print("=" * 60)
    print("JSONL 문장 추출 스크립트")
    print("=" * 60)
    
    # JSONL 파일 경로 입력
    jsonl_file = input("\nJSONL 파일 경로를 입력하세요: ").strip()
    
    if not jsonl_file:
        print("오류: 파일 경로를 입력해주세요.", file=sys.stderr)
        sys.exit(1)
    
    # 파일 존재 확인
    if not Path(jsonl_file).exists():
        print(f"오류: 파일이 존재하지 않습니다: {jsonl_file}", file=sys.stderr)
        sys.exit(1)
    
    # 추출할 필드 선택
    print("\n추출할 문장을 선택하세요:")
    print("1. input 문장")
    print("2. output 문장")
    
    while True:
        choice_str = input("\n선택 (1 또는 2): ").strip()
        try:
            choice = int(choice_str)
            if choice in [1, 2]:
                break
            else:
                print("1 또는 2를 입력해주세요.")
        except ValueError:
            print("숫자를 입력해주세요.")
    
    # 문장 추출
    field_name = 'input' if choice == 1 else 'output'
    print(f"\n'{field_name}' 문장을 추출하는 중...")
    sentences = extract_sentences(jsonl_file, choice)
    
    if not sentences:
        print(f"경고: 추출된 문장이 없습니다.", file=sys.stderr)
        sys.exit(0)
    
    # 출력 파일 경로 생성
    input_path = Path(jsonl_file)
    output_file = input_path.parent / f"{input_path.stem}_{field_name}.txt"
    
    # 파일 저장
    save_to_txt(sentences, output_file)
    
    print(f"\n완료!")
    print("=" * 60)


if __name__ == "__main__":
    main()
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
TXT to JSONL Converter
txt 파일을 읽어서 번역 데이터셋 형식의 jsonl 파일로 변환합니다.
"""

import json
import os
from pathlib import Path


def txt_to_jsonl(
    input_file: str,
    output_file: str,
    src_lang: str = "Korean",
    tgt_lang: str = "English",
    domain: str = "shipbuilding",
    glossary: str = "None"
):
    """
    txt 파일을 jsonl 형식으로 변환합니다.
    
    Args:
        input_file: 입력 txt 파일 경로
        output_file: 출력 jsonl 파일 경로
        src_lang: 원본 언어 (기본값: "Korean")
        tgt_lang: 목표 언어 (기본값: "English")
        domain: 도메인 (기본값: "shipbuilding")
        glossary: 용어집 (기본값: "None")
    """
    
    # 입력 파일 확인
    if not os.path.exists(input_file):
        raise FileNotFoundError(f"입력 파일을 찾을 수 없습니다: {input_file}")
    
    # txt 파일 읽기
    with open(input_file, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    # 빈 줄 제거 및 정리
    lines = [line.strip() for line in lines if line.strip()]
    
    # instruction 템플릿
    instruction = (
        f"###Instruction:\n"
        f"Translate the following text from {src_lang} to {tgt_lang} using the provided glossary. "
        f"If domain is not common, this sentence is about a specific domain.\n\n"
        f"###Domain: {domain}\n"
        f"###Glossary:\n{glossary}"
    )
    
    # jsonl 파일로 저장
    with open(output_file, 'w', encoding='utf-8') as f:
        for line in lines:
            data = {
                "instruction": instruction,
                "src": src_lang,
                "input": line,
                "tgt": tgt_lang,
                "output": line
            }
            f.write(json.dumps(data, ensure_ascii=False) + '\n')
    
    print(f"변환 완료!")
    print(f"입력 파일: {input_file}")
    print(f"출력 파일: {output_file}")
    print(f"총 {len(lines)}개의 라인이 변환되었습니다.")
    print(f"설정: {src_lang} → {tgt_lang}, Domain: {domain}")


if __name__ == "__main__":
    # ===== 여기서 설정을 수정하세요 =====
    
    # 입력/출력 파일 경로
    INPUT_FILE = input("입력 파일 경로를 입력하세요: ")  # 입력 txt 파일 경로
    OUTPUT_FILE = "output.jsonl"  # 출력 jsonl 파일 경로
    
    # 언어 설정
    SOURCE_LANGUAGE = "Korean"  # 원본 언어
    TARGET_LANGUAGE = "English"  # 목표 언어
    
    # 도메인 및 용어집 설정
    DOMAIN = "shipbuilding"  # 도메인
    GLOSSARY = "None"  # 용어집
    
    # ====================================
    
    # 변환 실행
    try:
        txt_to_jsonl(
            input_file=INPUT_FILE,
            output_file=OUTPUT_FILE,
            src_lang=SOURCE_LANGUAGE,
            tgt_lang=TARGET_LANGUAGE,
            domain=DOMAIN,
            glossary=GLOSSARY
        )
    except Exception as e:
        print(f"오류 발생: {e}")

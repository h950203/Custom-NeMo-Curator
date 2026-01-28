#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
TXT to JSONL Converter
두 개의 txt 파일을 읽어서 번역 데이터셋 형식의 jsonl 파일로 변환합니다.
"""

import json
import os
from pathlib import Path


def txt_pair_to_jsonl(
    input_file: str,
    output_file_txt: str,
    output_file_jsonl: str,
    src_lang: str = "Korean",
    tgt_lang: str = "English",
    domain: str = "shipbuilding",
    glossary: str = "None"
):
    """
    두 개의 txt 파일을 jsonl 형식으로 변환합니다.
    
    Args:
        input_file: 입력(source) txt 파일 경로
        output_file_txt: 출력(target) txt 파일 경로
        output_file_jsonl: 최종 출력 jsonl 파일 경로
        src_lang: 원본 언어 (기본값: "Korean")
        tgt_lang: 목표 언어 (기본값: "English")
        domain: 도메인 (기본값: "shipbuilding")
        glossary: 용어집 (기본값: "None")
    """
    
    # 입력 파일 확인
    if not os.path.exists(input_file):
        raise FileNotFoundError(f"입력 파일을 찾을 수 없습니다: {input_file}")
    if not os.path.exists(output_file_txt):
        raise FileNotFoundError(f"출력 파일을 찾을 수 없습니다: {output_file_txt}")
    
    # 첫 번째 txt 파일 읽기 (input)
    with open(input_file, 'r', encoding='utf-8') as f:
        input_lines = f.readlines()
    
    # 두 번째 txt 파일 읽기 (output)
    with open(output_file_txt, 'r', encoding='utf-8') as f:
        output_lines = f.readlines()
    
    # 빈 줄 제거 및 정리
    input_lines = [line.strip() for line in input_lines if line.strip()]
    output_lines = [line.strip() for line in output_lines if line.strip()]
    
    # 라인 수 확인
    if len(input_lines) != len(output_lines):
        print(f"⚠️  경고: 두 파일의 라인 수가 다릅니다!")
        print(f"   입력 파일: {len(input_lines)}줄")
        print(f"   출력 파일: {len(output_lines)}줄")
        min_lines = min(len(input_lines), len(output_lines))
        print(f"   → 처음 {min_lines}줄만 사용합니다.")
        input_lines = input_lines[:min_lines]
        output_lines = output_lines[:min_lines]
    
    # instruction 템플릿
    instruction = (
        f"###Instruction:\n"
        f"Translate the following text from {src_lang} to {tgt_lang} using the provided glossary. "
        f"If domain is not common, this sentence is about a specific domain.\n\n"
        f"###Domain: {domain}\n"
        f"###Glossary:\n{glossary}"
    )
    
    # jsonl 파일로 저장
    with open(output_file_jsonl, 'w', encoding='utf-8') as f:
        for input_line, output_line in zip(input_lines, output_lines):
            data = {
                "instruction": instruction,
                "src": src_lang,
                "input": input_line,
                "tgt": tgt_lang,
                "output": output_line
            }
            f.write(json.dumps(data, ensure_ascii=False) + '\n')
    
    print(f"\n✅ 변환 완료!")
    print(f"📄 입력(source) 파일: {input_file}")
    print(f"📄 출력(target) 파일: {output_file_txt}")
    print(f"💾 최종 JSONL 파일: {output_file_jsonl}")
    print(f"📊 총 {len(input_lines)}개의 라인이 변환되었습니다.")
    print(f"🌐 설정: {src_lang} → {tgt_lang}, Domain: {domain}")


if __name__ == "__main__":
    print("=" * 60)
    print("TXT Pair to JSONL Converter")
    print("=" * 60)
    
    # ===== 여기서 설정을 수정하세요 =====
    
    # 입력/출력 파일 경로
    print("\n📌 두 개의 TXT 파일을 입력해주세요:")
    INPUT_FILE = input("1️⃣  입력(source) 파일 경로: ").strip()
    OUTPUT_FILE_TXT = input("2️⃣  출력(target) 파일 경로: ").strip()
    
    OUTPUT_FILE_JSONL = "output.jsonl"  # 최종 출력 jsonl 파일 경로
    
    # 언어 설정
    SOURCE_LANGUAGE = "Korean"  # 원본 언어
    TARGET_LANGUAGE = "English"  # 목표 언어
    
    # 도메인 및 용어집 설정
    DOMAIN = "shipbuilding"  # 도메인
    GLOSSARY = "None"  # 용어집
    
    # ====================================
    
    # 변환 실행
    try:
        txt_pair_to_jsonl(
            input_file=INPUT_FILE,
            output_file_txt=OUTPUT_FILE_TXT,
            output_file_jsonl=OUTPUT_FILE_JSONL,
            src_lang=SOURCE_LANGUAGE,
            tgt_lang=TARGET_LANGUAGE,
            domain=DOMAIN,
            glossary=GLOSSARY
        )
    except Exception as e:
        print(f"\n❌ 오류 발생: {e}")
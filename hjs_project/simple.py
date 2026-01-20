from nemo_curator.core.client import RayClient
from nemo_curator.backends.xenna import XennaExecutor
from nemo_curator.pipeline import Pipeline
from nemo_curator.stages.text.io.reader import JsonlReader
from nemo_curator.stages.text.io.writer import JsonlWriter
from nemo_curator.stages.text.filters.custom_filter import MultiLingualQualityFilterStage
from nemo_curator.stages.text.preprocessing.korean_preprocessing import KoreanPersonalFilter
from nemo_curator.stages.text.preprocessing.english_preprocessing import EnglishPersonalFilter
from nemo_curator.stages.text.preprocessing.japanese_preprocessing import JapanesePersonalFilter
from nemo_curator.stages.text.preprocessing.chinese_preprocessing import ChinesePersonalFilter
from nemo_curator.stages.text.preprocessing.other_preprocessing import OtherPersonalFilter

import os
import json
import argparse
import glob
import shutil
import re
import torch
from multiprocessing import cpu_count
from datetime import datetime

# ============================================================================
# 설정
# ============================================================================
FILTER_CONFIG = {
    'defaults': {
        'max_repeated_line_fraction': 0.7,
        'ngram_n': 5,
        'max_repeating_ngram_ratio': 0.2,
        'max_symbol_to_word_ratio': 0.8,
        'max_url_ratio': 0.01,
        'min_quality_score': 0.5,
    },
    'ko': {
        'min_words': 2,
        'max_words': 100000,
        'lang_char_pattern': r'[가-힣]',
        'lang_threshold': 0.7,
    },
    'en': {
        'min_words': 3,
        'max_words': 100000,
        'lang_char_pattern': r'[a-zA-Z]',
        'lang_threshold': 0.7,
    },
    'ja': {
        'min_words': 3,
        'max_words': 100000,
        'lang_char_pattern': r'[ぁ-んァ-ン一-龯]',
        'lang_threshold': 0.7,
    },
    'zh': {
        'min_words': 3,
        'max_words': 100000,
        'lang_char_pattern': r'[\u4e00-\u9fff]',
        'lang_threshold': 0.7,
    },
    'quality_weights': {
        'lang_ratio': 0.1,
        'repeated_lines': 0.25,
        'repeated_ngrams': 0.25,
        'symbols': 0.2,
        'urls': 0.2,
    }
}

LANG_CODE_MAP = {
    "korean": "ko",
    "english": "en",
    "japanese": "ja",
    "chinese": "zh",
}

PREPROCESSORS = {
    "ko": KoreanPersonalFilter(),
    "en": EnglishPersonalFilter(),
    "ja": JapanesePersonalFilter(),
    "zh": ChinesePersonalFilter(),
}


def detect_language(text):
    """텍스트의 언어를 감지"""
    korean_chars = len(re.findall(r'[가-힣]', text))
    english_chars = len(re.findall(r'[a-zA-Z]', text))
    japanese_chars = len(re.findall(r'[ぁ-んァ-ン一-龯]', text))
    chinese_chars = len(re.findall(r'[\u4e00-\u9fff]', text))
    
    total_chars = len(text)
    if total_chars == 0:
        return "english"
    
    ratios = {
        'korean': korean_chars / total_chars,
        'english': english_chars / total_chars,
        'japanese': japanese_chars / total_chars,
        'chinese': chinese_chars / total_chars,
    }
    
    max_lang = max(ratios, key=ratios.get)
    if ratios[max_lang] >= 0.3:
        return max_lang
    return "english"


def main():
    parser = argparse.ArgumentParser(description='텍스트 파일 전처리')
    parser.add_argument('input_file', help='입력 txt 파일')
    parser.add_argument('--output-dir', '-o', default='output', help='출력 디렉토리')
    parser.add_argument('--lang', '-g', 
                       choices=['korean', 'english', 'japanese', 'chinese'],
                       help='대상 언어')
    args = parser.parse_args()
    
    print("=" * 70)
    print("텍스트 파일 전처리 시작")
    print("=" * 70)
    print(f"입력: {args.input_file}")
    print(f"출력 디렉토리: {args.output_dir}")
    
    # 출력 디렉토리 생성
    os.makedirs(args.output_dir, exist_ok=True)
    log_dir = os.path.join(args.output_dir, "logs")
    os.makedirs(log_dir, exist_ok=True)
    
    # Ray 초기화
    ray_client = None
    try:
        num_gpus = torch.cuda.device_count() if torch.cuda.is_available() else 0
        num_cpus = cpu_count()
        print(f"\nRay 초기화: {num_cpus} CPUs, {num_gpus} GPUs")
        
        ray_client = RayClient(num_cpus=num_cpus, num_gpus=num_gpus)
        ray_client.start()
        
        # 임시 디렉토리
        temp_dir = "/tmp/txt_process"
        os.makedirs(temp_dir, exist_ok=True)
        
        temp_input = os.path.join(temp_dir, "input.jsonl")
        temp_output = os.path.join(temp_dir, "output")
        
        # 1. txt -> jsonl 변환
        print("\n1. txt -> jsonl 변환 중...")
        with open(args.input_file, 'r', encoding='utf-8') as f:
            lines = [line.rstrip('\n\r') for line in f if line.strip()]
        
        with open(temp_input, 'w', encoding='utf-8') as f:
            for i, line in enumerate(lines, 1):
                lang = args.lang if args.lang else detect_language(line)
                doc = {
                    'input': line,
                    'output': line,
                    'src': lang,
                    'tgt': lang,
                    '_line_num': i,
                }
                f.write(json.dumps(doc, ensure_ascii=False) + '\n')
        
        print(f"   변환 완료: {len(lines)}줄")
        
        # 2. 파이프라인 실행
        print("\n2. 전처리 파이프라인 실행 중...")
        pipeline = Pipeline(name="txt_preprocess")
        pipeline.add_stage(JsonlReader(file_paths=temp_input))
        pipeline.add_stage(MultiLingualQualityFilterStage(
            filter_config=FILTER_CONFIG,
            lang_code_map=LANG_CODE_MAP,
            preprocessors=PREPROCESSORS
        ))
        pipeline.add_stage(JsonlWriter(path=temp_output))
        
        executor = XennaExecutor()
        pipeline.run(executor)
        
        # 3. 결과 수집 및 분석
        print("\n3. 결과 처리 중...")
        output_files = glob.glob(os.path.join(temp_output, "*.jsonl"))
        
        output_lines = []
        preprocessing_logs = []
        
        for out_file in output_files:
            with open(out_file, 'r', encoding='utf-8') as f:
                for line in f:
                    doc = json.loads(line)
                    
                    # 전처리 결과 추출
                    if 'processing_results' in doc:
                        results = doc['processing_results']
                        original_text = doc.get('output', '')
                        processed_text = results.get('processed_output', '')
                        
                        # 변경사항 로깅
                        if original_text != processed_text:
                            preprocessing_logs.append({
                                'line_num': doc.get('_line_num'),
                                'original': original_text,
                                'processed': processed_text,
                            })
                        
                        # 전처리된 텍스트 사용
                        if processed_text and processed_text.strip():
                            output_lines.append(processed_text)
                    else:
                        # processing_results 없으면 원본 사용
                        text = doc.get('output', '')
                        if text and text.strip():
                            output_lines.append(text)
        
        # 4. output.txt 저장
        output_file = os.path.join(args.output_dir, "output.txt")
        with open(output_file, 'w', encoding='utf-8') as f:
            for line in output_lines:
                f.write(line + '\n')
        
        # 5. 로그 저장
        if preprocessing_logs:
            log_file = os.path.join(log_dir, "preprocessing_changes.jsonl")
            with open(log_file, 'w', encoding='utf-8') as f:
                for log in preprocessing_logs:
                    f.write(json.dumps(log, ensure_ascii=False) + '\n')
        
        # 6. 요약 저장
        summary = {
            'timestamp': datetime.now().isoformat(),
            'input_file': args.input_file,
            'output_file': output_file,
            'input_lines': len(lines),
            'output_lines': len(output_lines),
            'removed_lines': len(lines) - len(output_lines),
            'preprocessing_applied': len(preprocessing_logs),
        }
        
        summary_file = os.path.join(log_dir, "summary.json")
        with open(summary_file, 'w', encoding='utf-8') as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
        
        # 결과 출력
        print("\n" + "=" * 70)
        print("처리 완료")
        print("=" * 70)
        print(f"입력 줄 수: {len(lines)}")
        print(f"출력 줄 수: {len(output_lines)}")
        print(f"제거된 줄: {len(lines) - len(output_lines)}")
        print(f"전처리 변경: {len(preprocessing_logs)}건")
        print(f"\n출력: {output_file}")
        print(f"로그: {log_dir}")
        print("=" * 70)
        
        # 임시 파일 삭제
        shutil.rmtree(temp_dir)
        
    except Exception as e:
        print(f"\n오류 발생: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        if ray_client:
            print("\nRay 종료 중...")
            ray_client.stop()


if __name__ == "__main__":
    main()

import json
import sys
from pathlib import Path


def decode_jsonl(input_file: str, output_file: str = None):
    """
    유니코드 이스케이프 시퀀스로 인코딩된 JSONL 파일을 읽기 쉬운 형태로 변환
    
    Args:
        input_file: 입력 JSONL 파일 경로
        output_file: 출력 JSONL 파일 경로 (None이면 _decoded.jsonl로 자동 생성)
    """
    input_path = Path(input_file)
    
    if not input_path.exists():
        print(f"❌ Error: File not found: {input_file}")
        return
    
    # 출력 파일명 자동 생성
    if output_file is None:
        output_file = input_path.parent / f"{input_path.stem}_decoded.jsonl"
    
    output_path = Path(output_file)
    
    print(f"📖 Reading: {input_path}")
    print(f"📝 Writing: {output_path}")
    print("-" * 70)
    
    decoded_count = 0
    error_count = 0
    
    try:
        with open(input_path, 'r', encoding='utf-8') as infile, \
             open(output_path, 'w', encoding='utf-8') as outfile:
            
            for line_num, line in enumerate(infile, 1):
                line = line.strip()
                if not line:
                    continue
                
                try:
                    # JSON 파싱 (자동으로 유니코드 디코딩됨)
                    data = json.loads(line)
                    
                    # 보기 좋게 재저장 (ensure_ascii=False로 한글 그대로 출력)
                    outfile.write(json.dumps(data, ensure_ascii=False) + '\n')
                    
                    decoded_count += 1
                    
                    # 첫 3개 문서 미리보기
                    if decoded_count <= 3:
                        print(f"\n📄 Document {decoded_count}:")
                        print(f"   ID: {data.get('id', 'N/A')}")
                        text = data.get('text', '')
                        preview = text[:80] + "..." if len(text) > 80 else text
                        print(f"   Text: {preview}")
                        if 'word_count' in data:
                            print(f"   Word Count: {data['word_count']}")
                        
                except json.JSONDecodeError as e:
                    print(f"⚠️  Warning: Failed to parse line {line_num}: {e}")
                    error_count += 1
                except Exception as e:
                    print(f"⚠️  Warning: Error on line {line_num}: {e}")
                    error_count += 1
        
        print("\n" + "=" * 70)
        print(f"✅ Decoding completed!")
        print(f"   Total documents decoded: {decoded_count}")
        if error_count > 0:
            print(f"   Errors encountered: {error_count}")
        print(f"   Output saved to: {output_path}")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        return


def preview_jsonl(input_file: str, num_lines: int = 5):
    """
    JSONL 파일의 내용을 미리보기
    
    Args:
        input_file: 입력 JSONL 파일 경로
        num_lines: 미리볼 줄 수
    """
    input_path = Path(input_file)
    
    if not input_path.exists():
        print(f"❌ Error: File not found: {input_file}")
        return
    
    print(f"📖 Previewing: {input_path}")
    print("=" * 70)
    
    try:
        with open(input_path, 'r', encoding='utf-8') as f:
            for i, line in enumerate(f, 1):
                if i > num_lines:
                    break
                
                line = line.strip()
                if not line:
                    continue
                
                try:
                    data = json.loads(line)
                    print(f"\n📄 Document {i}:")
                    print(f"   ID: {data.get('id', 'N/A')}")
                    
                    text = data.get('text', '')
                    print(f"   Text: {text}")
                    
                    if 'metadata' in data:
                        print(f"   Metadata: {data['metadata']}")
                    
                    # 점수 정보가 있으면 출력
                    scores = {}
                    for key in ['word_count', 'non_alpha_numeric_ratio', 
                               'repeated_lines_ratio', 'punctuation_score']:
                        if key in data:
                            scores[key] = data[key]
                    
                    if scores:
                        print(f"   Scores: {scores}")
                    
                    print("-" * 70)
                    
                except json.JSONDecodeError as e:
                    print(f"⚠️  Warning: Failed to parse line {i}: {e}")
                    
    except Exception as e:
        print(f"❌ Error: {e}")


def main():
    """메인 함수"""
    if len(sys.argv) < 2:
        print("Usage:")
        print("  Decode JSONL file:")
        print("    python jsonl_decoder.py <input_file> [output_file]")
        print("\n  Preview JSONL file:")
        print("    python jsonl_decoder.py --preview <input_file> [num_lines]")
        print("\nExamples:")
        print("  python jsonl_decoder.py filtered_data/output.jsonl")
        print("  python jsonl_decoder.py filtered_data/output.jsonl decoded_output.jsonl")
        print("  python jsonl_decoder.py --preview filtered_data/output.jsonl 10")
        sys.exit(1)
    
    # Preview 모드
    if sys.argv[1] == "--preview":
        if len(sys.argv) < 3:
            print("❌ Error: Please specify input file")
            sys.exit(1)
        
        input_file = sys.argv[2]
        num_lines = int(sys.argv[3]) if len(sys.argv) > 3 else 5
        preview_jsonl(input_file, num_lines)
    
    # Decode 모드
    else:
        input_file = sys.argv[1]
        output_file = sys.argv[2] if len(sys.argv) > 2 else None
        decode_jsonl(input_file, output_file)


if __name__ == "__main__":
    main()

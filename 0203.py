import os
import torch
import pandas as pd
from transformers import AutoTokenizer, AutoModelForTokenClassification
from tqdm import tqdm
import argparse
import numpy as np

# 로깅 설정
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PosSequenceEntropyCalculator:
    """
    klue/roberta-large 모델을 사용하여 각 문장의 POS(Part-Of-Speech) 시퀀스 엔트로피를 계산합니다.
    엔트로피는 모델이 각 토큰에 대해 예측하는 POS 태그의 확률 분포를 기반으로 계산됩니다.
    높은 엔트로피는 모델이 특정 태그를 예측하기 어렵다는 것을 의미하며, 문장의 불확실성이 높다고 해석할 수 있습니다.
    """
    def __init__(self, model_name='klue/roberta-large', device=None):
        """
        모델과 토크나이저를 초기화합니다.

        Args:
            model_name (str): 사용할 사전 학습된 모델의 이름.
            device (str, optional): 계산을 수행할 장치 ('cuda', 'cpu' 등). None이면 자동 감지.
        """
        logger.info(f"'{model_name}' 모델 로딩 중...")
        if device is None:
            self.device = "cuda" if torch.cuda.is_available() else "cpu"
        else:
            self.device = device
        
        logger.info(f"사용 장치: {self.device}")

        self.tokenizer = AutoTokenizer.from_pretrained(model_name)
        self.model = AutoModelForTokenClassification.from_pretrained(model_name).to(self.device)
        self.model.eval()
        logger.info("모델 로딩 완료.")

    def calculate_entropy(self, text: str) -> float:
        """
        주어진 텍스트의 POS 시퀀스 엔트로피를 계산합니다.

        Args:
            text (str): 엔트로피를 계산할 문장.

        Returns:
            float: 계산된 평균 엔트로피 값. 문장이 너무 짧거나 비어 있으면 0.0을 반환.
        """
        try:
            if not text or len(text.strip()) < 3:
                return 0.0

            inputs = self.tokenizer(text, return_tensors="pt", truncation=True, max_length=512).to(self.device)
            input_ids = inputs["input_ids"]

            if input_ids.shape[1] == 0:
                return 0.0

            with torch.no_grad():
                logits = self.model(input_ids).logits

            # 첫 번째 토큰([CLS])과 마지막 토큰([SEP])은 제외
            sequence_logits = logits[0, 1:-1, :]
            
            if sequence_logits.shape[0] == 0:
                return 0.0

            # 각 토큰에 대한 확률 분포 및 엔트로피 계산
            probabilities = torch.softmax(sequence_logits, dim=-1)
            log_probabilities = torch.log(probabilities)
            
            # 엔트로피 계산: H(X) = -sum(p(x) * log(p(x)))
            # p*log(p)가 NaN이 되는 것을 방지하기 위해 torch.nansum 사용
            token_entropies = -torch.nansum(probabilities * log_probabilities, dim=-1)

            # 문장의 평균 엔트로피 계산
            mean_entropy = torch.mean(token_entropies).item()
            
            return mean_entropy

        except Exception as e:
            logger.error(f"'{text[:30]}...' 문장 처리 중 오류 발생: {e}")
            return 0.0

def main():
    """
    메인 실행 함수. CLI 인자를 파싱하고, 엔트로피 계산 및 파일 저장을 총괄합니다.
    """
    parser = argparse.ArgumentParser(description="""
        텍스트 파일에서 문장을 읽어 POS 시퀀스 엔트로피를 계산합니다.
        계산된 점수를 기준으로 문장을 세 그룹으로 나누어 별도의 txt 파일로 저장하고,
        전체 결과를 '문장|점수' 형태로 Excel 파일에 저장합니다.
    """ )
    parser.add_argument("input_file", type=str, help="처리할 원본 .txt 파일 경로")
    parser.add_argument("--output_dir", type=str, default="entropy_analysis_result", help="결과 파일을 저장할 디렉토리")
    parser.add_argument("--threshold_a", type=float, default=1.0, help="엔트로피 분류 기준 임계값 A")
    parser.add_argument("--threshold_b", type=float, default=2.0, help="엔트로피 분류 기준 임계값 B")
    parser.add_argument("--device", type=str, help="계산에 사용할 장치 (예: 'cpu', 'cuda', 'cuda:0')")
    
    args = parser.parse_args()

    # 출력 디렉토리 생성
    os.makedirs(args.output_dir, exist_ok=True)

    # 엔트로피 계산기 초기화
    calculator = PosSequenceEntropyCalculator(device=args.device)

    # 입력 파일 읽기
    try:
        with open(args.input_file, 'r', encoding='utf-8') as f:
            sentences = [line.strip() for line in f if line.strip()]
        logger.info(f"'{args.input_file}' 파일에서 {len(sentences)}개의 문장을 읽었습니다.")
    except FileNotFoundError:
        logger.error(f"입력 파일을 찾을 수 없습니다: {args.input_file}")
        return
        
    # 각 문장에 대해 엔트로피 계산
    results = []
    for sentence in tqdm(sentences, desc="엔트로피 계산 중"):
        entropy = calculator.calculate_entropy(sentence)
        results.append({"sentence": sentence, "score": entropy})

    # 결과 분류
    below_a = [r for r in results if r['score'] < args.threshold_a]
    between_ab = [r for r in results if args.threshold_a <= r['score'] < args.threshold_b]
    above_b = [r for r in results if r['score'] >= args.threshold_b]

    # 분류된 결과를 txt 파일로 저장
    try:
        with open(os.path.join(args.output_dir, f"sentences_below_{args.threshold_a}.txt"), 'w', encoding='utf-8') as f:
            for item in below_a:
                f.write(item['sentence'] + '\n')
        
        with open(os.path.join(args.output_dir, f"sentences_{args.threshold_a}_to_{args.threshold_b}.txt"), 'w', encoding='utf-8') as f:
            for item in between_ab:
                f.write(item['sentence'] + '\n')
                
        with open(os.path.join(args.output_dir, f"sentences_above_{args.threshold_b}.txt"), 'w', encoding='utf-8') as f:
            for item in above_b:
                f.write(item['sentence'] + '\n')
        logger.info(f"분류된 txt 파일들을 '{args.output_dir}'에 저장했습니다.")
    except IOError as e:
        logger.error(f"분류된 txt 파일 저장 중 오류 발생: {e}")


    # 전체 결과를 Excel 파일로 저장
    try:
        df = pd.DataFrame(results)
        df_sorted = df.sort_values(by="score", ascending=False)
        excel_path = os.path.join(args.output_dir, "all_sentences_with_scores.xlsx")
        df_sorted.to_excel(excel_path, index=False, engine='openpyxl')
        logger.info(f"전체 결과가 '{excel_path}'에 저장되었습니다.")
    except Exception as e:
        logger.error(f"Excel 파일 저장 중 오류 발생: {e}")

if __name__ == "__main__":
    main()

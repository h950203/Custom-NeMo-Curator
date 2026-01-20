import pandas as pd
import numpy as np
import os
from pathlib import Path
from sklearn.preprocessing import StandardScaler

def remove_outliers_iqr(df, columns):
    """
    IQR(Interquartile Range) 방법을 사용하여 이상치 제거
    Q1 - 1.5*IQR 미만이거나 Q3 + 1.5*IQR 초과하는 값을 이상치로 간주
    """
    mask = pd.Series([True] * len(df), index=df.index)
    
    outlier_info = {}
    
    for col in columns:
        Q1 = df[col].quantile(0.25)
        Q3 = df[col].quantile(0.75)
        IQR = Q3 - Q1
        
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR
        
        col_mask = (df[col] >= lower_bound) & (df[col] <= upper_bound)
        outliers_count = (~col_mask).sum()
        
        if outliers_count > 0:
            outlier_info[col] = {
                'count': outliers_count,
                'lower_bound': lower_bound,
                'upper_bound': upper_bound
            }
        
        mask &= col_mask
    
    return mask, outlier_info

def normalize_numeric_columns(df, numeric_columns):
    """
    숫자형 열을 표준화 (평균 0, 표준편차 1)
    """
    df_normalized = df.copy()
    scaler = StandardScaler()
    
    df_normalized[numeric_columns] = scaler.fit_transform(df[numeric_columns])
    
    return df_normalized

def process_csv_file(file_path):
    """
    CSV 파일을 읽어 이상치를 제거하고 새 파일로 저장
    """
    print(f"\n{'='*80}")
    print(f"파일 처리 시작: {file_path}")
    print(f"{'='*80}\n")
    
    # CSV 파일 읽기
    df = pd.read_csv(file_path)
    
    print(f"✓ 파일 로드 완료")
    print(f"  - 전처리 전 행 수: {len(df)}")
    print(f"  - 전처리 전 열 수: {len(df.columns)}")
    
    # 숫자형 열만 선택
    numeric_columns = df.select_dtypes(include=[np.number]).columns.tolist()
    print(f"\n✓ 숫자형 열 {len(numeric_columns)}개 감지:")
    for col in numeric_columns:
        print(f"  - {col}")
    
    if not numeric_columns:
        print("\n⚠ 숫자형 열이 없습니다. 이상치 제거를 수행하지 않습니다.")
        return
    
    # 이상치 제거
    print(f"\n{'='*80}")
    print("이상치 분석 및 제거 중...")
    print(f"{'='*80}\n")
    
    mask, outlier_info = remove_outliers_iqr(df, numeric_columns)
    df_cleaned = df[mask].copy()
    
    # 이상치 정보 출력
    if outlier_info:
        print("📊 열별 이상치 정보:")
        for col, info in outlier_info.items():
            print(f"\n  [{col}]")
            print(f"    - 이상치 개수: {info['count']}")
            print(f"    - 정상 범위: {info['lower_bound']:.2f} ~ {info['upper_bound']:.2f}")
    else:
        print("✓ 이상치가 발견되지 않았습니다.")
    
    # 결과 출력
    removed_count = len(df) - len(df_cleaned)
    print(f"\n{'='*80}")
    print("처리 결과:")
    print(f"{'='*80}")
    print(f"  - 전처리 전 행 수: {len(df)}")
    print(f"  - 전처리 후 행 수: {len(df_cleaned)}")
    print(f"  - 제거된 행 수: {removed_count} ({removed_count/len(df)*100:.2f}%)")
    
    # 파일 경로 설정
    file_path_obj = Path(file_path)
    
    # 1. 이상치 제거된 파일 저장
    removed_file_name = file_path_obj.stem + "_remove_outliers" + file_path_obj.suffix
    removed_file_path = file_path_obj.parent / removed_file_name
    df_cleaned.to_csv(removed_file_path, index=False, encoding='utf-8-sig')
    print(f"\n✓ 이상치 제거 파일 저장: {removed_file_path}")
    
    # 2. 표준화 수행
    print(f"\n{'='*80}")
    print("표준화(Normalization) 수행 중...")
    print(f"{'='*80}\n")
    
    df_normalized = normalize_numeric_columns(df_cleaned, numeric_columns)
    
    print("✓ 표준화 완료 (평균=0, 표준편차=1)")
    print("\n📊 표준화된 열별 통계:")
    for col in numeric_columns:
        print(f"\n  [{col}]")
        print(f"    - 평균: {df_normalized[col].mean():.6f}")
        print(f"    - 표준편차: {df_normalized[col].std():.6f}")
        print(f"    - 최소값: {df_normalized[col].min():.2f}")
        print(f"    - 최대값: {df_normalized[col].max():.2f}")
    
    # 3. 표준화된 파일 저장
    normalized_file_name = file_path_obj.stem + "_normalization" + file_path_obj.suffix
    normalized_file_path = file_path_obj.parent / normalized_file_name
    df_normalized.to_csv(normalized_file_path, index=False, encoding='utf-8-sig')
    print(f"\n✓ 표준화 파일 저장: {normalized_file_path}")
    
    print(f"\n{'='*80}")
    print("모든 처리 완료!")
    print(f"{'='*80}")
    print(f"\n생성된 파일:")
    print(f"  1. 이상치 제거: {removed_file_path}")
    print(f"  2. 표준화: {normalized_file_path}")
    print(f"{'='*80}\n")

# 사용 예시
if __name__ == "__main__":
    # 파일 경로 입력
    file_path = input("CSV 파일 경로를 입력하세요: ").strip()
    
    # 따옴표 제거 (드래그 앤 드롭으로 입력한 경우)
    file_path = file_path.strip('"').strip("'")
    
    # 파일 존재 확인
    if not os.path.exists(file_path):
        print(f"\n❌ 오류: 파일을 찾을 수 없습니다 - {file_path}")
    else:
        try:
            process_csv_file(file_path)
        except Exception as e:
            print(f"\n❌ 오류 발생: {str(e)}")
            import traceback
            traceback.print_exc()
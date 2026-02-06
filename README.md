# NeMo-Curator 한국어/개인 맞춤 확장 버전

**원본 프로젝트**  
https://github.com/NVIDIA-NeMo/Curator

이 저장소는 NVIDIA NeMo Curator (25.09 버전 기준)를 기반으로 개인적인 데이터 큐레이션 작업을 위해 일부 수정·확장한 포크입니다.

- 한국어 전용 필터 및 전처리 단계 추가
- 특정 도메인에 맞춘 품질 평가 로직 조정
- 커스텀 필터 및 파이프라인 구성

> **중요**: 이 저장소는 NVIDIA의 공식 NeMo Curator와는 별개의 개인/실험용 프로젝트입니다.  
> 공식 버그 리포트나 기능 요청은 원본 저장소에 해주세요.

## 라이선스

- 원본 NeMo Curator : **Apache License 2.0**  
  (https://github.com/NVIDIA-NeMo/Curator/blob/main/LICENSE)

- 본 저장소의 추가·수정 코드 역시 **Apache License 2.0** 을 따릅니다.

**원본 저작권 고지**  
Copyright (c) 2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");  
you may not use this file except in compliance with the License.  
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software  
distributed under the License is distributed on an "AS IS" BASIS,  
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  
See the License for the specific language governing permissions and  
limitations under the License.

## 사용 기반 환경

- **Docker 이미지**: `nvcr.io/nvidia/nemo-curator:25.09`
- **Python**: 3.10 이상 (컨테이너 내 기본 환경 기준)
- **CUDA**: 12.x (컨테이너에 포함됨)

### 컨테이너 내 추가 패키지 설치

```bash
# 컨테이너 실행 후
apt update
apt install -y python3-pip

pip install --no-cache-dir \
    kss \
    stanza==1.4.2 \
    konlpy \
    nltk \
    fugashi[unidic-lite] \
    pandas \
    textstat \
    scipy \
    matplotlib \
    seaborn \
    tqdm \
    psutil
```

> 필요에 따라 `requirements.txt`로 관리하는 것을 추천합니다.

## 설치 및 실행 방법 (간단 예시)

1. 원하는 디렉토리에서 저장소 클론

```bash
git clone https://github.com/당신의-아이디/당신의-저장소명.git
cd 당신의-저장소명
```

2. Docker 컨테이너 실행 (필요 시 volume 마운트)

```bash
docker run --gpus all -it --rm \
    -v $(pwd):/workspace \
    nvcr.io/nvidia/nemo-curator:25.09
```

3. 컨테이너 안에서 위의 pip 설치 명령 실행

4. 커스텀 파이프라인 실행 예시

```bash
# 예시 (실제 경로는 상황에 맞게 수정)
python -m curate.pipeline \
    --config configs/my_korean_pipeline.yaml \
    --input /data/raw \
    --output /data/curated
```

## 주요 변경/추가 사항

- 한국어 문장 분리 및 형태소 분석 강화 (kss + konlpy + stanza 조합)
- 한국어 특화 품질 필터 추가
- 특정 불필요한 영어 중심 필터 비활성화 또는 가중치 조정
- 개인 프로젝트용 로깅 및 중간 결과 저장 로직 개선

더 자세한 변경 내역은 커밋 로그 및 `docs/changes.md` (또는 `CHANGELOG.md`) 참고

## 원본 프로젝트 문서 및 자료

- 공식 문서: https://docs.nvidia.com/nemo/curator/latest/
- 원본 GitHub: https://github.com/NVIDIA-NeMo/Curator
- NGC 컨테이너: https://catalog.ngc.nvidia.com/orgs/nvidia/containers/nemo-curator

## 기여 및 문의

이 저장소는 주로 개인/실험 목적으로 운영되고 있습니다.  

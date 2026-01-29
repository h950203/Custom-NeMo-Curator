"""
영어 문장 완결성 분석을 위한 형태소 분석 라이브러리 설치 및 실행 가이드

=== 설치 방법 ===

1. spaCy 설치 (권장):
   pip install spacy
   python -m spacy download en_core_web_sm

2. NLTK 설치:
   pip install nltk
   
3. TextBlob 설치:
   pip install textblob
   python -m textblob.download_corpora

=== 실행 방법 ===
python test_english_pos_libraries.py

=== 비교 ===
- spaCy: 가장 정확하고 현대적, 의존 구문 분석 포함, 속도 빠름 ⭐ 권장
- NLTK: 전통적, 교육용으로 좋음, 다양한 기능
- TextBlob: 간단하고 쉬움, NLTK 기반

영어 문장 완결성 분석에는 spaCy가 가장 적합합니다.
"""

import sys

def test_spacy():
    """spaCy 라이브러리 테스트"""
    print("\n" + "="*80)
    print("1. spaCy 라이브러리 테스트")
    print("="*80)
    
    try:
        import spacy
        print("✓ spaCy 설치됨")
        
        # 영어 모델 로드 시도
        try:
            nlp = spacy.load("en_core_web_sm")
            print("✓ en_core_web_sm 모델 로드 성공")
        except:
            print("✗ en_core_web_sm 모델 없음")
            print("  설치 명령: python -m spacy download en_core_web_sm")
            return None
        
        return nlp
        
    except ImportError:
        print("✗ spaCy 미설치")
        print("  설치 명령: pip install spacy")
        return None


def test_nltk():
    """NLTK 라이브러리 테스트"""
    print("\n" + "="*80)
    print("2. NLTK 라이브러리 테스트")
    print("="*80)
    
    try:
        import nltk
        print("✓ NLTK 설치됨")
        
        # 필요한 데이터 다운로드 확인
        try:
            nltk.data.find('tokenizers/punkt')
            print("✓ punkt tokenizer 있음")
        except LookupError:
            print("  punkt tokenizer 다운로드 중...")
            try:
                nltk.download('punkt', quiet=True)
            except:
                print("  ✗ 다운로드 실패 (인터넷 연결 필요)")
            
        try:
            nltk.data.find('taggers/averaged_perceptron_tagger')
            print("✓ POS tagger 있음")
        except LookupError:
            print("  POS tagger 다운로드 중...")
            try:
                nltk.download('averaged_perceptron_tagger', quiet=True)
            except:
                print("  ✗ 다운로드 실패 (인터넷 연결 필요)")
        
        return nltk
        
    except ImportError:
        print("✗ NLTK 미설치")
        print("  설치 명령: pip install nltk")
        return None


def test_textblob():
    """TextBlob 라이브러리 테스트"""
    print("\n" + "="*80)
    print("3. TextBlob 라이브러리 테스트")
    print("="*80)
    
    try:
        from textblob import TextBlob
        print("✓ TextBlob 설치됨")
        return TextBlob
        
    except ImportError:
        print("✗ TextBlob 미설치")
        print("  설치 명령: pip install textblob")
        return None


def analyze_with_spacy(nlp, sentences, output_file):
    """spaCy로 문장 분석 - 가장 상세한 분석"""
    if nlp is None:
        return
    
    output_file.write("\n" + "="*80 + "\n")
    output_file.write("spaCy 분석 결과\n")
    output_file.write("="*80 + "\n\n")
    
    for sentence in sentences:
        output_file.write(f"\n문장: {sentence}\n")
        output_file.write("-" * 80 + "\n")
        doc = nlp(sentence)
        
        # 간결한 형식으로 토큰 정보 출력
        output_file.write(f"{'Token':<15} {'POS':<8} {'Tag':<8} {'Dep':<10}\n")
        output_file.write("-" * 80 + "\n")
        for token in doc:
            output_file.write(f"{token.text:<15} {token.pos_:<8} {token.tag_:<8} {token.dep_:<10}\n")
        
        # 중요 정보 추출
        output_file.write(f"\n[요약]\n")
        output_file.write(f"마지막: '{doc[-1].text}' (POS:{doc[-1].pos_}, Tag:{doc[-1].tag_})\n")
        
        # 동사 찾기
        verbs = [token for token in doc if token.pos_ == "VERB" or token.pos_ == "AUX"]
        if verbs:
            output_file.write(f"동사: {', '.join([f'{v.text}({v.tag_})' for v in verbs])}\n")
        
        # 주어 찾기
        subjects = [token for token in doc if "subj" in token.dep_]
        if subjects:
            output_file.write(f"주어: {', '.join([s.text for s in subjects])}\n")
        
        # 문장의 루트(root) 동사
        root = [token for token in doc if token.dep_ == "ROOT"]
        if root:
            output_file.write(f"ROOT: {root[0].text} ({root[0].pos_})\n")


def analyze_with_nltk(nltk, sentences, output_file):
    """NLTK로 문장 분석 - Penn Treebank 태그셋 사용"""
    if nltk is None:
        return
    
    output_file.write("\n" + "="*80 + "\n")
    output_file.write("NLTK 분석 결과\n")
    output_file.write("="*80 + "\n\n")
    
    for sentence in sentences:
        output_file.write(f"\n문장: {sentence}\n")
        output_file.write("-" * 80 + "\n")
        
        # 토큰화
        tokens = nltk.word_tokenize(sentence)
        
        # 품사 태깅
        pos_tags = nltk.pos_tag(tokens)
        
        output_file.write(f"{'Token':<15} {'POS':<8}\n")
        output_file.write("-" * 80 + "\n")
        
        for token, pos in pos_tags:
            output_file.write(f"{token:<15} {pos:<8}\n")
        
        if pos_tags:
            last_token, last_pos = pos_tags[-1]
            output_file.write(f"\n[요약]\n")
            output_file.write(f"마지막: '{last_token}' (POS:{last_pos})\n")
            
            # 동사 찾기
            verbs = [token for token, pos in pos_tags if pos.startswith('VB')]
            if verbs:
                output_file.write(f"동사: {', '.join(verbs)}\n")


def analyze_with_textblob(TextBlob, sentences, output_file):
    """TextBlob로 문장 분석 - NLTK 기반의 간단한 인터페이스"""
    if TextBlob is None:
        return
    
    output_file.write("\n" + "="*80 + "\n")
    output_file.write("TextBlob 분석 결과\n")
    output_file.write("="*80 + "\n\n")
    
    for sentence in sentences:
        output_file.write(f"\n문장: {sentence}\n")
        output_file.write("-" * 80 + "\n")
        
        blob = TextBlob(sentence)
        
        output_file.write(f"{'Token':<15} {'POS':<8}\n")
        output_file.write("-" * 80 + "\n")
        for word, pos in blob.tags:
            output_file.write(f"{word:<15} {pos:<8}\n")
        
        if blob.tags:
            last_word, last_pos = blob.tags[-1]
            output_file.write(f"\n[요약]\n")
            output_file.write(f"마지막: '{last_word}' (POS:{last_pos})\n")


def main():
    """메인 테스트 함수"""
    
    print("="*80)
    print("영어 문장 완결성 분석을 위한 형태소 분석 라이브러리 테스트")
    print("="*80)
    
    # 라이브러리 설치 확인
    spacy_nlp = test_spacy()
    nltk_lib = test_nltk()
    textblob_lib = test_textblob()
    
    # 하나라도 설치되지 않았으면 안내 메시지
    if not any([spacy_nlp, nltk_lib, textblob_lib]):
        print("\n" + "="*80)
        print("경고: 사용 가능한 라이브러리가 없습니다!")
        print("="*80)
        print("\n위의 설치 명령을 참고하여 라이브러리를 설치해주세요.")
        return
    
    # 테스트 문장들 (한국어 버전과 유사한 패턴)
    test_sentences = [
        # === 완전한 문장들 (Complete sentences) ===
        "The movie I watched yesterday was really touching.",
        "It's a beautiful day for a walk in the park.",
        "He finally admitted his mistake and apologized.",
        "The Earth revolves around the Sun.",
        "The dog is playing happily with a ball in the yard.",
        
        # === 불완전한 문장들 - 접속사/전치사로 끝남 ===
        "I was so hungry that I went to the store and",
        "After going to the post office and mailing the letter",
        "While studying at the library suddenly",
        
        # === 불완전한 문장들 - 관계절/명사구 ===
        "The promise that I will never give up",
        "The laptop and a pencil on the desk",
        "A very interesting book about",
        
        # === 종속절만 있는 불완전 문장 ===
        "Because it was raining",
        "Although the work was much more complicated",
        "If I could go back to that time",
        "When the sun rises in the morning",
        
        # === 구(phrase)만 있는 불완전 문장 ===
        "Running quickly through the park",
        "To achieve the goal",
        "Very interesting and",
        "The book that I was reading",
        
        # === 완전한 의문문/명령문/감탄문 ===
        "What time is it?",
        "Where are you going?",
        "Please close the door.",
        "Don't forget your umbrella.",
        "What a beautiful sunset!",
        "How amazing!",
        
        # === 추가 불완전 패턴 ===
        "The person who lives over the mountain",
        "Walking without shoes in the",
        "Too tired to",
        "Either this or",
    ]
    
    # 출력 파일 열기
    output_filename = "english_pos_analysis_results.txt"
    
    with open(output_filename, "w", encoding="utf-8") as f:
        f.write("="*80 + "\n")
        f.write("영어 문장 형태소 분석 결과\n")
        f.write("="*80 + "\n\n")
        f.write(f"총 {len(test_sentences)}개 문장 분석\n\n")
        
        # 각 라이브러리로 분석
        if spacy_nlp:
            f.write("\n" + "="*80 + "\n")
            f.write("사용 라이브러리: spaCy (권장)\n")
            f.write("특징: 의존 구문 분석, ROOT 동사 식별, 높은 정확도\n")
            f.write("="*80 + "\n")
            analyze_with_spacy(spacy_nlp, test_sentences, f)
        
        if nltk_lib:
            f.write("\n\n" + "="*80 + "\n")
            f.write("사용 라이브러리: NLTK\n")
            f.write("특징: Penn Treebank 태그셋, 전통적 NLP 도구\n")
            f.write("="*80 + "\n")
            analyze_with_nltk(nltk_lib, test_sentences, f)
        
        if textblob_lib:
            f.write("\n\n" + "="*80 + "\n")
            f.write("사용 라이브러리: TextBlob\n")
            f.write("특징: NLTK 기반, 사용하기 쉬움\n")
            f.write("="*80 + "\n")
            analyze_with_textblob(textblob_lib, test_sentences, f)
        
        # 가이드 추가
        f.write("\n\n" + "="*80 + "\n")
        f.write("주요 POS 태그 가이드\n")
        f.write("="*80 + "\n\n")
        
        f.write("Penn Treebank 태그 (NLTK, TextBlob):\n")
        f.write("- NN/NNS: 명사 (단수/복수)\n")
        f.write("- VB/VBD/VBG/VBN/VBP/VBZ: 동사 (원형/과거/현재분사/과거분사/현재/3인칭)\n")
        f.write("- JJ/JJR/JJS: 형용사 (원급/비교급/최상급)\n")
        f.write("- RB/RBR/RBS: 부사\n")
        f.write("- IN: 전치사 또는 종속접속사\n")
        f.write("- CC: 등위접속사\n")
        f.write("- TO: to (부정사 표지)\n")
        f.write("- DT: 관사\n")
        f.write("- PRP: 대명사\n")
        f.write("- MD: 조동사\n")
        f.write("- WP/WDT/WRB: 의문사\n\n")
        
        f.write("spaCy Universal 태그:\n")
        f.write("- NOUN: 명사\n")
        f.write("- VERB: 동사\n")
        f.write("- ADJ: 형용사\n")
        f.write("- ADV: 부사\n")
        f.write("- ADP: 전치사\n")
        f.write("- CCONJ/SCONJ: 접속사 (등위/종속)\n")
        f.write("- DET: 한정사\n")
        f.write("- PRON: 대명사\n")
        f.write("- AUX: 조동사\n")
        f.write("- PUNCT: 구두점\n\n")
        
        f.write("="*80 + "\n")
        f.write("영어 문장 완결성 판단 기준\n")
        f.write("="*80 + "\n\n")
        f.write("완전한 문장:\n")
        f.write("1. 주어(Subject) 존재\n")
        f.write("2. 본동사(Main Verb) 존재\n")
        f.write("3. 독립절(Independent clause)\n")
        f.write("4. 적절한 문장 종결 (. ? !)\n\n")
        
        f.write("불완전 문장 패턴:\n")
        f.write("1. 전치사로 끝남: IN (in, on, about...)\n")
        f.write("2. 등위접속사로 끝남: CC (and, but, or...)\n")
        f.write("3. 종속접속사로만 시작: because, if, when...\n")
        f.write("4. 부정사 불완전: TO 뒤 동사 없음\n")
        f.write("5. 관계절만: 주절 없이 종속절만\n")
        f.write("6. 동명사구만: VBG 시작, 본동사 없음\n")
        f.write("7. 명사구만: 동사 없음\n\n")
    
    print(f"\n✓ 분석 완료!")
    print(f"✓ 결과 파일: {output_filename}")
    print(f"✓ 총 {len(test_sentences)}개 문장 분석됨")
    print("\n다음 단계:")
    print("1. 결과 파일을 확인하세요")
    print("2. 파일 내용을 공유하면 영어 문장 완결성 점수 계산 코드를 작성해드립니다")
    print("\n권장: spaCy 결과를 중점적으로 확인하세요 (ROOT, dependency 정보 활용)")


if __name__ == "__main__":
    main()
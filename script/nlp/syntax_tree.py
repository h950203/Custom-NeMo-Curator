"""
하나의 문장을 다양한 NLP 라이브러리를 써서 분석한 후 구문 트리(Dependency Parse Tree) 생성기
CSV 파일로 저장하는 버전 (의존구조/트리구조 분리)
라이브러리에 따른 문장 분석 차이를 확인하기 위한 용도

의존성 설치 방법:
pip install "numpy<2.0"
pip install "torch<2.6"
pip install spacy==3.4.4
pip install stanza==1.4.2
pip install konlpy
pip install nltk
pip install fugashi[unidic-lite]
pip install pandas

# spaCy 모델 다운로드
python -m spacy download en_core_web_sm
python -m spacy download ko_core_news_sm
python -m spacy download ja_core_news_sm

# NLTK 데이터 다운로드
python -c "import nltk; nltk.download('punkt'); nltk.download('averaged_perceptron_tagger_eng'); nltk.download('maxent_ne_chunker'); nltk.download('words')"

파일명 형식: MMDD_HHMMSS.csv
지원 언어: 영어(en), 한국어(ko), 일본어(ja)
"""

import warnings
warnings.filterwarnings('ignore')
import os
from datetime import datetime
from io import StringIO
import sys
import pandas as pd

# PyTorch weights_only 이슈 해결
import torch
if hasattr(torch, 'serialization'):
    try:
        torch.serialization.add_safe_globals(['numpy.core.multiarray._reconstruct'])
    except:
        pass

# 결과 저장을 위한 전역 변수
results_dir = "results"
parse_results = []

def create_results_directory():
    """results 폴더 생성"""
    if not os.path.exists(results_dir):
        os.makedirs(results_dir)
        print(f"✅ '{results_dir}' 폴더 생성됨")

def get_timestamp():
    """현재 시간을 MMDD_HHMMSS 형식으로 반환"""
    return datetime.now().strftime("%m%d_%H%M%S")

def save_to_csv(sentence, lang):
    """결과를 CSV 파일로 저장"""
    timestamp = get_timestamp()
    filename = f"{timestamp}.csv"
    filepath = os.path.join(results_dir, filename)
    
    df = pd.DataFrame(parse_results, columns=['라이브러리', '구조유형', '내용'])
    
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(f"# 분석 문장: {sentence}\n")
        f.write(f"# 언어: {lang}\n")
        f.write(f"# 분석 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("\n")
    
    df.to_csv(filepath, mode='a', index=False, encoding='utf-8')
    
    print(f"\n💾 CSV 파일 저장됨: {filepath}")
    print(f"   총 {len(parse_results)}개의 파서 결과 저장됨")
    return filepath

class OutputCapture:
    """출력을 캡처하는 컨텍스트 매니저"""
    def __init__(self):
        self.output = StringIO()
        self.original_stdout = None
    
    def __enter__(self):
        self.original_stdout = sys.stdout
        sys.stdout = self.output
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        sys.stdout = self.original_stdout
        return False
    
    def get_output(self):
        return self.output.getvalue()

# ========== 1. spaCy ==========
def parse_with_spacy(text, lang='en'):
    """spaCy를 사용한 의존 구문 분석"""
    try:
        import spacy
        
        if lang == 'ko':
            nlp = spacy.load('ko_core_news_sm')
        elif lang == 'ja':
            nlp = spacy.load('ja_core_news_sm')
        else:
            nlp = spacy.load('en_core_web_sm')
        
        doc = nlp(text)
        
        # 의존 관계 캡처
        with OutputCapture() as dep_capture:
            print(f"{'토큰':<15} {'관계':<15} {'헤드':<15}")
            print("-" * 45)
            for token in doc:
                print(f"{token.text:<15} {token.dep_:<15} {token.head.text:<15}")
        
        # 트리 구조 캡처
        with OutputCapture() as tree_capture:
            for token in doc:
                if token.dep_ == "ROOT":
                    print_tree_spacy(token, 0)
        
        dep_output = dep_capture.get_output()
        tree_output = tree_capture.get_output()
        print(dep_output)
        print(tree_output)
        
        parse_results.append(['spaCy', '의존 관계', dep_output])
        parse_results.append(['spaCy', '트리 구조', tree_output])
        
        return doc
        
    except Exception as e:
        error_msg = f"❌ 오류: {e}\n설치 필요: python -m spacy download {lang}_core_news_sm"
        print(error_msg)
        parse_results.append(['spaCy', '오류', error_msg])
        return None

def print_tree_spacy(token, depth=0):
    """spaCy 트리 구조를 텍스트로 출력"""
    indent = "  " * depth
    print(f"{indent}└─ {token.text} ({token.dep_})")
    for child in token.children:
        print_tree_spacy(child, depth + 1)

# ========== 2. Stanza ==========
def parse_with_stanza(text, lang='en'):
    """Stanza를 사용한 의존 구문 분석"""
    try:
        import stanza
        
        original_load = torch.load
        def patched_load(*args, **kwargs):
            kwargs['weights_only'] = False
            return original_load(*args, **kwargs)
        torch.load = patched_load
        
        try:
            nlp = stanza.Pipeline(lang=lang, processors='tokenize,pos,lemma,depparse', 
                                 download_method=None, verbose=False, use_gpu=False)
        except Exception as download_error:
            print(f"모델 다운로드 중... (첫 실행시 시간이 걸릴 수 있습니다)")
            stanza.download(lang, verbose=False)
            nlp = stanza.Pipeline(lang=lang, processors='tokenize,pos,lemma,depparse', 
                                 verbose=False, use_gpu=False)
        
        torch.load = original_load
        
        doc = nlp(text)
        
        # 의존 관계 캡처
        with OutputCapture() as dep_capture:
            print(f"{'토큰':<15} {'관계':<15} {'헤드':<15} {'품사':<10}")
            print("-" * 55)
            
            for sentence in doc.sentences:
                for word in sentence.words:
                    head = sentence.words[word.head-1].text if word.head > 0 else "ROOT"
                    print(f"{word.text:<15} {word.deprel:<15} {head:<15} {word.upos:<10}")
        
        # 트리 구조 캡처
        with OutputCapture() as tree_capture:
            for sentence in doc.sentences:
                # ROOT 노드 찾기
                root_words = [w for w in sentence.words if w.head == 0]
                for root in root_words:
                    print_tree_stanza(root, sentence.words, 0)
        
        dep_output = dep_capture.get_output()
        tree_output = tree_capture.get_output()
        print(dep_output)
        print(tree_output)
        
        parse_results.append(['Stanza', '의존 관계', dep_output])
        parse_results.append(['Stanza', '트리 구조', tree_output])
        
        return doc
        
    except Exception as e:
        error_msg = f"❌ 오류: {e}\n해결 방법:\n  1. pip install 'torch<2.6'\n  2. pip install 'numpy<2.0'"
        print(error_msg)
        parse_results.append(['Stanza', '오류', error_msg])
        return None

def print_tree_stanza(word, all_words, depth=0):
    """Stanza 트리 구조를 텍스트로 출력"""
    indent = "  " * depth
    print(f"{indent}└─ {word.text} ({word.deprel})")
    
    # 현재 단어를 헤드로 가지는 자식들 찾기
    children = [w for w in all_words if w.head == word.id]
    for child in children:
        print_tree_stanza(child, all_words, depth + 1)

# ========== 3. KoNLPy (한국어 전용) ==========
def parse_with_konlpy(text):
    """KoNLPy를 사용한 형태소 분석"""
    try:
        from konlpy.tag import Okt
        
        okt = Okt()
        morphs = okt.pos(text)
        
        with OutputCapture() as morph_capture:
            print(f"{'형태소':<15} {'품사':<10}")
            print("-" * 25)
            
            for word, pos in morphs:
                print(f"{word:<15} {pos:<10}")
        
        with OutputCapture() as noun_capture:
            nouns = okt.nouns(text)
            print(", ".join(nouns))
        
        # 트리 구조 생성 (품사 기반)
        with OutputCapture() as tree_capture:
            print("구문 트리 (품사 기반):")
            build_tree_konlpy(morphs)
        
        morph_output = morph_capture.get_output()
        noun_output = noun_capture.get_output()
        tree_output = tree_capture.get_output()
        
        print(morph_output)
        print(noun_output)
        print(tree_output)
        
        parse_results.append(['KoNLPy', '형태소 분석', morph_output])
        parse_results.append(['KoNLPy', '명사 추출', noun_output])
        parse_results.append(['KoNLPy', '트리 구조', tree_output])
        
        return morphs
        
    except Exception as e:
        error_msg = f"❌ 오류: {e}\n설치 필요: pip install konlpy"
        print(error_msg)
        parse_results.append(['KoNLPy', '오류', error_msg])
        return None

def build_tree_konlpy(morphs):
    """KoNLPy 형태소를 트리 구조로 구성"""
    # 문장 루트
    print("└─ S (문장)")
    
    # 품사별 그룹화
    noun_phrases = []
    verb_phrases = []
    adj_phrases = []
    adverb_phrases = []
    josa_phrases = []
    others = []
    
    for word, pos in morphs:
        if pos in ['Noun']:
            noun_phrases.append((word, pos))
        elif pos in ['Verb']:
            verb_phrases.append((word, pos))
        elif pos in ['Adjective']:
            adj_phrases.append((word, pos))
        elif pos in ['Adverb']:
            adverb_phrases.append((word, pos))
        elif pos in ['Josa']:
            josa_phrases.append((word, pos))
        else:
            others.append((word, pos))
    
    # 명사구
    if noun_phrases:
        print("  └─ NP (명사구)")
        for word, pos in noun_phrases:
            print(f"    └─ {word} ({pos})")
    
    # 동사구
    if verb_phrases:
        print("  └─ VP (동사구)")
        for word, pos in verb_phrases:
            print(f"    └─ {word} ({pos})")
    
    # 형용사구
    if adj_phrases:
        print("  └─ AP (형용사구)")
        for word, pos in adj_phrases:
            print(f"    └─ {word} ({pos})")
    
    # 부사구
    if adverb_phrases:
        print("  └─ ADVP (부사구)")
        for word, pos in adverb_phrases:
            print(f"    └─ {word} ({pos})")
    
    # 조사
    if josa_phrases:
        print("  └─ PP (조사구)")
        for word, pos in josa_phrases:
            print(f"    └─ {word} ({pos})")
    
    # 기타
    if others:
        print("  └─ X (기타)")
        for word, pos in others:
            print(f"    └─ {word} ({pos})")

# ========== 4. 일본어 형태소 분석 (MeCab/Fugashi) ==========
def parse_with_fugashi(text):
    """Fugashi(MeCab)를 사용한 일본어 형태소 분석"""
    try:
        import fugashi
        
        tagger = fugashi.Tagger()
        words = list(tagger(text))
        
        with OutputCapture() as morph_capture:
            print(f"{'형태소':<15} {'품사':<15} {'원형':<15}")
            print("-" * 45)
            
            for word in words:
                surface = word.surface
                pos = word.feature.pos1 if hasattr(word.feature, 'pos1') else 'UNKNOWN'
                lemma = word.feature.lemma if hasattr(word.feature, 'lemma') else surface
                print(f"{surface:<15} {pos:<15} {lemma:<15}")
        
        # 트리 구조 생성 (품사 기반)
        with OutputCapture() as tree_capture:
            print("構文木 (品詞ベース):")
            build_tree_fugashi(words)
        
        morph_output = morph_capture.get_output()
        tree_output = tree_capture.get_output()
        
        print(morph_output)
        print(tree_output)
        
        parse_results.append(['Fugashi/MeCab', '形態素分析', morph_output])
        parse_results.append(['Fugashi/MeCab', '木構造', tree_output])
        
        return words
        
    except Exception as e:
        error_msg = f"❌ 오류: {e}\n설치 필요: pip install fugashi[unidic-lite]"
        print(error_msg)
        parse_results.append(['Fugashi/MeCab', '오류', error_msg])
        return None

def build_tree_fugashi(words):
    """Fugashi 형태소를 트리 구조로 구성"""
    # 문장 루트
    print("└─ S (文)")
    
    # 품사별 그룹화
    noun_phrases = []
    verb_phrases = []
    adj_phrases = []
    adverb_phrases = []
    particle_phrases = []
    others = []
    
    for word in words:
        surface = word.surface
        pos = word.feature.pos1 if hasattr(word.feature, 'pos1') else 'UNKNOWN'
        
        if pos in ['名詞']:  # 명사
            noun_phrases.append((surface, pos))
        elif pos in ['動詞']:  # 동사
            verb_phrases.append((surface, pos))
        elif pos in ['形容詞', '形状詞']:  # 형용사
            adj_phrases.append((surface, pos))
        elif pos in ['副詞']:  # 부사
            adverb_phrases.append((surface, pos))
        elif pos in ['助詞', '助動詞']:  # 조사
            particle_phrases.append((surface, pos))
        else:
            others.append((surface, pos))
    
    # 명사구
    if noun_phrases:
        print("  └─ NP (名詞句)")
        for word, pos in noun_phrases:
            print(f"    └─ {word} ({pos})")
    
    # 동사구
    if verb_phrases:
        print("  └─ VP (動詞句)")
        for word, pos in verb_phrases:
            print(f"    └─ {word} ({pos})")
    
    # 형용사구
    if adj_phrases:
        print("  └─ AP (形容詞句)")
        for word, pos in adj_phrases:
            print(f"    └─ {word} ({pos})")
    
    # 부사구
    if adverb_phrases:
        print("  └─ ADVP (副詞句)")
        for word, pos in adverb_phrases:
            print(f"    └─ {word} ({pos})")
    
    # 조사
    if particle_phrases:
        print("  └─ PP (助詞句)")
        for word, pos in particle_phrases:
            print(f"    └─ {word} ({pos})")
    
    # 기타
    if others:
        print("  └─ X (その他)")
        for word, pos in others:
            print(f"    └─ {word} ({pos})")

# ========== 5. NLTK ==========
def parse_with_nltk(text):
    """NLTK의 기본 구문 분석 기능"""
    try:
        import nltk
        
        required_packages = {
            'punkt': 'tokenizers/punkt',
            'punkt_tab': 'tokenizers/punkt_tab',
            'averaged_perceptron_tagger': 'taggers/averaged_perceptron_tagger',
            'averaged_perceptron_tagger_eng': 'taggers/averaged_perceptron_tagger_eng',
            'maxent_ne_chunker': 'chunkers/maxent_ne_chunker',
            'maxent_ne_chunker_tab': 'chunkers/maxent_ne_chunker_tab',
            'words': 'corpora/words'
        }
        
        for package_name, package_path in required_packages.items():
            try:
                nltk.data.find(package_path)
            except LookupError:
                try:
                    print(f"다운로드 중: {package_name}")
                    nltk.download(package_name, quiet=True)
                except:
                    pass
        
        tokens = nltk.word_tokenize(text)
        pos_tags = nltk.pos_tag(tokens)
        
        # 품사 태깅 캡처
        with OutputCapture() as pos_capture:
            print(f"{'단어':<15} {'품사':<10}")
            print("-" * 25)
            for word, pos in pos_tags:
                print(f"{word:<15} {pos:<10}")
        
        # 청크 파스 트리 캡처 (개선된 문법)
        with OutputCapture() as chunk_capture:
            grammar = r"""
                NP: {<DT>?<JJ>*<NN.*>+}
                VP: {<VB.*><NP|PP|RB>*}
                PP: {<IN><NP>}
                ADVP: {<RB>+}
            """
            
            cp = nltk.RegexpParser(grammar)
            result_tree = cp.parse(pos_tags)
            
            print("청크 파스 트리:")
            print(result_tree)
        
        # 트리 구조 시각화 캡처
        with OutputCapture() as tree_capture:
            print("트리 구조 (계층적 표현):")
            print_tree_nltk(result_tree, 0)
        
        # 개체명 인식 캡처
        with OutputCapture() as ne_capture:
            try:
                ne_tree = nltk.ne_chunk(pos_tags)
                print("개체명 인식 결과:")
                print(ne_tree)
            except Exception as ne_error:
                print(f"개체명 인식 건너뜀: {ne_error}")
        
        pos_output = pos_capture.get_output()
        chunk_output = chunk_capture.get_output()
        tree_output = tree_capture.get_output()
        ne_output = ne_capture.get_output()
        
        print(pos_output)
        print(chunk_output)
        print(tree_output)
        print(ne_output)
        
        parse_results.append(['NLTK', '품사 태깅', pos_output])
        parse_results.append(['NLTK', '청크 파스 트리', chunk_output])
        parse_results.append(['NLTK', '트리 구조', tree_output])
        parse_results.append(['NLTK', '개체명 인식', ne_output])
        
        return result_tree
        
    except Exception as e:
        error_msg = f"❌ 오류: {e}"
        print(error_msg)
        parse_results.append(['NLTK', '오류', error_msg])
        return None

def print_tree_nltk(tree, depth=0):
    """NLTK 트리 구조를 계층적으로 출력"""
    import nltk
    
    indent = "  " * depth
    
    if isinstance(tree, nltk.Tree):
        # 구(phrase) 노드
        print(f"{indent}└─ {tree.label()}")
        for child in tree:
            print_tree_nltk(child, depth + 1)
    else:
        # 리프 노드 (단어, 품사)
        word, pos = tree
        print(f"{indent}  └─ {word} ({pos})")

# ========== 6. 간단한 규칙 기반 파서 ==========
def parse_with_simple_rules(text):
    """간단한 규칙 기반 의존 구문 분석"""
    try:
        import nltk
        
        try:
            tokens = nltk.word_tokenize(text)
            pos_tags = nltk.pos_tag(tokens)
        except Exception as nltk_error:
            print(f"NLTK 데이터 부족: {nltk_error}")
            print("기본 분석만 수행합니다.")
            tokens = text.split()
            pos_tags = [(word, 'UNKNOWN') for word in tokens]
        
        with OutputCapture() as capture:
            subjects = [word for word, pos in pos_tags if pos in ['NN', 'NNS', 'NNP', 'NNPS', 'PRP']]
            print(f"주어 후보: {', '.join(subjects) if subjects else '없음'}")
            
            verbs = [word for word, pos in pos_tags if pos.startswith('VB')]
            print(f"동사: {', '.join(verbs) if verbs else '없음'}")
            
            objects = []
            for i, (word, pos) in enumerate(pos_tags):
                if pos.startswith('VB') and i + 1 < len(pos_tags):
                    for j in range(i + 1, len(pos_tags)):
                        if pos_tags[j][1] in ['NN', 'NNS', 'NNP', 'NNPS']:
                            objects.append(pos_tags[j][0])
                            break
            print(f"목적어 후보: {', '.join(objects) if objects else '없음'}")
            
            adjectives = [word for word, pos in pos_tags if pos.startswith('JJ')]
            print(f"형용사: {', '.join(adjectives) if adjectives else '없음'}")
        
        output = capture.get_output()
        print(output)
        
        parse_results.append(['Rule-based', '구조 분석', output])
        
    except Exception as e:
        error_msg = f"❌ 오류: {e}"
        print(error_msg)
        parse_results.append(['Rule-based', '오류', error_msg])

# ========== 메인 분석 함수 ==========
def analyze_sentence(text, lang='en'):
    """모든 파서로 문장 분석"""
    global parse_results
    parse_results = []
    
    print("\n" + "🔍" * 25)
    print(f"분석할 문장: {text}")
    print(f"언어: {lang}")
    print("🔍" * 25 + "\n")
    
    create_results_directory()
    
    results = {}
    
    # 1. spaCy
    print("\n>>> 1/6 spaCy 실행 중...")
    results['spacy'] = parse_with_spacy(text, lang)
    
    # 2. Stanza
    print("\n>>> 2/6 Stanza 실행 중...")
    results['stanza'] = parse_with_stanza(text, lang)
    
    # 3. KoNLPy (한국어만)
    if lang == 'ko':
        print("\n>>> 3/6 KoNLPy 실행 중...")
        results['konlpy'] = parse_with_konlpy(text)
    else:
        print("\n>>> 3/6 KoNLPy 건너뜀 (한국어 전용)")
    
    # 4. Fugashi (일본어만)
    if lang == 'ja':
        print("\n>>> 4/6 Fugashi/MeCab 실행 중...")
        results['fugashi'] = parse_with_fugashi(text)
    else:
        print("\n>>> 4/6 Fugashi 건너뜀 (일본어 전용)")
    
    # 5. NLTK (영어만)
    if lang == 'en':
        print("\n>>> 5/6 NLTK 실행 중...")
        results['nltk'] = parse_with_nltk(text)
    else:
        print("\n>>> 5/6 NLTK 건너뜀 (영어 전용)")
    
    # 6. 규칙 기반 (영어만)
    if lang == 'en':
        print("\n>>> 6/6 규칙 기반 파서 실행 중...")
        parse_with_simple_rules(text)
    else:
        print("\n>>> 6/6 규칙 기반 파서 건너뜀 (영어 전용)")
    
    # CSV로 저장
    save_to_csv(text, lang)
    
    return results

# ========== 메인 실행 ==========
def main():
    """메인 함수"""
    print("="*70)
    print("구문 트리 생성기 - CSV 저장 버전 (트리 구조 개선)")
    print("="*70)
    
    # 사용자 입력 받기
    print("\n문장을 입력하세요:")
    text = input("> ").strip()
    
    if not text:
        print("❌ 문장이 입력되지 않았습니다.")
        return
    
    # 언어 선택
    print("\n언어를 선택하세요:")
    print("1. 영어 (en)")
    print("2. 한국어 (ko)")
    print("3. 일본어 (ja)")
    lang_choice = input("> ").strip()
    
    if lang_choice == '2' or lang_choice.lower() == 'ko':
        lang = 'ko'
    elif lang_choice == '3' or lang_choice.lower() == 'ja':
        lang = 'ja'
    else:
        lang = 'en'
    
    # 분석 실행
    analyze_sentence(text, lang=lang)
    
    print("\n\n" + "="*70)
    print("분석 완료!")
    print(f"결과가 CSV 파일로 '{results_dir}' 폴더에 저장되었습니다.")
    print("="*70)
    print("\n사용 가능한 파서:")
    print("  1. spaCy - 실용적이고 빠른 의존 구문 분석 (모든 언어)")
    print("     → 의존구조, 트리구조 두 행으로 저장")
    print("  2. Stanza - Stanford NLP의 최신 딥러닝 파서 (모든 언어)")
    print("     → 의존구조, 트리구조 두 행으로 저장")
    print("  3. KoNLPy - 한국어 형태소 분석 전용")
    print("     → 형태소분석, 명사추출, 트리구조 세 행으로 저장")
    print("  4. Fugashi/MeCab - 일본어 형태소 분석 전용")
    print("     → 형태소분석, 트리구조 두 행으로 저장")
    print("  5. NLTK - 전통적인 NLP 도구 (영어 전용)")
    print("     → 품사태깅, 청크파스트리, 트리구조, 개체명인식 네 행으로 저장")
    print("  6. 규칙 기반 - 외부 라이브러리 최소 사용 (영어 전용)")
    print("     → 구조분석 한 행으로 저장")
    print("\n※ 에러가 발생한 라이브러리는 건너뛰고 나머지만 실행됩니다.")
    print("※ CSV 파일은 '라이브러리', '구조유형', '내용' 세 열로 구성됩니다.")

if __name__ == "__main__":
    main()
import stanza

stanza.download('ko')  # 한국어 모델
nlp = stanza.Pipeline('ko')

doc = nlp("와 그 대표는 교육과 훈련 프로그램의 효과성을 검토해야 한다.")
# 문장 구조 분석으로 완결성 판단
for sentence in doc.sentences:
    # 주어/서술어 존재 여부 등 체크
    print([word.upos for word in sentence.words])
from kiwipiepy import Kiwi

kiwi = Kiwi()

sentences = [
    "권한있는 기관: 법적 구속력이 있는 법령, 명령 또는 지시를 공표 및 이행할 수 있는 부처, 정부부서 또는",
    "(1) 건강에 유해할 수 있는 물질을 분류하기 위한 시스템 및 기준 (2) 물질의 유해성 여부 결정을 위해"
]

for s in sentences:
    print(f"\n--- Analyzing: '{s}'")
    analysis = kiwi.analyze(s)
    if analysis:
        tokens = analysis[0][0]
        print("Tokens and Tags:")
        for token in tokens:
            print(f"  '{token.form}' -> {token.tag}")
        print(f"** Last Token: '{tokens[-1].form}' -> {tokens[-1].tag} **")
    else:
        print("Analysis failed for this sentence.")


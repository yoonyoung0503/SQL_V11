import streamlit as st
import pandas as pd
import io

# ════════════════════════════════════════════════════════════════
# 탭 정의
# ════════════════════════════════════════════════════════════════
TABS = [
    ("🏠", "개요"),
    ("📦", "환경설정"),
    ("①",  "Python 기초"),
    ("②",  "Pandas 기초"),
    ("③",  "연결·쿼리"),
    ("④",  "JSON 처리"),
    ("⑤",  "파일 저장"),
    ("⑥",  "데이터 정제"),
    ("🚀", "실전 템플릿"),
    ("✏️", "연습과제"),
]
N = len(TABS)

GREEN  = "#10B981"
ORANGE = "#F59E0B"
TEAL   = "#0D9488"

# ════════════════════════════════════════════════════════════════
# 페이지 설정 & 전역 CSS  (v9 디자인 동일 적용)
# ════════════════════════════════════════════════════════════════
st.set_page_config(page_title="HF Python 교육", page_icon="🐍",
                   layout="wide", initial_sidebar_state="collapsed")

st.markdown(f"""
<style>
:root {{
  --c-green:  {GREEN};
  --c-orange: {ORANGE};
  --c-teal:   {TEAL};
}}

[data-testid="stSidebar"],[data-testid="collapsedControl"]{{display:none!important}}
[data-testid="stToolbar"]{{display:none!important}}
[data-testid="stDecoration"]{{display:none!important}}
header[data-testid="stHeader"]{{display:none!important}}

.block-container{{
  padding: 0 1.2rem 2rem !important;
  padding-top: 0 !important;
  margin-top: 0 !important;
  max-width: 100% !important;
}}
.stApp > div:first-child {{
  padding-top: 0 !important;
}}

/* ── 헤더 ── */
.hdr-wrap {{
  position: relative;
  background: linear-gradient(135deg, #064E3B 0%, #065F46 60%, #047857 100%);
  border-radius: 0 0 14px 14px;
  box-shadow: 0 4px 16px rgba(0,0,0,.28);
  padding: .9rem 1.4rem .75rem;
  margin: 0 0 .7rem 0;
  display: flex;
  align-items: center;
  gap: .8rem;
}}
.hdr-text {{ flex: 1; min-width: 0; }}
.hdr-title {{
  color: #ECFDF5;
  font-size: 1.15rem;
  font-weight: 800;
  margin: 0;
  line-height: 1.35;
  letter-spacing: -.02em;
  white-space: normal;
  word-break: keep-all;
}}
.hdr-sub {{
  color: rgba(167,243,208,.85);
  font-size: .78rem;
  margin: .15rem 0 0;
  white-space: normal;
  word-break: keep-all;
}}
.hdr-badges {{ display: flex; gap: .4rem; flex-shrink: 0; align-items: center; }}
.badge {{
  font-size: .65rem;
  font-weight: 700;
  padding: .2rem .55rem;
  border-radius: 20px;
  white-space: nowrap;
  letter-spacing: .02em;
}}
.badge-tag {{ background: rgba(255,255,255,.15); color: #D1FAE5; border: 1px solid rgba(255,255,255,.25); }}
.badge-main {{ background: {ORANGE}; color: #1C1917; border: none; }}

/* ── 탭 구분선 ── */
div[data-testid="stHorizontalBlock"] {{
  border-bottom: 2px solid {GREEN};
  padding-bottom: 2px;
  margin-bottom: 1rem;
}}

/* ── primary 버튼 ── */
[data-testid="stBaseButton-primary"] {{
  background: linear-gradient(135deg, #047857, #065F46) !important;
  border-color: #065F46 !important;
  font-weight: 700 !important;
}}
[data-testid="stBaseButton-primary"]:hover {{
  background: linear-gradient(135deg, #065F46, #064E3B) !important;
  border-color: #064E3B !important;
}}

/* ── 콘텐츠 박스 ── */
.tip {{
  border-left: 4px solid {GREEN};
  background: rgba(16,185,129,.11);
  color: inherit;
  padding: .75rem 1rem;
  border-radius: 0 8px 8px 0;
  margin: .5rem 0;
  font-size: .9rem;
  line-height: 1.65;
}}
.wrn {{
  border-left: 4px solid {ORANGE};
  background: rgba(245,158,11,.11);
  color: inherit;
  padding: .75rem 1rem;
  border-radius: 0 8px 8px 0;
  margin: .5rem 0;
  font-size: .9rem;
  line-height: 1.65;
}}
.okb {{
  border-left: 4px solid {GREEN};
  background: rgba(16,185,129,.11);
  color: inherit;
  padding: .75rem 1rem;
  border-radius: 0 8px 8px 0;
  margin: .5rem 0;
  font-size: .9rem;
  line-height: 1.65;
}}
.err {{
  border-left: 4px solid #F87171;
  background: rgba(239,68,68,.11);
  color: inherit;
  padding: .75rem 1rem;
  border-radius: 0 8px 8px 0;
  margin: .5rem 0;
  font-size: .9rem;
  line-height: 1.65;
}}
.syn {{
  border-left: 4px solid {TEAL};
  background: rgba(13,148,136,.09);
  color: inherit;
  padding: .75rem 1rem;
  border-radius: 0 8px 8px 0;
  margin: .5rem 0;
  font-family: 'Courier New', monospace;
  font-size: .9rem;
  line-height: 2;
}}
.qbox {{
  border: 2px dashed {ORANGE};
  background: rgba(245,158,11,.08);
  color: inherit;
  padding: .9rem 1.15rem;
  border-radius: 10px;
  margin: .65rem 0;
  font-size: .92rem;
  line-height: 1.7;
}}
.okbox {{
  border-left: 4px solid {GREEN};
  background: rgba(16,185,129,.11);
  color: inherit;
  padding: .55rem 1rem;
  border-radius: 0 8px 8px 0;
  margin: .4rem 0;
  font-size: .9rem;
}}
.sec {{
  font-size: 1.28rem;
  font-weight: 800;
  border-bottom: 3px solid {GREEN};
  padding-bottom: .38rem;
  margin-bottom: 1rem;
  color: {GREEN} !important;
  letter-spacing: -.02em;
}}
.cls {{
  border-radius: 12px;
  padding: 1rem 1.15rem;
  border: 1.5px solid rgba(128,128,128,.25);
  background: transparent;
  color: inherit;
  height: 100%;
}}
.step-card {{
  border-radius: 12px;
  padding: 1rem 1.15rem;
  border: 1.5px solid rgba(128,128,128,.2);
  background: transparent;
  color: inherit;
  position: relative;
}}
.step-num {{
  display: inline-block;
  background: {GREEN};
  color: #fff;
  font-weight: 800;
  font-size: .75rem;
  border-radius: 50%;
  width: 1.5rem;
  height: 1.5rem;
  line-height: 1.5rem;
  text-align: center;
  margin-right: .5rem;
}}
</style>
""", unsafe_allow_html=True)

# ── 탭 인덱스 ──────────────────────────────────────────────────
if "t" not in st.session_state:
    try:
        st.session_state.t = int(st.query_params.get("t", 0))
    except Exception:
        st.session_state.t = 0
idx = max(0, min(st.session_state.t, N - 1))

# ── 헤더 ──────────────────────────────────────────────────────
st.markdown(f"""
<div class="hdr-wrap">
  <div class="hdr-text">
    <p class="hdr-title">🐍 빅데이터 플랫폼 사용을 위한 Python 기초</p>
    <p class="hdr-sub">SQLAlchemy · Trino · Pandas · JSON — 실무 데이터 추출부터 파일 저장까지</p>
  </div>
  <div class="hdr-badges">
    <span class="badge badge-tag">데이터분석도구 ①</span>
    <span class="badge badge-main">Python 실습</span>
  </div>
</div>
""", unsafe_allow_html=True)

# ── 탭 바 ─────────────────────────────────────────────────────
tab_cols = st.columns(N)
for i, (col, (ico, lbl)) in enumerate(zip(tab_cols, TABS)):
    with col:
        active = (i == idx)
        if st.button(f"{ico} {lbl}", key=f"tb{i}",
                     type="primary" if active else "secondary",
                     use_container_width=True):
            st.session_state.t = i
            st.rerun()


def nav_footer(i):
    st.divider()
    lc, _, rc = st.columns([2, 6, 2])
    with lc:
        if i > 0:
            if st.button(f"← {TABS[i-1][1]}", key=f"pv{i}", use_container_width=True):
                st.session_state.t = i - 1
                st.rerun()
    with rc:
        if i < N - 1:
            if st.button(f"{TABS[i+1][1]} →", key=f"nx{i}",
                         use_container_width=True, type="primary"):
                st.session_state.t = i + 1
                st.rerun()


def _practice(q, ans, k, hint=None):
    st.markdown(f'<div class="qbox">🎯 <b>실습 문제</b><br>{q}</div>', unsafe_allow_html=True)
    if hint:
        with st.expander("💡 힌트"):
            st.info(hint)
    usr = st.text_area("코드 작성", height=120, key=f"{k}_in",
                       placeholder="여기에 Python 코드를 작성하세요...")
    c1, c2 = st.columns(2)
    with c1:
        if st.button("▶ 내 답안 실행", key=f"{k}_run", type="primary"):
            if usr.strip():
                try:
                    import io, contextlib
                    buf = io.StringIO()
                    with contextlib.redirect_stdout(buf):
                        exec(usr, {"pd": pd, "io": io})
                    out = buf.getvalue()
                    st.success("✅ 실행 완료")
                    if out:
                        st.code(out)
                except Exception as e:
                    st.error(f"오류: {e}")
            else:
                st.warning("코드를 입력해주세요.")
    with c2:
        if st.button("📋 정답 보기", key=f"{k}_ans"):
            st.markdown('<div class="okbox">✅ 정답</div>', unsafe_allow_html=True)
            st.code(ans, language="python")


# ════════════════════════════════════════════════════════════════
# 0. 개요
# ════════════════════════════════════════════════════════════════
if idx == 0:
    st.markdown('<div class="sec">🏠 교육 개요</div>', unsafe_allow_html=True)
    st.markdown("""
이 교육은 **주택금융공사 빅데이터 플랫폼(Trino/Iceberg)**에서 데이터를 추출하고,
**Python(pandas)** 으로 가공·저장하는 실무 전 과정을 다룹니다.
SQL 쿼리를 Python 코드로 실행하고, JSON 응답을 DataFrame으로 변환하며, Excel/CSV 파일로 내보내는 방법을 익힙니다.
    """)

    # 전체 흐름도
    st.markdown("### 📌 전체 실무 흐름")
    st.markdown(f"""
<div class="syn">
① 환경 설정 &nbsp;→&nbsp; ② Trino 연결 &nbsp;→&nbsp; ③ SQL 쿼리 실행 &nbsp;→&nbsp; ④ DataFrame 변환<br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;↓<br>
⑤ JSON 컬럼 파싱 &nbsp;→&nbsp; ⑥ 데이터 정제 &nbsp;→&nbsp; ⑦ CSV / Excel 저장
</div>
""", unsafe_allow_html=True)

    st.markdown("### 📚 커리큘럼")
    curriculum = [
        ("📦", "환경설정", "라이브러리 설치, import, Trino 연결 설정"),
        ("①", "Python 기초", "변수·자료형·리스트·딕셔너리·반복문·함수"),
        ("②", "Pandas 기초", "DataFrame 생성·조회·필터·집계"),
        ("③", "연결·쿼리", "SQLAlchemy + Trino로 빅데이터 플랫폼 쿼리 실행"),
        ("④", "JSON 처리", "clean_payload 컬럼 파싱, json_normalize"),
        ("⑤", "파일 저장", "CSV / Excel 내보내기, 파일명 자동생성"),
        ("⑥", "데이터 정제", "결측값·중복·타입 변환·컬럼 가공"),
        ("🚀", "실전 템플릿", "전체 파이프라인 완성 코드 & 커스터마이즈"),
        ("✏️", "연습과제", "단계별 실습 문제"),
    ]
    c1, c2 = st.columns(2)
    for j, (ico, title, desc) in enumerate(curriculum):
        col = c1 if j % 2 == 0 else c2
        col.markdown(
            f'<div class="step-card" style="margin-bottom:.6rem">'
            f'<span class="step-num">{ico}</span><b>{title}</b><br>'
            f'<small style="opacity:.75">{desc}</small></div>',
            unsafe_allow_html=True)

    st.divider()
    st.markdown("### 🔧 사전 준비")
    c1, c2 = st.columns(2)
    with c1:
        st.markdown('<div class="cls" style="border-top:4px solid ' + GREEN + '"><b>필수 패키지</b><br><br>', unsafe_allow_html=True)
        st.code("""pip install sqlalchemy
pip install trino
pip install pandas
pip install openpyxl     # Excel 저장용""", language="bash")
        st.markdown('</div>', unsafe_allow_html=True)
    with c2:
        st.markdown('<div class="cls" style="border-top:4px solid ' + ORANGE + '"><b>실습 환경</b><br><br>', unsafe_allow_html=True)
        st.markdown("""
- **Python 3.8+** 권장
- **Jupyter Notebook** 또는 **VSCode**
- 사내망 연결 필수 (Trino 호스트 접근)
- IT팀에 `trino://` 접속 권한 요청
        """)
        st.markdown('</div>', unsafe_allow_html=True)

    nav_footer(0)


# ════════════════════════════════════════════════════════════════
# 1. 환경설정
# ════════════════════════════════════════════════════════════════
elif idx == 1:
    st.markdown('<div class="sec">📦 환경설정 & 기본 import</div>', unsafe_allow_html=True)

    st.markdown("### ① 표준 import 블록")
    st.markdown("""실무에서 매 스크립트 상단에 반드시 넣는 기본 import 코드입니다.""")
    st.code("""from sqlalchemy import create_engine, text
import trino                          # 사내 Trino 드라이버
import pandas as pd
import json

# ── Pandas 출력 옵션 ────────────────────────────────────────────
pd.set_option('display.max_columns', None)   # 모든 컬럼 표시
pd.set_option('display.max_colwidth', None)  # 컬럼 내용 잘림 방지
pd.options.display.float_format = '{:,.0f}'.format  # 숫자 천단위 쉼표
""", language="python")

    st.markdown('''<div class="wrn">⚠️ <b>오타 주의</b><br>
원본 코드에 <code>display.max_cloumns</code>(오타)가 있습니다. 올바른 옵션명은 <code>display.max_columns</code>입니다.
</div>''', unsafe_allow_html=True)

    st.divider()
    st.markdown("### ② Trino 엔진 생성")
    st.code("""# 기본형
engine = create_engine("trino://사용자명@호스트:포트/카탈로그/스키마")

# 예시 (사내 Iceberg)
engine = create_engine(
    "trino://uid123@trino.internal.hf.co.kr:8443/iceberg/bronze"
)

# HTTPS + 인증이 필요한 경우
engine = create_engine(
    "trino://uid123@trino.internal.hf.co.kr:8443/iceberg/bronze",
    connect_args={
        "http_scheme": "https",
        "verify": False,             # 사내 인증서 없을 때
    }
)
""", language="python")

    st.markdown('''<div class="tip">💡 <b>Connection String 구조</b><br>
<code>trino://[사용자]@[호스트]:[포트]/[카탈로그]/[스키마]</code><br>
• 카탈로그: iceberg / hive 등 DBA에 확인<br>
• 스키마: bronze / silver / gold 등 레이어 구분
</div>''', unsafe_allow_html=True)

    st.divider()
    st.markdown("### ③ 각 라이브러리 역할 요약")
    roles = [
        ("sqlalchemy", GREEN,  "Python ↔ DB 연결 표준 인터페이스. create_engine·text 함수 제공"),
        ("trino",      TEAL,   "사내 Trino(분산 SQL 엔진) 전용 드라이버. SQLAlchemy와 연동"),
        ("pandas",     ORANGE, "DataFrame 기반 데이터 가공·집계·출력·파일 저장의 핵심"),
        ("json",       GREEN,  "JSON 문자열 ↔ Python dict 변환. clean_payload 파싱에 사용"),
        ("openpyxl",   TEAL,   "pandas의 Excel 저장(to_excel)을 위한 백엔드 엔진"),
    ]
    for lib, bc, desc in roles:
        st.markdown(
            f'<div class="step-card" style="margin-bottom:.5rem;border-left:4px solid {bc}">'
            f'<code style="font-size:.95rem;font-weight:700">{lib}</code> &nbsp;—&nbsp; {desc}</div>',
            unsafe_allow_html=True)

    nav_footer(1)


# ════════════════════════════════════════════════════════════════
# 2. Python 기초
# ════════════════════════════════════════════════════════════════
elif idx == 2:
    st.markdown('<div class="sec">① Python 기초 — 데이터 분석에 필요한 핵심만</div>', unsafe_allow_html=True)

    t1, t2, t3, t4, t5 = st.tabs(["변수·자료형", "리스트·딕셔너리", "조건·반복문", "함수", "f-string"])

    with t1:
        st.markdown("#### 변수와 자료형")
        st.code("""# 기본 자료형
name   = "홍길동"          # str  (문자열)
age    = 30                # int  (정수)
salary = 3250000.50        # float (실수)
is_hf  = True              # bool (참/거짓)
dept   = None              # NoneType (값 없음)

# 타입 확인
print(type(name))          # <class 'str'>
print(type(age))           # <class 'int'>

# 타입 변환
print(str(age))            # "30"
print(int("2024"))         # 2024
print(float("3.14"))       # 3.14
""", language="python")
        st.markdown('''<div class="tip">💡 데이터 분석에서 타입 변환은 자주 사용됩니다. DB에서 숫자가 문자열로 오는 경우 <code>int()</code> 또는 <code>pd.to_numeric()</code>으로 변환합니다.</div>''', unsafe_allow_html=True)

    with t2:
        st.markdown("#### 리스트 (list) — 순서 있는 모음")
        st.code("""# 리스트 생성
regions  = ["서울", "경기", "인천", "부산"]
codes    = ["11", "41", "28", "26"]

# 인덱싱 (0부터 시작)
print(regions[0])          # 서울
print(regions[-1])         # 부산 (마지막)

# 슬라이싱
print(regions[0:2])        # ['서울', '경기']

# 추가 / 제거
regions.append("대구")
regions.remove("인천")

# 리스트 컴프리헨션 (실무에서 자주 사용)
upper_codes = [c for c in codes if c != "28"]
print(upper_codes)         # ['11', '41', '26']
""", language="python")

        st.markdown("#### 딕셔너리 (dict) — 키·값 쌍")
        st.code("""# 딕셔너리 생성
region_map = {
    "11": "서울",
    "41": "경기",
    "28": "인천",
    "26": "부산",
}

# 값 조회
print(region_map["11"])              # 서울
print(region_map.get("99", "기타")) # 기타 (없으면 기본값)

# 키·값 순회
for code, name in region_map.items():
    print(f"{code} → {name}")

# 딕셔너리 → DataFrame (실무 핵심!)
import pandas as pd
df = pd.DataFrame(list(region_map.items()), columns=["코드", "지역명"])
print(df)
""", language="python")

    with t3:
        st.markdown("#### 조건문 & 반복문")
        st.code("""# if / elif / else
score = 85
if score >= 90:
    grade = "A"
elif score >= 80:
    grade = "B"
else:
    grade = "C"
print(grade)   # B

# for 반복문
months = ["202301", "202302", "202303"]
for ym in months:
    print(f"처리 중: {ym}")

# while
count = 0
while count < 3:
    print(count)
    count += 1

# enumerate: 인덱스 + 값 동시에
for i, ym in enumerate(months):
    print(f"{i}: {ym}")

# zip: 두 리스트 동시 순회
for code, name in zip(["11","41"], ["서울","경기"]):
    print(f"{code}={name}")
""", language="python")

    with t4:
        st.markdown("#### 함수 정의 & 활용")
        st.code("""# 기본 함수
def greet(name):
    return f"안녕하세요, {name}님!"

print(greet("홍길동"))   # 안녕하세요, 홍길동님!

# 기본값 인자
def get_data(ym, limit=100):
    print(f"기준연월: {ym}, 최대건수: {limit}")

get_data("202306")          # limit=100 기본값 사용
get_data("202306", limit=500)

# 실무 패턴: 쿼리 함수화
def make_query(basis_ym: str, region_cd: str = "11") -> str:
    return f\"\"\"
        SELECT *
        FROM   TB_RGR011M_HSPRC
        WHERE  basis_ym = '{basis_ym}'
          AND  hs_loc_zone_dvcd = '{region_cd}'
        LIMIT  100
    \"\"\"

sql = make_query("202306", "41")
print(sql)
""", language="python")

    with t5:
        st.markdown("#### f-string — 문자열 포맷팅 (실무 핵심)")
        st.code("""# 기본 f-string
name = "김철수"
dept = "데이터분석팀"
print(f"담당자: {name} / 부서: {dept}")

# 숫자 포맷
amount = 1234567890
print(f"보증금액: {amount:,}원")           # 1,234,567,890원
print(f"보증금액: {amount/100000000:.1f}억") # 12.3억

# 파일명 자동 생성 (실무 자주 사용)
import datetime
today = datetime.date.today().strftime("%Y%m%d")
basis_ym = "202306"
fname = f"보증현황_{basis_ym}_{today}.csv"
print(fname)   # 보증현황_202306_20240101.csv

# 멀티라인 SQL에서 f-string
ym = "202306"
query = f\"\"\"
    SELECT * FROM TB_RGR011M_HSPRC
    WHERE  basis_ym = '{ym}'
\"\"\"
""", language="python")

    st.divider()
    _practice(
        "region_map = {'11':'서울','41':'경기','28':'인천'} 딕셔너리를 이용해서,<br>"
        "각 코드와 지역명을 <code>코드: 11 → 지역: 서울</code> 형식으로 출력하는 코드를 작성하세요.",
        """region_map = {'11': '서울', '41': '경기', '28': '인천'}
for code, name in region_map.items():
    print(f"코드: {code} → 지역: {name}")""",
        "p_py1",
        hint="딕셔너리의 .items()를 for 루프와 f-string으로 조합하세요."
    )
    nav_footer(2)


# ════════════════════════════════════════════════════════════════
# 3. Pandas 기초
# ════════════════════════════════════════════════════════════════
elif idx == 3:
    st.markdown('<div class="sec">② Pandas 기초 — DataFrame 핵심 사용법</div>', unsafe_allow_html=True)

    t1, t2, t3, t4 = st.tabs(["DataFrame 생성·조회", "필터링", "집계·그룹", "컬럼 가공"])

    with t1:
        st.markdown("#### DataFrame 생성 & 기본 조회")
        st.code("""import pandas as pd

# 딕셔너리로 DataFrame 생성
data = {
    "basis_ym": ["202306","202306","202306","202306"],
    "region":   ["서울",  "경기",  "인천",  "부산"],
    "grnt_cnt": [38300,   32200,   11200,    7100],
    "grnt_amt": [7770.0,  6330.0,  1900.0,   910.0],
}
df = pd.DataFrame(data)

# 기본 조회
print(df.shape)            # (행수, 열수) → (4, 4)
print(df.dtypes)           # 각 컬럼 타입
print(df.head(3))          # 상위 3행
print(df.tail(2))          # 하위 2행
print(df.info())           # 요약 정보
print(df.describe())       # 수치형 통계 요약

# 특정 컬럼 선택
print(df["region"])                     # Series
print(df[["region", "grnt_cnt"]])      # DataFrame
""", language="python")

        # 실제 데모 DataFrame
        st.markdown("**▶ 위 코드 실행 결과 미리보기**")
        demo_df = pd.DataFrame({
            "basis_ym": ["202306","202306","202306","202306"],
            "region":   ["서울","경기","인천","부산"],
            "grnt_cnt": [38300, 32200, 11200, 7100],
            "grnt_amt": [7770.0, 6330.0, 1900.0, 910.0],
        })
        st.dataframe(demo_df, use_container_width=True, hide_index=True)

    with t2:
        st.markdown("#### 데이터 필터링")
        st.code("""import pandas as pd
df = pd.DataFrame({
    "basis_ym": ["202306","202306","202306","202212"],
    "region":   ["서울","경기","인천","서울"],
    "grnt_cnt": [38300, 32200, 11200, 35100],
    "grnt_amt": [7770.0, 6330.0, 1900.0, 6900.0],
})

# 단일 조건
print(df[df["region"] == "서울"])

# 숫자 비교
print(df[df["grnt_cnt"] >= 30000])

# AND 조건 (&)
print(df[(df["basis_ym"] == "202306") & (df["grnt_cnt"] >= 30000)])

# OR 조건 (|)
print(df[(df["region"] == "서울") | (df["region"] == "경기")])

# isin — 목록 필터
print(df[df["region"].isin(["서울", "인천"])])

# str 패턴 필터
print(df[df["basis_ym"].str.startswith("2023")])

# 결측값 필터
print(df[df["grnt_amt"].isna()])     # NULL 행
print(df[df["grnt_amt"].notna()])    # NULL 아닌 행
""", language="python")

    with t3:
        st.markdown("#### 집계 & 그룹")
        st.code("""import pandas as pd
df = pd.DataFrame({
    "basis_ym": ["202306","202306","202212","202212"],
    "region":   ["서울","경기","서울","경기"],
    "grnt_cnt": [38300, 32200, 35100, 29800],
    "grnt_amt": [7770.0, 6330.0, 6900.0, 5800.0],
})

# 기본 집계
print(df["grnt_cnt"].sum())           # 총합
print(df["grnt_cnt"].mean())          # 평균
print(df["grnt_amt"].max())           # 최댓값
print(df["grnt_amt"].min())           # 최솟값
print(df["grnt_cnt"].count())         # 건수

# groupby 집계 (실무 핵심)
result = (
    df.groupby("basis_ym")
    .agg(
        총공급건수=("grnt_cnt", "sum"),
        평균보증금액=("grnt_amt", "mean"),
        건수=("grnt_cnt", "count"),
    )
    .reset_index()
    .round(1)
)
print(result)

# pivot_table
pivot = df.pivot_table(
    index="basis_ym",
    columns="region",
    values="grnt_cnt",
    aggfunc="sum",
    fill_value=0,
)
print(pivot)
""", language="python")
        st.markdown("**▶ groupby 결과 미리보기**")
        demo2 = pd.DataFrame({
            "basis_ym": ["202306","202306","202212","202212"],
            "region":   ["서울","경기","서울","경기"],
            "grnt_cnt": [38300, 32200, 35100, 29800],
            "grnt_amt": [7770.0, 6330.0, 6900.0, 5800.0],
        })
        r = demo2.groupby("basis_ym").agg(
            총공급건수=("grnt_cnt","sum"),
            평균보증금액=("grnt_amt","mean"),
        ).reset_index().round(1)
        st.dataframe(r, use_container_width=True, hide_index=True)

    with t4:
        st.markdown("#### 컬럼 가공 & 추가")
        st.code("""import pandas as pd
df = pd.DataFrame({
    "basis_ym": ["202306","202306","202212"],
    "region":   ["서울","경기","서울"],
    "grnt_cnt": [38300, 32200, 35100],
    "grnt_amt": [7770.0, 6330.0, 6900.0],
})

# 새 컬럼 추가
df["grnt_amt_만원"] = df["grnt_amt"] * 10000   # 억 → 만원

# apply: 함수 적용
df["연도"] = df["basis_ym"].apply(lambda x: x[:4])
df["월"]   = df["basis_ym"].apply(lambda x: x[4:])

# map: 값 변환
region_ko = {"서울": "Seoul", "경기": "Gyeonggi"}
df["region_en"] = df["region"].map(region_ko)

# 컬럼명 변경
df = df.rename(columns={
    "grnt_cnt": "공급건수",
    "grnt_amt": "보증금액(억)",
})

# 컬럼 순서 변경
df = df[["연도", "월", "region", "공급건수", "보증금액(억)"]]

print(df)
""", language="python")

    st.divider()
    _practice(
        "아래 데이터에서 <b>basis_ym='202306'</b>이고 <b>grnt_cnt >= 30000</b>인 행만 필터링하고,<br>"
        "grnt_amt 컬럼에 *10000을 적용해 <b>grnt_amt_만원</b> 컬럼을 추가하세요.",
        """import pandas as pd
df = pd.DataFrame({
    "basis_ym": ["202306","202306","202306","202212"],
    "region":   ["서울","경기","인천","서울"],
    "grnt_cnt": [38300, 32200, 11200, 35100],
    "grnt_amt": [7770.0, 6330.0, 1900.0, 6900.0],
})

# 필터링
filtered = df[(df["basis_ym"] == "202306") & (df["grnt_cnt"] >= 30000)].copy()

# 컬럼 추가
filtered["grnt_amt_만원"] = filtered["grnt_amt"] * 10000

print(filtered)""",
        "p_pd1",
        hint="조건 두 개를 &로 연결하고, .copy()로 복사본을 만든 후 컬럼을 추가하세요."
    )
    nav_footer(3)


# ════════════════════════════════════════════════════════════════
# 4. 연결·쿼리
# ════════════════════════════════════════════════════════════════
elif idx == 4:
    st.markdown('<div class="sec">③ Trino 연결 & 쿼리 실행</div>', unsafe_allow_html=True)

    st.markdown("### 기본 쿼리 실행 패턴")
    st.markdown("""사내 빅데이터 플랫폼에서 데이터를 가져오는 **표준 코드**입니다. 매번 동일한 구조를 사용합니다.""")

    st.code("""from sqlalchemy import create_engine, text
import trino
import pandas as pd
import json

# ① 엔진 생성 (스크립트 상단에 1번만)
engine = create_engine("trino://사용자ID@호스트:포트/iceberg/bronze")

# ② 연결 → 쿼리 → DataFrame
with engine.connect() as conn:
    query = \"\"\"
        SELECT
            basis_ym,
            hs_loc_zone_dvcd  AS region_cd,
            pnsn_gv_meth_dvcd AS pay_cd,
            grnt_sply_cnt,
            grnt_amt,
            kab_trd_avg_prc,
            kb_trd_avg_prc
        FROM  iceberg.bronze.TB_RGR011M_HSPRC
        WHERE basis_ym = '202306'
        LIMIT 1000
    \"\"\"
    df = pd.read_sql(text(query), conn)

# ③ JSON 파싱 (clean_payload 컬럼이 있는 경우)
if 'clean_payload' in df.columns:
    parsed = df['clean_payload'].apply(
        lambda x: json.loads(x) if pd.notnull(x) else {}
    )
    df = pd.json_normalize(parsed)

# ④ 결과 확인
display(df)
print(f"\\n총 {len(df):,}행 조회됨")
""", language="python")

    st.markdown('''<div class="tip">💡 <b>with 블록 이유</b><br>
<code>with engine.connect() as conn:</code> 구문은 쿼리가 끝나면 자동으로 DB 연결을 닫아줍니다.
연결을 수동으로 닫지 않아도 되어 리소스 낭비를 방지합니다.
</div>''', unsafe_allow_html=True)

    st.divider()
    st.markdown("### 쿼리를 함수로 관리하기 (실무 권장)")
    st.code("""from sqlalchemy import create_engine, text
import pandas as pd

engine = create_engine("trino://uid@host:8443/iceberg/bronze")

def fetch_grnt_data(basis_ym: str, region_cd: str = None,
                    limit: int = 5000) -> pd.DataFrame:
    \"\"\"보증 현황 데이터 조회 함수\"\"\"
    region_filter = f"AND hs_loc_zone_dvcd = '{region_cd}'" if region_cd else ""

    query = f\"\"\"
        SELECT
            basis_ym,
            hs_loc_zone_dvcd  AS region_cd,
            pnsn_gv_meth_dvcd AS pay_cd,
            SUM(grnt_sply_cnt) AS total_cnt,
            ROUND(SUM(grnt_amt), 1) AS total_amt
        FROM  iceberg.bronze.TB_RGR011M_HSPRC
        WHERE basis_ym = '{basis_ym}'
        {region_filter}
        GROUP BY 1, 2, 3
        ORDER BY total_cnt DESC
        LIMIT {limit}
    \"\"\"
    with engine.connect() as conn:
        return pd.read_sql(text(query), conn)


# 사용 예
df_all    = fetch_grnt_data("202306")             # 전체
df_seoul  = fetch_grnt_data("202306", region_cd="11")  # 서울만
df_recent = fetch_grnt_data("202306", limit=100)  # 상위 100건
""", language="python")

    st.divider()
    st.markdown("### 여러 연월 반복 조회 패턴")
    st.code("""import pandas as pd
from sqlalchemy import create_engine, text

engine = create_engine("trino://uid@host:8443/iceberg/bronze")

# 조회할 연월 목록
ym_list = ["202301", "202302", "202303", "202304", "202305", "202306"]

results = []
for ym in ym_list:
    print(f"조회 중: {ym} ...")
    with engine.connect() as conn:
        query = f\"\"\"
            SELECT '{ym}' AS basis_ym,
                   SUM(grnt_sply_cnt) AS total_cnt,
                   ROUND(SUM(grnt_amt), 1) AS total_amt
            FROM   iceberg.bronze.TB_RGR011M_HSPRC
            WHERE  basis_ym = '{ym}'
        \"\"\"
        df_tmp = pd.read_sql(text(query), conn)
        results.append(df_tmp)

# 전체 결합
df_all = pd.concat(results, ignore_index=True)
print(df_all)
""", language="python")

    st.markdown('''<div class="wrn">⚠️ <b>대용량 주의</b><br>
LIMIT 없이 조회하면 수억 건이 내려올 수 있습니다.<br>
개발·확인 단계에서는 항상 <code>LIMIT 1000</code> 등을 붙이고, 최종 확정 후 제거하세요.
</div>''', unsafe_allow_html=True)

    nav_footer(4)


# ════════════════════════════════════════════════════════════════
# 5. JSON 처리
# ════════════════════════════════════════════════════════════════
elif idx == 5:
    st.markdown('<div class="sec">④ JSON 처리 — clean_payload 파싱</div>', unsafe_allow_html=True)

    st.markdown("""
빅데이터 플랫폼의 일부 테이블은 여러 필드를 **JSON 문자열**로 묶어 `clean_payload` 컬럼에 저장합니다.
이를 개별 컬럼으로 펼쳐야 실제 분석이 가능합니다.
""")

    st.markdown("### ① clean_payload 파싱 표준 코드")
    st.code("""import json
import pandas as pd

# DB에서 가져온 DataFrame (clean_payload 컬럼 포함 가정)
# df = pd.read_sql(text(query), conn)

# clean_payload 컬럼이 있는지 확인 후 분기
if 'clean_payload' in df.columns:
    # 각 행의 JSON 문자열 → dict로 변환
    parsed_data = df['clean_payload'].apply(
        lambda x: json.loads(x) if pd.notnull(x) else {}
    )
    # dict 리스트 → DataFrame (중첩 구조도 자동 펼침)
    df = pd.json_normalize(parsed_data)
    display(df)
else:
    # clean_payload 없으면 그냥 출력
    display(df)
""", language="python")

    st.markdown('''<div class="tip">💡 <b>pd.json_normalize 란?</b><br>
딕셔너리가 중첩된 구조(nested dict)도 자동으로 평탄화(flatten)해줍니다.<br>
예: <code>{{"user": {{"name": "홍길동", "age": 30}}}}</code> → <code>user.name</code>, <code>user.age</code> 컬럼 생성
</div>''', unsafe_allow_html=True)

    st.divider()
    st.markdown("### ② 직접 실습 — JSON 파싱 시뮬레이션")

    sample_json_rows = [
        '{"grnt_no":"G-2023-001","region":"서울","pay_type":"전세","amount":185050}',
        '{"grnt_no":"G-2023-002","region":"경기","pay_type":"구입","amount":312000}',
        '{"grnt_no":"G-2023-003","region":"인천","pay_type":"전세","amount":98000}',
        None,
        '{"grnt_no":"G-2023-005","region":"부산","pay_type":"전세","amount":54000}',
    ]
    demo_df = pd.DataFrame({"clean_payload": sample_json_rows})

    st.markdown("**샘플 데이터 (clean_payload 컬럼)**")
    st.dataframe(demo_df, use_container_width=True, hide_index=True)

    if st.button("▶ JSON 파싱 실행", type="primary"):
        parsed = demo_df["clean_payload"].apply(
            lambda x: json.loads(x) if pd.notnull(x) else {}
        )
        result_df = pd.json_normalize(parsed)
        st.markdown('<div class="okb">✅ 파싱 완료 — clean_payload가 개별 컬럼으로 분리되었습니다.</div>', unsafe_allow_html=True)
        st.dataframe(result_df, use_container_width=True, hide_index=True)

    st.divider()
    st.markdown("### ③ NULL·오류 안전 처리")
    st.code("""import json
import pandas as pd

def safe_json_parse(x):
    \"\"\"JSON 파싱 실패·NULL 모두 안전하게 처리\"\"\"
    if pd.isnull(x):
        return {}
    try:
        return json.loads(x)
    except (json.JSONDecodeError, TypeError):
        return {}   # 파싱 실패 시 빈 dict 반환

# 적용
parsed = df['clean_payload'].apply(safe_json_parse)
df_flat = pd.json_normalize(parsed)

# 특정 키만 추출하고 싶은 경우
df['grnt_no'] = df['clean_payload'].apply(
    lambda x: safe_json_parse(x).get('grnt_no', None)
)
""", language="python")

    st.markdown('''<div class="wrn">⚠️ <b>실무 주의사항</b><br>
• NULL 값이 있으면 <code>json.loads(None)</code>이 TypeError를 발생시킵니다. 반드시 <code>pd.notnull(x)</code> 체크 필요<br>
• JSON 형식이 잘못된 행이 섞여 있을 수 있으므로 try/except로 감싸는 것을 권장합니다
</div>''', unsafe_allow_html=True)

    nav_footer(5)


# ════════════════════════════════════════════════════════════════
# 6. 파일 저장
# ════════════════════════════════════════════════════════════════
elif idx == 6:
    st.markdown('<div class="sec">⑤ 파일 저장 — CSV & Excel 내보내기</div>', unsafe_allow_html=True)

    t1, t2, t3 = st.tabs(["CSV 저장", "Excel 저장", "파일명 자동화"])

    with t1:
        st.markdown("#### CSV 저장 표준 코드")
        st.code("""import pandas as pd

# ── 기본 CSV 저장 ───────────────────────────────────────────────
csv_file_name = "보증현황_202306.csv"
df.to_csv(
    csv_file_name,
    index=False,           # 인덱스 번호 저장 안 함 (권장)
    encoding="utf-8-sig",  # 한글 깨짐 방지 (Excel에서 열 때 필수)
)
print(f"저장 완료: {csv_file_name}")

# ── 특정 컬럼만 저장 ────────────────────────────────────────────
df[["basis_ym", "region", "grnt_cnt"]].to_csv(
    "요약.csv",
    index=False,
    encoding="utf-8-sig",
)

# ── 구분자 변경 (탭 구분) ───────────────────────────────────────
df.to_csv("output.tsv", sep="\\t", index=False, encoding="utf-8-sig")
""", language="python")
        st.markdown('''<div class="wrn">⚠️ <b>인코딩 주의</b><br>
<code>encoding="utf-8"</code>로 저장하면 Windows Excel에서 한글이 깨집니다.<br>
반드시 <code>encoding="utf-8-sig"</code> (BOM 포함)을 사용하세요.
</div>''', unsafe_allow_html=True)

    with t2:
        st.markdown("#### Excel 저장 (.xlsx)")
        st.code("""import pandas as pd

# ── 단일 시트 ───────────────────────────────────────────────────
df.to_excel(
    "보증현황_202306.xlsx",
    index=False,
    sheet_name="보증현황",
)

# ── 여러 시트로 저장 (실무 자주 사용) ──────────────────────────
with pd.ExcelWriter("보증현황_종합.xlsx", engine="openpyxl") as writer:
    df_all.to_excel(writer, sheet_name="전체", index=False)
    df_seoul.to_excel(writer, sheet_name="서울", index=False)
    df_gyeonggi.to_excel(writer, sheet_name="경기", index=False)

# ── 열 너비 자동 조정 포함 ──────────────────────────────────────
with pd.ExcelWriter("보증현황_포맷.xlsx", engine="openpyxl") as writer:
    df.to_excel(writer, sheet_name="데이터", index=False)
    ws = writer.sheets["데이터"]
    for col in ws.columns:
        max_len = max(len(str(cell.value or "")) for cell in col)
        ws.column_dimensions[col[0].column_letter].width = min(max_len + 2, 40)

print("Excel 저장 완료")
""", language="python")
        st.markdown('''<div class="tip">💡 <b>openpyxl 설치 필요</b><br>
<code>pip install openpyxl</code> 설치 후 사용하세요.
Excel 포맷(.xlsx) 저장·읽기 모두 openpyxl 엔진이 처리합니다.
</div>''', unsafe_allow_html=True)

    with t3:
        st.markdown("#### 파일명 자동화 — 날짜·조건 포함")
        st.code("""import datetime
import pandas as pd

today     = datetime.date.today().strftime("%Y%m%d")
now       = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
basis_ym  = "202306"
region_cd = "11"

# 패턴 1: 기준연월 + 오늘날짜
fname_csv   = f"보증현황_{basis_ym}_{today}.csv"
fname_excel = f"보증현황_{basis_ym}_{today}.xlsx"

# 패턴 2: 조건 포함
fname_region = f"보증현황_{basis_ym}_지역{region_cd}_{today}.csv"

# 패턴 3: 타임스탬프 (실행시각 포함, 덮어쓰기 방지)
fname_ts = f"보증현황_{now}.csv"

# 저장
df.to_csv(fname_csv, index=False, encoding="utf-8-sig")
print(f"저장 완료: {fname_csv}")

# 패턴 4: 폴더 포함 경로
import os
os.makedirs("output", exist_ok=True)  # 없으면 생성
df.to_csv(f"output/{fname_csv}", index=False, encoding="utf-8-sig")
""", language="python")

        st.divider()
        st.markdown("#### 실습: 다운로드 버튼 (Streamlit 환경)")
        demo_df = pd.DataFrame({
            "basis_ym": ["202306","202306","202306"],
            "region":   ["서울","경기","인천"],
            "grnt_cnt": [38300, 32200, 11200],
            "grnt_amt": [7770.0, 6330.0, 1900.0],
        })
        st.dataframe(demo_df, use_container_width=True, hide_index=True)
        c1, c2 = st.columns(2)
        with c1:
            csv_bytes = demo_df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
            st.download_button("⬇️ CSV 다운로드", data=csv_bytes,
                               file_name="보증현황_샘플.csv", mime="text/csv")
        with c2:
            buf = io.BytesIO()
            demo_df.to_excel(buf, index=False, sheet_name="보증현황")
            st.download_button("⬇️ Excel 다운로드", data=buf.getvalue(),
                               file_name="보증현황_샘플.xlsx",
                               mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    nav_footer(6)


# ════════════════════════════════════════════════════════════════
# 7. 데이터 정제
# ════════════════════════════════════════════════════════════════
elif idx == 7:
    st.markdown('<div class="sec">⑥ 데이터 정제 — 실무에서 자주 만나는 상황</div>', unsafe_allow_html=True)

    t1, t2, t3, t4 = st.tabs(["결측값(NULL)", "중복 제거", "타입 변환", "컬럼 정리"])

    with t1:
        st.markdown("#### 결측값(NULL/NaN) 처리")
        st.code("""import pandas as pd
import numpy as np

df = pd.DataFrame({
    "region":   ["서울","경기",None,"부산"],
    "grnt_cnt": [38300, None, 11200, 7100],
    "grnt_amt": [7770.0, 6330.0, None, 910.0],
})

# 결측값 확인
print(df.isnull().sum())          # 컬럼별 NULL 개수
print(df.isnull().any())          # NULL 있는 컬럼 True/False

# 행 단위 제거
df_drop = df.dropna()             # NULL 포함 행 전체 제거
df_drop2 = df.dropna(subset=["grnt_cnt"])  # 특정 컬럼만 기준

# 채우기
df_fill = df.copy()
df_fill["grnt_cnt"] = df_fill["grnt_cnt"].fillna(0)         # 0으로
df_fill["region"]   = df_fill["region"].fillna("미상")       # 문자로
df_fill["grnt_amt"] = df_fill["grnt_amt"].fillna(df_fill["grnt_amt"].mean())  # 평균

# 앞/뒤 값으로 채우기
df_fill["grnt_cnt"] = df_fill["grnt_cnt"].ffill()  # 앞 값
df_fill["grnt_cnt"] = df_fill["grnt_cnt"].bfill()  # 뒤 값

print(df_fill)
""", language="python")
        st.markdown('''<div class="tip">💡 실무에서는 무조건 dropna()보다 상황에 맞게 판단하세요.<br>
수치형: fillna(0) 또는 평균/중앙값 | 코드형: fillna("기타") | 핵심 컬럼 NULL은 drop
</div>''', unsafe_allow_html=True)

    with t2:
        st.markdown("#### 중복 행 처리")
        st.code("""import pandas as pd

df = pd.DataFrame({
    "grnt_no":  ["G-001","G-001","G-002","G-003","G-003"],
    "basis_ym": ["202306","202306","202306","202306","202212"],
    "region":   ["서울","서울","경기","인천","인천"],
})

# 중복 확인
print(df.duplicated().sum())               # 전체 중복 행 수
print(df.duplicated(subset=["grnt_no"]))   # 특정 컬럼 기준

# 중복 제거
df_unique = df.drop_duplicates()                        # 완전 중복 제거
df_unique2 = df.drop_duplicates(subset=["grnt_no"])     # grnt_no 기준
df_unique3 = df.drop_duplicates(subset=["grnt_no"], keep="last")  # 마지막 유지

print(df_unique2)
""", language="python")

    with t3:
        st.markdown("#### 타입 변환 — DB에서 문자열로 오는 경우")
        st.code("""import pandas as pd

df = pd.DataFrame({
    "basis_ym": ["202306","202306","202306"],
    "grnt_cnt": ["38300", "32200", "11200"],  # 문자열로 옴
    "grnt_amt": ["7770.0","6330.0","1900.0"],  # 문자열로 옴
    "del_yn":   ["N","N","Y"],
})

# 수치형 변환
df["grnt_cnt"] = pd.to_numeric(df["grnt_cnt"], errors="coerce")
df["grnt_amt"] = df["grnt_amt"].astype(float)

# 날짜 변환 (YYYYMM 형식)
df["year"]  = df["basis_ym"].str[:4].astype(int)
df["month"] = df["basis_ym"].str[4:].astype(int)

# YYYYMMDD → datetime
date_df = pd.DataFrame({"date_str": ["20230601","20230615"]})
date_df["date"] = pd.to_datetime(date_df["date_str"], format="%Y%m%d")

# bool 변환
df["is_valid"] = df["del_yn"] == "N"

print(df.dtypes)
print(df)
""", language="python")

    with t4:
        st.markdown("#### 컬럼 정리 & 순서 변경")
        st.code("""import pandas as pd

df = pd.DataFrame({
    "basis_ym": ["202306"],
    "hs_loc_zone_dvcd": ["11"],
    "pnsn_gv_meth_dvcd": ["07"],
    "grnt_sply_cnt": [38300],
    "grnt_amt": [7770.0],
    "aprs_eval_amt": [7200.0],
    "kab_trd_avg_prc": [None],
    "kb_trd_avg_prc": [7500.0],
})

# 컬럼명 한글화
rename_map = {
    "basis_ym":           "기준연월",
    "hs_loc_zone_dvcd":   "지역코드",
    "pnsn_gv_meth_dvcd":  "지급방식코드",
    "grnt_sply_cnt":      "공급건수",
    "grnt_amt":           "보증금액(억)",
    "aprs_eval_amt":      "감정가(억)",
    "kab_trd_avg_prc":    "KB거래가(억)",
    "kb_trd_avg_prc":     "KB시세(억)",
}
df = df.rename(columns=rename_map)

# 필요한 컬럼만 선택 & 순서 지정
cols = ["기준연월","지역코드","지급방식코드","공급건수","보증금액(억)","KB시세(억)"]
df = df[cols]

# 불필요한 컬럼 제거
df = df.drop(columns=["지급방식코드"], errors="ignore")

print(df)
""", language="python")

    st.divider()
    _practice(
        "아래 DataFrame에서:<br>"
        "① grnt_cnt, grnt_amt를 수치형으로 변환<br>"
        "② grnt_cnt가 NULL인 행 제거<br>"
        "③ 컬럼명을 <b>공급건수, 보증금액</b>으로 변경하세요.",
        """import pandas as pd

df = pd.DataFrame({
    "basis_ym": ["202306","202306","202306","202306"],
    "grnt_cnt": ["38300", None, "11200", "7100"],
    "grnt_amt": ["7770.0","6330.0",None,"910.0"],
})

# ① 수치형 변환
df["grnt_cnt"] = pd.to_numeric(df["grnt_cnt"], errors="coerce")
df["grnt_amt"] = pd.to_numeric(df["grnt_amt"], errors="coerce")

# ② NULL 행 제거
df = df.dropna(subset=["grnt_cnt"])

# ③ 컬럼명 변경
df = df.rename(columns={"grnt_cnt": "공급건수", "grnt_amt": "보증금액"})

print(df)""",
        "p_clean1",
        hint="pd.to_numeric(errors='coerce'), dropna(subset=[...]), rename(columns={...}) 순서대로"
    )
    nav_footer(7)


# ════════════════════════════════════════════════════════════════
# 8. 실전 템플릿
# ════════════════════════════════════════════════════════════════
elif idx == 8:
    st.markdown('<div class="sec">🚀 실전 완성 템플릿</div>', unsafe_allow_html=True)
    st.markdown("실무에서 바로 복사해 사용할 수 있는 **전체 파이프라인 코드**입니다.")

    t1, t2, t3 = st.tabs(["기본 템플릿", "JSON 포함 템플릿", "다중연월 배치"])

    with t1:
        st.markdown("#### 기본 쿼리 → CSV/Excel 저장 완성 코드")
        st.code("""# ══════════════════════════════════════════════════════
# 빅데이터 플랫폼 데이터 추출 템플릿 (기본)
# ══════════════════════════════════════════════════════
from sqlalchemy import create_engine, text
import trino
import pandas as pd
import datetime
import os

# ── 설정 ────────────────────────────────────────────────────────
ENGINE_URL = "trino://사용자ID@호스트:포트/iceberg/bronze"
BASIS_YM   = "202306"                    # ← 조회 기준연월 변경
OUTPUT_DIR = "output"                    # ← 저장 폴더

# ── Pandas 출력 옵션 ─────────────────────────────────────────────
pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', None)
pd.options.display.float_format = '{:,.0f}'.format

# ── 엔진 생성 ────────────────────────────────────────────────────
engine = create_engine(ENGINE_URL)

# ── 쿼리 실행 ────────────────────────────────────────────────────
with engine.connect() as conn:
    query = f\"\"\"
        SELECT
            basis_ym                        AS 기준연월,
            hs_loc_zone_dvcd                AS 지역코드,
            pnsn_gv_meth_dvcd               AS 지급방식코드,
            SUM(grnt_sply_cnt)              AS 공급건수,
            ROUND(SUM(grnt_amt), 1)         AS "보증금액(억)",
            ROUND(AVG(kab_trd_avg_prc), 2)  AS "KB거래평균가(억)",
            ROUND(AVG(kb_trd_avg_prc), 2)   AS "KB시세평균가(억)"
        FROM  iceberg.bronze.TB_RGR011M_HSPRC
        WHERE basis_ym = '{BASIS_YM}'
        GROUP BY 1, 2, 3
        ORDER BY 공급건수 DESC
    \"\"\"
    df = pd.read_sql(text(query), conn)

print(f"✅ 조회 완료: {len(df):,}행")
display(df.head(20))

# ── 파일 저장 ────────────────────────────────────────────────────
today = datetime.date.today().strftime("%Y%m%d")
os.makedirs(OUTPUT_DIR, exist_ok=True)

csv_path   = f"{OUTPUT_DIR}/보증현황_{BASIS_YM}_{today}.csv"
excel_path = f"{OUTPUT_DIR}/보증현황_{BASIS_YM}_{today}.xlsx"

df.to_csv(csv_path, index=False, encoding="utf-8-sig")
df.to_excel(excel_path, index=False, sheet_name="보증현황")

print(f"📁 CSV   저장: {csv_path}")
print(f"📁 Excel 저장: {excel_path}")
""", language="python")

    with t2:
        st.markdown("#### clean_payload JSON 포함 완성 코드")
        st.code("""# ══════════════════════════════════════════════════════
# 빅데이터 플랫폼 데이터 추출 템플릿 (JSON 파싱 포함)
# ══════════════════════════════════════════════════════
from sqlalchemy import create_engine, text
import trino
import pandas as pd
import json
import datetime
import os

pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', None)
pd.options.display.float_format = '{:,.0f}'.format

ENGINE_URL = "trino://사용자ID@호스트:포트/iceberg/bronze"
BASIS_YM   = "202306"

engine = create_engine(ENGINE_URL)

with engine.connect() as conn:
    query = f\"\"\"
        SELECT *
        FROM   iceberg.bronze.YOUR_TABLE
        WHERE  basis_ym = '{BASIS_YM}'
        LIMIT  10000
    \"\"\"
    df = pd.read_sql(text(query), conn)

# ── JSON 파싱 분기 ───────────────────────────────────────────────
if 'clean_payload' in df.columns:
    def safe_parse(x):
        if pd.isnull(x):
            return {}
        try:
            return json.loads(x)
        except (json.JSONDecodeError, TypeError):
            return {}

    parsed_data = df['clean_payload'].apply(safe_parse)
    df = pd.json_normalize(parsed_data)
    print("✅ JSON 파싱 완료")
else:
    print("ℹ️ clean_payload 없음 — 원본 DataFrame 사용")

display(df)
print(f"총 {len(df):,}행 | 컬럼: {list(df.columns)}")

# ── 저장 ─────────────────────────────────────────────────────────
today = datetime.date.today().strftime("%Y%m%d")
os.makedirs("output", exist_ok=True)

csv_name = f"output/추출데이터_{BASIS_YM}_{today}.csv"
df.to_csv(csv_name, index=False, encoding="utf-8-sig")
print(f"📁 저장 완료: {csv_name}")
""", language="python")

    with t3:
        st.markdown("#### 다중 연월 배치 처리 템플릿")
        st.code("""# ══════════════════════════════════════════════════════
# 여러 연월 반복 조회 → 하나의 파일로 합치기
# ══════════════════════════════════════════════════════
from sqlalchemy import create_engine, text
import pandas as pd
import datetime
import os

pd.set_option('display.max_columns', None)
pd.options.display.float_format = '{:,.0f}'.format

ENGINE_URL = "trino://사용자ID@호스트:포트/iceberg/bronze"

# ── 조회 기간 설정 ────────────────────────────────────────────────
YM_LIST = [
    "202301","202302","202303",
    "202304","202305","202306",
]

engine = create_engine(ENGINE_URL)
all_results = []

for ym in YM_LIST:
    print(f"⏳ 조회 중: {ym} ...")
    with engine.connect() as conn:
        query = f\"\"\"
            SELECT
                '{ym}'                       AS 기준연월,
                hs_loc_zone_dvcd             AS 지역코드,
                SUM(grnt_sply_cnt)           AS 공급건수,
                ROUND(SUM(grnt_amt), 1)      AS "보증금액(억)"
            FROM  iceberg.bronze.TB_RGR011M_HSPRC
            WHERE basis_ym = '{ym}'
            GROUP BY 1, 2
            ORDER BY 공급건수 DESC
        \"\"\"
        df_tmp = pd.read_sql(text(query), conn)
        all_results.append(df_tmp)
        print(f"   → {len(df_tmp):,}행")

# ── 전체 합치기 ───────────────────────────────────────────────────
df_all = pd.concat(all_results, ignore_index=True)
print(f"\\n✅ 전체 완료: {len(df_all):,}행")
display(df_all)

# ── 저장 ─────────────────────────────────────────────────────────
today     = datetime.date.today().strftime("%Y%m%d")
start_ym  = YM_LIST[0]
end_ym    = YM_LIST[-1]
os.makedirs("output", exist_ok=True)

csv_name = f"output/보증현황_{start_ym}_{end_ym}_{today}.csv"
df_all.to_csv(csv_name, index=False, encoding="utf-8-sig")

# 연월별 시트로 Excel 저장
excel_name = f"output/보증현황_{start_ym}_{end_ym}_{today}.xlsx"
with pd.ExcelWriter(excel_name, engine="openpyxl") as writer:
    df_all.to_excel(writer, sheet_name="전체", index=False)
    for ym in YM_LIST:
        df_ym = df_all[df_all["기준연월"] == ym]
        df_ym.to_excel(writer, sheet_name=ym, index=False)

print(f"📁 CSV   저장: {csv_name}")
print(f"📁 Excel 저장: {excel_name} (연월별 시트)")
""", language="python")

    nav_footer(8)


# ════════════════════════════════════════════════════════════════
# 9. 연습과제
# ════════════════════════════════════════════════════════════════
elif idx == 9:
    st.markdown('<div class="sec">✏️ 연습과제</div>', unsafe_allow_html=True)

    t_quiz, t_ref = st.tabs(["📝 실습 문제", "📋 빠른 참조"])

    with t_quiz:
        st.markdown("각 문제를 직접 풀고 **📋 정답 보기**로 확인하세요.")
        st.divider()

        # 문제 1
        st.markdown("### 문제 1 &nbsp; 🟢 기초 — f-string & 파일명 생성")
        _practice(
            "basis_ym = '202306', region = '서울' 변수를 이용하여<br>"
            "<code>보증현황_서울_202306_오늘날짜.csv</code> 형식의 파일명 문자열을 만들고 출력하세요.",
            """import datetime
basis_ym = "202306"
region   = "서울"
today    = datetime.date.today().strftime("%Y%m%d")
fname    = f"보증현황_{region}_{basis_ym}_{today}.csv"
print(fname)""",
            "q1",
            hint="datetime.date.today().strftime('%Y%m%d')로 오늘날짜를 문자열로 만드세요."
        )
        st.divider()

        # 문제 2
        st.markdown("### 문제 2 &nbsp; 🟢 기초 — DataFrame 필터 & 집계")
        _practice(
            "아래 DataFrame에서 공급건수 합계가 가장 큰 지역을 출력하세요.<br><br>"
            "<code>df = pd.DataFrame({'region':['서울','경기','인천','부산'],'grnt_cnt':[38300,32200,11200,7100]})</code>",
            """import pandas as pd
df = pd.DataFrame({
    "region":   ["서울","경기","인천","부산"],
    "grnt_cnt": [38300, 32200, 11200, 7100],
})
top = df.loc[df["grnt_cnt"].idxmax(), "region"]
print(f"공급건수 최대 지역: {top}")""",
            "q2",
            hint="df['grnt_cnt'].idxmax()로 최댓값 인덱스를 구하고, df.loc[...]으로 지역명을 가져오세요."
        )
        st.divider()

        # 문제 3
        st.markdown("### 문제 3 &nbsp; 🟡 중급 — JSON 파싱")
        _practice(
            "아래 JSON 문자열 리스트를 DataFrame으로 변환하고, amount 컬럼을 기준으로 내림차순 정렬하세요.<br><br>"
            "<code>rows = ['{\"region\":\"서울\",\"amount\":185050}', '{\"region\":\"경기\",\"amount\":312000}', '{\"region\":\"인천\",\"amount\":98000}']</code>",
            """import json
import pandas as pd

rows = [
    '{"region":"서울","amount":185050}',
    '{"region":"경기","amount":312000}',
    '{"region":"인천","amount":98000}',
]

parsed = [json.loads(r) for r in rows]
df = pd.json_normalize(parsed)
df = df.sort_values("amount", ascending=False).reset_index(drop=True)
print(df)""",
            "q3",
            hint="json.loads()로 각 행을 dict로 변환 후 리스트로 모아 pd.json_normalize()에 전달하세요."
        )
        st.divider()

        # 문제 4
        st.markdown("### 문제 4 &nbsp; 🟡 중급 — 데이터 정제 & CSV 저장")
        _practice(
            "아래 DataFrame에서 ① grnt_cnt를 수치형으로 변환, ② NULL 행 제거, "
            "③ grnt_cnt >= 10000인 행만 남기고, ④ CSV 바이트로 변환하여 print하세요.",
            """import pandas as pd, io

df = pd.DataFrame({
    "region":   ["서울","경기","인천","부산","대구"],
    "grnt_cnt": ["38300","32200",None,"7100","11500"],
})

# ① 수치형 변환
df["grnt_cnt"] = pd.to_numeric(df["grnt_cnt"], errors="coerce")

# ② NULL 제거
df = df.dropna(subset=["grnt_cnt"])

# ③ 필터
df = df[df["grnt_cnt"] >= 10000].reset_index(drop=True)

# ④ CSV 바이트 변환
csv_bytes = df.to_csv(index=False, encoding="utf-8-sig")
print(csv_bytes)
print(df)""",
            "q4",
            hint="pd.to_numeric → dropna → 필터 → to_csv 순서대로 처리하세요."
        )
        st.divider()

        # 문제 5
        st.markdown("### 문제 5 &nbsp; 🔴 심화 — 전체 파이프라인")
        _practice(
            "아래 데이터를 이용해 전체 파이프라인을 완성하세요:<br>"
            "① 지역별 공급건수 합계 집계 (groupby)<br>"
            "② 컬럼명 한글화<br>"
            "③ 공급건수 내림차순 정렬<br>"
            "④ CSV 문자열로 출력 (index=False, utf-8-sig)",
            """import pandas as pd

df = pd.DataFrame({
    "basis_ym": ["202306"]*6,
    "region_cd": ["11","41","28","11","41","26"],
    "grnt_cnt":  [22000,18000,9000,16300,14200,7100],
})

# ① groupby 집계
result = (
    df.groupby("region_cd")
    .agg(grnt_cnt=("grnt_cnt","sum"))
    .reset_index()
)

# ② 컬럼명 한글화
result = result.rename(columns={"region_cd":"지역코드","grnt_cnt":"공급건수합계"})

# ③ 정렬
result = result.sort_values("공급건수합계", ascending=False).reset_index(drop=True)

# ④ CSV 출력
print(result.to_csv(index=False, encoding="utf-8-sig"))
print(result)""",
            "q5",
            hint="groupby → rename → sort_values → to_csv 순서로 체이닝하세요."
        )

    with t_ref:
        st.markdown("### 📋 빠른 참조 — 실무 치트시트")
        c1, c2 = st.columns(2)
        with c1:
            st.markdown("**표준 import 블록**")
            st.code("""from sqlalchemy import create_engine, text
import trino
import pandas as pd
import json
import datetime, os

pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', None)
pd.options.display.float_format = '{:,.0f}'.format""", language="python")

            st.markdown("**Trino 연결 & 쿼리**")
            st.code("""engine = create_engine("trino://uid@host:port/catalog/schema")

with engine.connect() as conn:
    df = pd.read_sql(text(\"\"\"
        SELECT * FROM table WHERE basis_ym = '202306'
    \"\"\"), conn)""", language="python")

            st.markdown("**JSON 파싱**")
            st.code("""if 'clean_payload' in df.columns:
    parsed = df['clean_payload'].apply(
        lambda x: json.loads(x) if pd.notnull(x) else {}
    )
    df = pd.json_normalize(parsed)""", language="python")

        with c2:
            st.markdown("**데이터 정제 패턴**")
            st.code("""# 타입 변환
df["col"] = pd.to_numeric(df["col"], errors="coerce")
df["date"] = pd.to_datetime(df["date"], format="%Y%m%d")

# NULL 처리
df = df.dropna(subset=["핵심컬럼"])
df["col"] = df["col"].fillna(0)

# 중복 제거
df = df.drop_duplicates(subset=["key_col"])

# 컬럼 한글화
df = df.rename(columns={"eng_col": "한글컬럼"})""", language="python")

            st.markdown("**파일 저장**")
            st.code("""today = datetime.date.today().strftime("%Y%m%d")
os.makedirs("output", exist_ok=True)

# CSV
df.to_csv(f"output/파일명_{today}.csv",
          index=False, encoding="utf-8-sig")

# Excel (다중 시트)
with pd.ExcelWriter(f"output/파일명_{today}.xlsx",
                    engine="openpyxl") as writer:
    df.to_excel(writer, sheet_name="데이터", index=False)""", language="python")

        st.divider()
        st.markdown('''<div class="tip">
✅ <b>실수 방지 체크리스트</b><br>
• CSV 저장 시 반드시 <code>encoding="utf-8-sig"</code> (한글 깨짐 방지)<br>
• <code>index=False</code> 누락 → 불필요한 행 번호 컬럼 생성<br>
• NULL 체크 없이 <code>json.loads()</code> → TypeError 발생<br>
• LIMIT 없는 대용량 쿼리 → 수억 건 다운로드 위험<br>
• <code>display.max_cloumns</code>(오타) → 실제 옵션: <code>display.max_columns</code><br>
• <code>to_numeric(errors="coerce")</code>: 변환 실패 시 NaN (오류 중단 방지)<br>
• groupby 후 반드시 <code>.reset_index()</code> 호출해야 인덱스 정상화
</div>''', unsafe_allow_html=True)

    nav_footer(9)

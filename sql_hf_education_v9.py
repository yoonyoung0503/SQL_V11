import streamlit as st
import sqlite3
import pandas as pd

# ════════════════════════════════════════════════════════════════
# SQLAlchemy 내부망 연결 설정
# USE_INTERNAL_DB = True 로 변경하고 CONNECTION_STRING 수정 후 사용
# ════════════════════════════════════════════════════════════════
USE_INTERNAL_DB   = False
CONNECTION_STRING = (
    # Trino(사내 Iceberg) 예시
    # "trino://유저명@호스트:포트/iceberg/bronze"
    # MS-SQL 예시
    # "mssql+pyodbc://서버명/DB명?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
    ""
)
# 실제 테이블명 (스키마 포함 시: "iceberg.bronze.TA_COA311M_CDBSC")
TBL_COA = "TA_COA311M_CDBSC"
TBL_RGR = "TB_RGR011M_HSPRC"

try:
    from sqlalchemy import create_engine, text as _satext
    _SA = True
except ImportError:
    _SA = False

try:
    import trino  # noqa: F401  사내 trino 드라이버
except ImportError:
    pass


@st.cache_resource
def _engine():
    if not (USE_INTERNAL_DB and _SA and CONNECTION_STRING):
        return None
    try:
        eng = create_engine(CONNECTION_STRING, pool_pre_ping=True)
        with eng.connect() as c:
            c.execute(_satext("SELECT 1"))
        return eng
    except Exception as e:
        st.warning(f"내부DB 연결 실패 → 데모 DB 사용: {e}")
        return None


# ════════════════════════════════════════════════════════════════
# SQLite 데모 DB  —  COA311M / RGR011M 실제 컬럼 기반
# ════════════════════════════════════════════════════════════════
@st.cache_resource
def _demo():
    c = sqlite3.connect(":memory:", check_same_thread=False)
    c.executescript("""
-- ── TA_COA311M_CDBSC : 공통코드 마스터 (실제 컬럼 기준) ──────
CREATE TABLE TA_COA311M_CDBSC (
    cd_unq_no       TEXT,          -- 고유번호
    cd_grp_id       TEXT,          -- 코드그룹ID  (COA0083=지역, COA0021=지급방식)
    up_cd_grp_id    TEXT,          -- 상위 코드그룹ID
    valid_strt_dy   TEXT,          -- 유효시작일자 (YYYYMMDD)
    valid_end_dy    TEXT,          -- 유효종료일자 (YYYYMMDD)
    cd_id           TEXT,          -- 코드ID  ← JOIN 키
    cd_nm           TEXT,          -- 코드명  ← 실무에서 조회하는 값
    up_cd_id        TEXT,          -- 상위 코드ID
    cd_expn_cont    TEXT,          -- 코드설명
    idct_seq        INTEGER,       -- 정렬순서
    cd_val          TEXT,          -- 코드값
    min_val         TEXT,          -- 최솟값
    max_val         TEXT,          -- 최댓값
    del_yn          TEXT           -- 삭제여부 (Y/N)
);

-- ── TB_RGR011M_HSPRC : 시세·보증 현황 (교육용 컬럼 포함) ──────
CREATE TABLE TB_RGR011M_HSPRC (
    basis_ym            TEXT,      -- 기준연월 (YYYYMM)
    grnt_no             TEXT,      -- 보증번호
    hs_loc_zone_dvcd    TEXT,      -- 주택소재지역코드 → COA311M.cd_id (COA0083)
    pnsn_gv_meth_dvcd   TEXT,      -- 지급방식코드 → COA311M.cd_id (COA0021)
    grnt_sply_cnt       INTEGER,   -- 공급건수
    grnt_amt            REAL,      -- 보증금액 (억원)
    aprs_eval_amt       REAL,      -- 감정평가금액 (억원)
    kab_trd_avg_prc     REAL,      -- KB거래평균가격 (억원, NULL 가능)
    kb_trd_avg_prc      REAL,      -- KB시세평균가격 (억원, NULL 가능)
    kab_sise_rsch_dy    TEXT       -- 시세조사구분 (0=국민은행, 기타=부동산원)
);

-- ── 공통코드 데이터 (실제 컬럼 순서 맞춤) ─────────────────────
-- cd_unq_no, cd_grp_id, up_cd_grp_id, valid_strt_dy, valid_end_dy,
-- cd_id, cd_nm, up_cd_id, cd_expn_cont, idct_seq, cd_val, min_val, max_val, del_yn

-- 지역 코드 (COA0083)
INSERT INTO TA_COA311M_CDBSC VALUES
 ('U001','COA0083','COA0000','20000101','99991231','11','서울','00',  '서울특별시',1, NULL,NULL,NULL,'N'),
 ('U002','COA0083','COA0000','20000101','99991231','41','경기','00',  '경기도',    2, NULL,NULL,NULL,'N'),
 ('U003','COA0083','COA0000','20000101','99991231','28','인천','00',  '인천광역시',3, NULL,NULL,NULL,'N'),
 ('U004','COA0083','COA0000','20000101','99991231','26','부산','00',  '부산광역시',4, NULL,NULL,NULL,'N'),
 ('U005','COA0083','COA0000','20000101','99991231','27','대구','00',  '대구광역시',5, NULL,NULL,NULL,'N'),
 ('U006','COA0083','COA0000','20000101','99991231','29','광주','00',  '광주광역시',6, NULL,NULL,NULL,'N'),
 ('U007','COA0083','COA0000','20000101','99991231','30','대전','00',  '대전광역시',7, NULL,NULL,NULL,'N'),
 ('U008','COA0083','COA0000','20000101','99991231','51','강원','00',  '강원도',    8, NULL,NULL,NULL,'N'),
-- 지급방식 코드 (COA0021)
 ('U101','COA0021','COA0000','20000101','99991231','07','전세보증','00','전세자금보증',1,NULL,NULL,NULL,'N'),
 ('U102','COA0021','COA0000','20000101','99991231','08','구입자금보증','00','구입자금보증',2,NULL,NULL,NULL,'N'),
 ('U103','COA0021','COA0000','20000101','99991231','09','중도금보증','00','중도금보증',3,NULL,NULL,NULL,'N'),
-- 삭제된 코드 예시 (del_yn 교육용)
 ('U201','COA0083','COA0000','20000101','20201231','99','구분없음','00','미사용코드',99,NULL,NULL,NULL,'Y');

-- ── 시세·보증 데이터 ──────────────────────────────────────────
INSERT INTO TB_RGR011M_HSPRC VALUES
 ('202006','G-2020-001','11','07',12400,1850.5,4.2, 4.1, 4.0,'1'),
 ('202006','G-2020-002','11','08', 8200,3120.0,3.1, 3.0, 2.9,'0'),
 ('202006','G-2020-003','11','09', 5100,2340.0,2.4, 2.3, 2.2,'1'),
 ('202006','G-2020-004','41','07',18700,2100.0,3.8, 3.7, 3.6,'0'),
 ('202006','G-2020-005','41','08',11500,4200.0,4.2, 4.1, 4.0,'1'),
 ('202006','G-2020-006','28','07', 5800, 980.0,2.1, 2.0, 1.9,'0'),
 ('202006','G-2020-007','26','07', 4200, 540.0,1.9, 1.8, 1.7,'1'),
 ('202006','G-2020-008','26','08', 2900, 870.0,1.7, 1.6,NULL,'0'),
 ('202106','G-2021-001','11','07',13200,2050.0,4.5, 4.4, 4.3,'1'),
 ('202106','G-2021-002','11','08', 9100,3560.0,3.4, 3.3, 3.2,'0'),
 ('202106','G-2021-003','11','07',22000,4100.0,4.8, 4.7, 4.6,'1'),
 ('202106','G-2021-004','41','07',20100,2450.0,4.1, 4.0, 3.9,'1'),
 ('202106','G-2021-005','41','08',13200,5100.0,5.1, 5.0, 4.9,'0'),
 ('202106','G-2021-006','28','07', 6400,1100.0,2.6, 2.5, 2.4,'1'),
 ('202106','G-2021-007','27','07', 3100, 420.0,2.0, 1.9,NULL,'0'),
 ('202106','G-2021-008','29','08', 1800, 510.0,1.8, 1.7, 1.6,'1'),
 ('202206','G-2022-001','11','07',11800,1920.0,4.8, 4.7, 4.6,'1'),
 ('202206','G-2022-002','11','08', 8600,3300.0,3.3, 3.2, 3.1,'0'),
 ('202206','G-2022-003','11','07',25000,4800.0,5.2, 5.1, 5.0,'1'),
 ('202206','G-2022-004','41','07',19200,2280.0,4.3, 4.2, 4.1,'0'),
 ('202206','G-2022-005','41','09', 8400,3100.0,3.1, 3.0, 2.9,'1'),
 ('202206','G-2022-006','26','07', 4500, 580.0,1.9, 1.8,NULL,'0'),
 ('202206','G-2022-007','30','07', 2100, 310.0,1.6, 1.5, 1.4,'1'),
 ('202206','G-2022-008','51','08',  980, 220.0,1.5, 1.4, 1.3,'0'),
 ('202306','G-2023-001','11','07',10500,1750.0,5.1, 5.0, 4.9,'1'),
 ('202306','G-2023-002','11','08', 7800,3050.0,3.9, 3.8, 3.7,'0'),
 ('202306','G-2023-003','11','07',27500,5200.0,5.8, 5.7, 5.6,'1'),
 ('202306','G-2023-004','41','07',17900,2100.0,4.5, 4.4, 4.3,'0'),
 ('202306','G-2023-005','41','08',12100,4800.0,4.8, 4.7, 4.6,'1'),
 ('202306','G-2023-006','28','07', 5200, 920.0,3.0, 2.9, 2.8,'0'),
 ('202306','G-2023-007','27','08', 2400, 680.0,2.1, 2.0, 1.9,'1'),
 ('202306','G-2023-008','29','07', 1900, 280.0,1.9, 1.8,NULL,'0');
""")
    c.commit()
    return c


def run_sql(sql: str):
    eng = _engine()
    if eng:
        try:
            with eng.connect() as conn:
                return pd.read_sql(_satext(sql), conn), None
        except Exception as e:
            return None, str(e)
    demo = _demo()
    try:
        return pd.read_sql_query(sql, demo), None
    except Exception as e:
        return None, str(e)


def _result(sql, key):
    if st.button("▶ 실행", key=key):
        df, err = run_sql(sql)
        if err:
            st.error(f"오류: {err}")
        else:
            st.success(f"✅ {len(df)}행")
            st.dataframe(df, use_container_width=True, hide_index=True)


def _practice(q, ans, k, hint=None):
    st.markdown(f'''<div class="qbox">🎯 <b>실습 문제</b><br>{q}</div>''', unsafe_allow_html=True)
    if hint:
        with st.expander("💡 힌트"):
            st.info(hint)
    usr = st.text_area("SQL 작성", height=110, key=f"{k}_in",
                       placeholder="여기에 SQL을 작성하세요...")
    c1, c2 = st.columns(2)
    with c1:
        if st.button("▶ 내 답안 실행", key=f"{k}_run", type="primary"):
            if usr.strip():
                df, err = run_sql(usr)
                st.error(f"오류: {err}") if err else (
                    st.success(f"✅ {len(df)}행") or
                    st.dataframe(df, use_container_width=True, hide_index=True))
            else:
                st.warning("SQL을 입력해주세요.")
    with c2:
        if st.button("📋 정답 보기", key=f"{k}_ans"):
            st.markdown('''<div class="okbox">✅ 정답</div>''', unsafe_allow_html=True)
            st.code(ans, language="sql")
            df, err = run_sql(ans)
            if not err:
                st.caption(f"결과 {len(df)}행")
                st.dataframe(df, use_container_width=True, hide_index=True)


# ════════════════════════════════════════════════════════════════
# 탭 정의  —  아이콘 + 레이블  
# ════════════════════════════════════════════════════════════════
TABS = [
    ("🏠", "개요"),
    ("🔌", "환경"),
    ("①",  "SELECT"),
    ("②",  "WHERE"),
    ("③",  "LIKE"),
    ("④",  "ORDER BY"),
    ("⑤",  "GROUP BY"),
    ("⑥",  "JOIN"),
    ("⑦",  "ANSI 함수"),
    ("🚀", "실전"),
    ("✏️", "연습문제"),
]
N = len(TABS)

GREEN  = "#10B981"   # 에메랄드 그린 (라이트/다크 모두 선명)
ORANGE = "#F59E0B"   # 앰버 오렌지
TEAL   = "#0D9488"   # 딥 틸 (서브 액센트)

# ════════════════════════════════════════════════════════════════
# 페이지 설정 & 전역 CSS
# ════════════════════════════════════════════════════════════════
st.set_page_config(page_title="HF SQL 교육", page_icon="🗄️",
                   layout="wide", initial_sidebar_state="collapsed")

st.markdown(f"""
<style>
/* ── CSS 변수 (헤더 내 색상용만 유지) ─────────────── */
:root {{
  --c-green:  {GREEN};
  --c-orange: {ORANGE};
  --c-teal:   {TEAL};
}}

/* ── 레이아웃 ─────────────────────────────────────── */
[data-testid="stSidebar"],[data-testid="collapsedControl"]{{display:none!important}}

/* Streamlit 상단 툴바·데코레이션 숨김 → 헤더 가림 방지 */
[data-testid="stToolbar"]{{display:none!important}}
[data-testid="stDecoration"]{{display:none!important}}
header[data-testid="stHeader"]{{display:none!important}}

/* block-container 상단 여백 제거 */
.block-container{{
  padding: 0 1.2rem 2rem !important;
  padding-top: 0 !important;
  margin-top: 0 !important;
  max-width: 100% !important;
}}
/* Streamlit이 헤더 높이만큼 자동으로 추가하는 여백 제거 */
.stApp > div:first-child {{
  padding-top: 0 !important;
}}

/* ── 헤더 바 ──────────────────────────────────────── */
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
.badge-db  {{ background: rgba(255,255,255,.15); color: #D1FAE5; border: 1px solid rgba(255,255,255,.25); }}
.badge-sql {{ background: {ORANGE}; color: #1C1917; border: none; }}

/* ── 탭 네비 컨테이너 구분선 ─────────────────────── */
div[data-testid="stHorizontalBlock"] {{
  border-bottom: 2px solid {GREEN};
  padding-bottom: 2px;
  margin-bottom: 1rem;
}}

/* ── primary 버튼(활성탭) ──────────────────────────── */
[data-testid="stBaseButton-primary"] {{
  background: linear-gradient(135deg, #047857, #065F46) !important;
  border-color: #065F46 !important;
  font-weight: 700 !important;
}}
[data-testid="stBaseButton-primary"]:hover {{
  background: linear-gradient(135deg, #065F46, #064E3B) !important;
  border-color: #064E3B !important;
}}

/* ── 콘텐츠 박스 (배경 투명, 텍스트 inherit → 라이트·다크 모두 OK) */
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

/* ── DB 연결 상태 카드 ───────────────────────────── */
.conn-card {{
  border-radius: 12px;
  padding: 1.1rem 1.3rem;
  margin: .5rem 0;
  border: 1.5px solid rgba(128,128,128,.25);
  background: transparent;
  color: inherit;
}}
.conn-ok  {{ border-color: {GREEN}; background: rgba(16,185,129,.08); }}
.conn-off {{ border-color: rgba(128,128,128,.3); }}
</style>
""", unsafe_allow_html=True)

# ── 탭 인덱스  (query_params 기반, 새로고침 없이 전환) ───────
if "t" not in st.session_state:
    try:
        st.session_state.t = int(st.query_params.get("t", 0))
    except Exception:
        st.session_state.t = 0
idx = max(0, min(st.session_state.t, N - 1))

# ── 헤더 ──────────────────────────────────────────────────────
eng    = _engine()
dbtag  = "🟢 내부DB 연결됨" if eng else "⚪ 데모DB"
st.markdown(f"""
<div class="hdr-wrap">
  <div class="hdr-text">
    <p class="hdr-title">🗄️ HF SQL 교육자료</p>
    <p class="hdr-sub">주택금융 실무 테이블 (COA311M · RGR011M)로 배우는 ANSI SQL</p>
  </div>
  <div class="hdr-badges">
    <span class="badge badge-db">{dbtag}</span>
    <span class="badge badge-sql">SQL 실습</span>
  </div>
</div>
""", unsafe_allow_html=True)

# ── 탭 바 (Streamlit 버튼) ────────────────────────────────────
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


# ════════════════════════════════════════════════════════════════
# 0. 개요
# ════════════════════════════════════════════════════════════════
if idx == 0:
    st.markdown('<div class="sec">🏠 SQL 개요 & 기본 문법</div>', unsafe_allow_html=True)
    st.markdown("""
**SQL(Structured Query Language)** 은 관계형 DB에서 데이터를 **조회·삽입·수정·삭제**하는 ANSI/ISO 표준 언어입니다.
"어떻게(HOW)"가 아닌 **"무엇을(WHAT)"** 원하는지 기술하면 DB 엔진이 최적 방법으로 실행합니다.
    """)

    c1, c2, c3 = st.columns(3)
    for col, border_c, title_c, sub, body in [
        (c1, GREEN,   "📥 DML","데이터 조작",
         "• <b>SELECT</b> — 조회<br>• <b>INSERT</b> — 삽입<br>• <b>UPDATE</b> — 수정<br>• <b>DELETE</b> — 삭제<br><br><small>⭐ 이 교육에서 집중적으로 다룹니다</small>"),
        (c2, ORANGE,  "🏗️ DDL","데이터 정의",
         "• <b>CREATE</b> — 테이블 생성<br>• <b>ALTER</b> — 구조 변경<br>• <b>DROP</b> — 테이블 삭제<br>• <b>TRUNCATE</b> — 전체 삭제"),
        (c3, TEAL,    "🔐 DCL","데이터 제어",
         "• <b>GRANT</b> — 권한 부여<br>• <b>REVOKE</b> — 권한 회수<br>• <b>COMMIT</b> — 확정<br>• <b>ROLLBACK</b> — 취소"),
    ]:
        col.markdown(
            f'<div class="cls" style="border-top:4px solid {border_c}"><b>{title_c}</b> '
            f'<small>— {sub}</small><br><br>{body}</div>',
            unsafe_allow_html=True)

    st.divider()
    st.markdown("### SELECT 전체 문법 & 실행 순서")
    st.code("""-- [ ] = 선택 요소
SELECT   [DISTINCT] 컬럼, 집계함수(컬럼)   -- ⑥ SELECT
FROM     테이블  [AS 별칭]                 -- ① FROM (가장 먼저)
[JOIN    테이블2  ON 조인조건]             -- ②
[WHERE   행 필터 조건]                     -- ③ 집계 전 필터
[GROUP BY 그룹 컬럼]                       -- ④
[HAVING  집계 결과 필터]                   -- ⑤ 집계 후 필터
[ORDER BY 컬럼 [ASC|DESC]]               -- ⑦
[LIMIT   n];                              -- ⑧""", language="sql")

    st.markdown('''<div class="wrn">
⚠️ <b>실행 순서 ≠ 작성 순서</b><br>
FROM → JOIN → WHERE → GROUP BY → HAVING → SELECT → ORDER BY<br><br>
• WHERE에서 집계함수(SUM, AVG…) 사용 불가 → HAVING 사용<br>
• WHERE에서 SELECT 별칭 사용 불가 (아직 SELECT가 실행되지 않았음)<br>
• ORDER BY에서 SELECT 별칭 사용 가능 (SELECT 이후 실행)
</div>''', unsafe_allow_html=True)

    st.divider()
    t1, t2, t3 = st.tabs(["📐 데이터 타입 & 개념", "⚙️ 비교·논리 연산자", "🔢 집계 함수"])
    with t1:
        c1, c2 = st.columns(2)
        with c1:
            st.markdown("""
| 타입 | 설명 | RGR011M 예시 |
|------|------|-------------|
| `INTEGER` | 정수 | grnt_sply_cnt |
| `REAL / FLOAT` | 실수 | grnt_amt, kab_trd_avg_prc |
| `TEXT / VARCHAR` | 문자열 | grnt_no, basis_ym |
| `NULL` | 값 없음 (0·빈문자와 다름) | kab_trd_avg_prc 일부 |
            """)
        with c2:
            st.markdown("""
| 개념 | 설명 |
|------|------|
| `PRIMARY KEY` | 행을 유일하게 식별 |
| `FOREIGN KEY` | 다른 테이블 PK 참조 |
| `AS (Alias)` | 컬럼·테이블 별칭 |
| `DISTINCT` | 중복 행 제거 |
            """)
    with t2:
        st.markdown("""
| 연산자 | 의미 | 예시 |
|--------|------|------|
| `=` | 같다 | `basis_ym = '202306'` |
| `<>` / `!=` | 다르다 | `pnsn_gv_meth_dvcd <> '09'` |
| `>` / `<` / `>=` / `<=` | 비교 | `grnt_sply_cnt >= 10000` |
| `BETWEEN a AND b` | 범위 (양 끝 포함) | `grnt_amt BETWEEN 1000 AND 5000` |
| `IN (...)` | 목록 중 하나 | `pnsn_gv_meth_dvcd IN ('07','08')` |
| `IS NULL` / `IS NOT NULL` | NULL 체크 | `kab_trd_avg_prc IS NULL` |
| `AND` / `OR` / `NOT` | 논리 연산 | `... AND ... OR ...` |
| `LIKE` | 패턴 | `grnt_no LIKE 'G-2023%'` |
        """)
        st.markdown('''<div class="tip">💡 우선순위: NOT > AND > OR — 괄호로 명시적으로 묶는 것을 권장합니다.</div>''', unsafe_allow_html=True)
    with t3:
        st.markdown("""
| 함수 | 설명 | NULL 처리 |
|------|------|-----------|
| `COUNT(*)` | 전체 행 수 | NULL 포함 |
| `COUNT(col)` | 컬럼 행 수 | NULL 제외 |
| `SUM(col)` | 합계 | NULL 무시 |
| `AVG(col)` | 평균 | NULL 무시 |
| `MAX(col)` / `MIN(col)` | 최대·최소 | NULL 무시 |
| `ROUND(v, n)` | 소수점 n자리 반올림 | — |
| `COALESCE(a,b,…)` | 첫 번째 non-NULL 값 | NULL 대체 |
| `NULLIF(a,b)` | a=b이면 NULL 반환 | 0 나누기 방지 |
        """)

    nav_footer(0)


# ════════════════════════════════════════════════════════════════
# 1. 환경
# ════════════════════════════════════════════════════════════════
elif idx == 1:
    st.markdown('<div class="sec">🔌 실습 환경 & 데이터 구조</div>', unsafe_allow_html=True)

    # ── 섹션 A: 내부망 연결 설정 ─────────────────────────────
    st.markdown("### ① 내부망 SQLAlchemy 연결 설정")
    st.markdown("""
아래 설정을 채우고 **연결 테스트** 버튼을 누르면 실제 내부망 DB로 연결됩니다.
연결 성공 시 이 앱 전체의 SQL 실행이 **실제 데이터**로 동작합니다.
""")

    c_conn, c_guide = st.columns([3, 2], gap="large")

    with c_conn:
        st.markdown("**연결 정보 입력**")

        db_type = st.selectbox(
            "DB 종류",
            ["Trino / Iceberg (사내)", "MS-SQL (pyodbc)", "PostgreSQL", "Oracle", "직접 입력"],
            key="db_type_sel"
        )

        if db_type == "Trino / Iceberg (사내)":
            col1, col2 = st.columns(2)
            with col1:
                t_user = st.text_input("사용자명", placeholder="uid123", key="t_user")
                t_host = st.text_input("호스트", placeholder="trino.internal.hf.co.kr", key="t_host")
            with col2:
                t_port = st.text_input("포트", value="8443", key="t_port")
                t_schema = st.text_input("카탈로그/스키마", value="iceberg/bronze", key="t_schema")
            t_https = st.checkbox("HTTPS 사용", value=True, key="t_https")
            t_kerb  = st.checkbox("Kerberos 인증", value=False, key="t_kerb")

            scheme_part = "https" if t_https else "http"
            conn_str_preview = f"trino://{t_user or 'USER'}@{t_host or 'HOST'}:{t_port}/{t_schema}"
            conn_args_preview = f'{{"http_scheme": "{scheme_part}"'
            if t_kerb:
                conn_args_preview += ', "auth": trino.auth.KerberosAuthentication()'
            conn_args_preview += "}"

        elif db_type == "MS-SQL (pyodbc)":
            col1, col2 = st.columns(2)
            with col1:
                ms_server = st.text_input("서버명", placeholder="SQLSRV01", key="ms_server")
                ms_db     = st.text_input("데이터베이스명", placeholder="HF_DW", key="ms_db")
            with col2:
                ms_driver = st.selectbox("ODBC 드라이버", [
                    "ODBC Driver 17 for SQL Server",
                    "ODBC Driver 18 for SQL Server",
                    "SQL Server"], key="ms_drv")
                ms_trusted = st.checkbox("Windows 통합 인증", value=True, key="ms_trusted")

            drv_enc = ms_driver.replace(" ", "+")
            auth_part = "trusted_connection=yes" if ms_trusted else "UID=USER;PWD=PASS"
            conn_str_preview = (
                f"mssql+pyodbc://{ms_server or 'SERVER'}/{ms_db or 'DB'}"
                f"?driver={drv_enc}&{auth_part}"
            )
            conn_args_preview = ""

        elif db_type == "PostgreSQL":
            col1, col2 = st.columns(2)
            with col1:
                pg_user = st.text_input("사용자명", key="pg_user")
                pg_pw   = st.text_input("비밀번호", type="password", key="pg_pw")
            with col2:
                pg_host = st.text_input("호스트", placeholder="db.internal", key="pg_host")
                pg_db   = st.text_input("DB명", key="pg_db")
            conn_str_preview = f"postgresql+psycopg2://{pg_user or 'USER'}:***@{pg_host or 'HOST'}/{pg_db or 'DB'}"
            conn_args_preview = ""

        elif db_type == "Oracle":
            col1, col2 = st.columns(2)
            with col1:
                ora_user = st.text_input("사용자명", key="ora_user")
                ora_pw   = st.text_input("비밀번호", type="password", key="ora_pw")
            with col2:
                ora_dsn  = st.text_input("DSN / SID", placeholder="ORCL", key="ora_dsn")
            conn_str_preview = f"oracle+cx_oracle://{ora_user or 'USER'}:***@{ora_dsn or 'DSN'}"
            conn_args_preview = ""

        else:  # 직접 입력
            conn_str_preview = st.text_input(
                "Connection String (SQLAlchemy URL)",
                placeholder="dialect+driver://user:pass@host:port/dbname",
                key="direct_conn"
            )
            conn_args_preview = ""

        # 생성된 Connection String 미리보기
        st.markdown("**생성된 Connection String**")
        st.code(conn_str_preview, language="text")

        # 연결 테스트 버튼
        col_btn1, col_btn2 = st.columns([2, 3])
        with col_btn1:
            do_test = st.button("🔌 연결 테스트", type="primary", use_container_width=True)
        with col_btn2:
            do_copy_code = st.button("📋 적용 코드 복사용 출력", use_container_width=True)

        if do_test:
            if not _SA:
                st.markdown('<div class="err">❌ SQLAlchemy가 설치되지 않았습니다.<br><code>pip install sqlalchemy</code> 를 실행하세요.</div>', unsafe_allow_html=True)
            elif not conn_str_preview or conn_str_preview.startswith("dialect"):
                st.markdown('<div class="wrn">⚠️ Connection String을 먼저 입력해주세요.</div>', unsafe_allow_html=True)
            else:
                with st.spinner("내부망 연결 시도 중..."):
                    try:
                        from sqlalchemy import create_engine, text as _t
                        test_eng = create_engine(conn_str_preview, pool_pre_ping=True,
                                                  connect_args={"connect_timeout": 8})
                        with test_eng.connect() as _c:
                            _c.execute(_t("SELECT 1"))
                        st.markdown('<div class="okb">✅ <b>연결 성공!</b> 내부망 DB에 정상 접속되었습니다.<br>파일 상단 <code>USE_INTERNAL_DB = True</code> 와 <code>CONNECTION_STRING</code>을 설정하면 전체 앱에 적용됩니다.</div>', unsafe_allow_html=True)
                    except Exception as e:
                        st.markdown(f'<div class="err">❌ <b>연결 실패</b><br><code>{e}</code></div>', unsafe_allow_html=True)

        if do_copy_code:
            code_out = f'''# ── 파일 상단 설정값으로 붙여넣기 ──────────────────
USE_INTERNAL_DB   = True
CONNECTION_STRING = "{conn_str_preview}"
'''
            st.code(code_out, language="python")
            st.markdown('<div class="tip">💡 위 코드를 <code>sql_hf_education_v7.py</code> 상단의 설정 영역에 붙여넣으면 앱 재시작 후 내부망 DB로 동작합니다.</div>', unsafe_allow_html=True)

    with c_guide:
        st.markdown("**설치 패키지 가이드**")
        st.code("""# 공통
pip install sqlalchemy pandas streamlit

# Trino (사내 Iceberg)
pip install trino

# MS-SQL
pip install pyodbc

# PostgreSQL
pip install psycopg2-binary

# Oracle
pip install cx_oracle""", language="bash")

        st.markdown('<div class="tip">💡 내부망 방화벽 환경에서는 IT팀에 <b>아웃바운드 포트 허용</b>을 요청하세요.<br>(Trino: 8443, MSSQL: 1433, PG: 5432)</div>', unsafe_allow_html=True)

        st.markdown("**내부망 적용 코드 예시**")
        st.code("""# sql_hf_education_v7.py 상단 수정
USE_INTERNAL_DB   = True   # ← True로 변경
CONNECTION_STRING = (
  "trino://uid@host:8443/iceberg/bronze"
)

# 실제 테이블명 (스키마 포함 시)
TBL_COA = "iceberg.bronze.TA_COA311M_CDBSC"
TBL_RGR = "iceberg.bronze.TB_RGR011M_HSPRC"
""", language="python")

    st.divider()

    # ── 섹션 B: 현재 연결 상태 확인 ──────────────────────────
    st.markdown("### ② 현재 연결 상태")
    col_s1, col_s2 = st.columns(2)
    with col_s1:
        if eng:
            st.markdown('<div class="conn-card conn-ok">🟢 <b>내부망 DB 연결 중</b><br><small>실제 데이터로 실습합니다.</small></div>', unsafe_allow_html=True)
        else:
            st.markdown('<div class="conn-card conn-off">⚪ <b>데모 DB (SQLite InMemory)</b><br><small>파일 상단 USE_INTERNAL_DB = True 설정 후 재시작하면 내부망으로 전환됩니다.</small></div>', unsafe_allow_html=True)
    with col_s2:
        sa_ok = "✅ 설치됨" if _SA else "❌ 미설치 (pip install sqlalchemy)"
        st.markdown(f'<div class="conn-card conn-off">📦 <b>SQLAlchemy 상태</b><br><small>{sa_ok}</small></div>', unsafe_allow_html=True)

    st.divider()

    # ── 섹션 C: 테이블 구조 ───────────────────────────────────
    st.markdown("### ③ 테이블 구조 & 관계")
    st.code("""
TA_COA311M_CDBSC  (공통코드 마스터)
┌─────────────────────────────────────────────────────────────┐
│ cd_grp_id    TEXT   코드그룹ID  (COA0083=지역 / COA0021=지급방식)│
│ cd_id        TEXT   코드ID  ← JOIN 키                        │
│ cd_nm        TEXT   코드명  ('서울', '전세보증' …)            │
│ valid_strt_dy / valid_end_dy  유효기간 (YYYYMMDD)            │
│ del_yn       TEXT   삭제여부 (Y=삭제, N=유효)  ← 항상 필터   │
│ cd_expn_cont TEXT   코드 설명                                │
│ idct_seq     INT    정렬순서                                  │
└──────────────────────┬──────────────────────────────────────┘
                       │ cd_id
           ┌───────────┴──────────────────────┐
           │ ON hs_loc_zone_dvcd = cd_id       │ ON pnsn_gv_meth_dvcd = cd_id
           │ AND cd_grp_id = 'COA0083'         │ AND cd_grp_id = 'COA0021'
           │ AND del_yn = 'N'                  │ AND del_yn = 'N'
           ▼                                   ▼
TB_RGR011M_HSPRC  (시세·보증 현황)
┌─────────────────────────────────────────────────────────────┐
│ basis_ym           TEXT     기준연월 (YYYYMM)                │
│ grnt_no            TEXT     보증번호                         │
│ hs_loc_zone_dvcd   TEXT     주택소재지역코드 → COA311M       │
│ pnsn_gv_meth_dvcd  TEXT     지급방식코드    → COA311M       │
│ grnt_sply_cnt      INTEGER  공급건수                         │
│ grnt_amt           REAL     보증금액 (억원)                  │
│ aprs_eval_amt      REAL     감정평가금액 (억원)              │
│ kab_trd_avg_prc    REAL     KB거래평균가격 (억원, NULL 가능) │
│ kb_trd_avg_prc     REAL     KB시세평균가격 (억원, NULL 가능) │
│ kab_sise_rsch_dy   TEXT     시세구분 (0=국민은행/기타=부동산원)│
└─────────────────────────────────────────────────────────────┘
""", language="text")

    st.divider()

    # ── 섹션 D: 샘플 데이터 확인 ─────────────────────────────
    st.markdown("### ④ 샘플 데이터 확인")
    t1, t2 = st.tabs(["📋 TA_COA311M_CDBSC", "📋 TB_RGR011M_HSPRC"])
    with t1:
        df, _ = run_sql("SELECT cd_grp_id, cd_id, cd_nm, valid_strt_dy, valid_end_dy, del_yn, cd_expn_cont, idct_seq FROM TA_COA311M_CDBSC ORDER BY cd_grp_id, idct_seq")
        st.dataframe(df, use_container_width=True, hide_index=True)
        st.caption("cd_grp_id: COA0083=지역코드, COA0021=지급방식코드 | del_yn=Y 이면 삭제된 코드")
    with t2:
        df, _ = run_sql("SELECT * FROM TB_RGR011M_HSPRC ORDER BY basis_ym, grnt_no LIMIT 15")
        st.dataframe(df, use_container_width=True, hide_index=True)
        st.caption("basis_ym: 202006~202306 | kab_trd_avg_prc 일부 NULL | kab_sise_rsch_dy: 0=국민은행")

    nav_footer(1)


# ════════════════════════════════════════════════════════════════
# 2. SELECT
# ════════════════════════════════════════════════════════════════
elif idx == 2:
    st.markdown('<div class="sec">① SELECT — 데이터 조회</div>', unsafe_allow_html=True)
    st.markdown("`SELECT`는 테이블에서 원하는 데이터를 읽어오는 핵심 명령입니다.")
    st.markdown('''<div class="syn">SELECT [DISTINCT] 컬럼1, 컬럼2, … ← 조회할 컬럼 (*=전체)<br>FROM   테이블명;</div>''', unsafe_allow_html=True)

    exs = [
        ("예제 1 — SELECT * (전체 컬럼)",
         "테이블의 모든 컬럼을 가져옵니다. 구조 파악용으로 사용하세요.",
         "SELECT *\nFROM   TB_RGR011M_HSPRC\nLIMIT  5;",
         "wrn","⚠️ 실무에서는 <code>SELECT *</code> 대신 필요한 컬럼만 명시하세요. 성능·가독성 모두 향상됩니다."),
        ("예제 2 — 특정 컬럼만 조회",
         "보증번호·기준연월·지역코드·공급건수만 선택합니다.",
         "SELECT grnt_no, basis_ym, hs_loc_zone_dvcd, grnt_sply_cnt\nFROM   TB_RGR011M_HSPRC;",
         None, None),
        ("예제 3 — AS 별칭 + 산술 연산",
         "한글 별칭을 붙이고 억원 → 만원 환산을 계산합니다.",
         """SELECT
    grnt_no                         AS 보증번호,
    basis_ym                        AS 기준연월,
    grnt_sply_cnt                   AS 공급건수,
    grnt_amt                        AS "보증금액(억)",
    ROUND(grnt_amt * 10000, 0)      AS "보증금액(만원)"
FROM TB_RGR011M_HSPRC;""",
         "tip","💡 AS 별칭은 ORDER BY에서 사용 가능, WHERE에서는 불가합니다."),
        ("예제 4 — DISTINCT 중복 제거",
         "어떤 지급방식코드가 존재하는지 고유값만 조회합니다.",
         "SELECT DISTINCT pnsn_gv_meth_dvcd AS 지급방식코드\nFROM   TB_RGR011M_HSPRC;",
         None, None),
        ("예제 5 — COA311M 코드 조회",
         "cd_grp_id별로 어떤 코드가 있는지 확인합니다. del_yn=N 인 유효 코드만 조회합니다.",
         """SELECT cd_grp_id, cd_id, cd_nm, cd_expn_cont, idct_seq
FROM   TA_COA311M_CDBSC
WHERE  del_yn = 'N'
ORDER BY cd_grp_id, idct_seq;""",
         "tip","💡 COA311M 조회 시 del_yn = 'N' 조건을 항상 추가해 삭제된 코드를 제외하세요."),
    ]
    for title, desc, sql, btype, bmsg in exs:
        with st.expander(title, expanded=True):
            st.caption(desc)
            if btype == "wrn": st.markdown(f'<div class="wrn">{bmsg}</div>', unsafe_allow_html=True)
            elif btype == "tip": st.markdown(f'<div class="tip">{bmsg}</div>', unsafe_allow_html=True)
            st.code(sql, language="sql")
            _result(sql, f"s2_{title}")

    st.divider()
    _practice(
        "TB_RGR011M_HSPRC 에서 보증번호·기준연월·보증금액(억)과 보증금액을 만원 단위로 환산한 값(ROUND, 소수점 없음)을 함께 조회하세요.",
        """SELECT
    grnt_no                       AS 보증번호,
    basis_ym                      AS 기준연월,
    grnt_amt                      AS "보증금액(억)",
    ROUND(grnt_amt * 10000, 0)    AS "보증금액(만원)"
FROM TB_RGR011M_HSPRC;""",
        "p2", hint="ROUND(값 * 10000, 0)으로 억→만원 환산하세요.")
    nav_footer(2)


# ════════════════════════════════════════════════════════════════
# 3. WHERE
# ════════════════════════════════════════════════════════════════
elif idx == 3:
    st.markdown('<div class="sec">② WHERE — 조건 필터</div>', unsafe_allow_html=True)
    st.markdown("`WHERE`는 FROM으로 불러온 데이터 중 조건을 만족하는 **행(Row)만** 남기는 필터입니다. 집계함수는 WHERE에서 사용 불가 → HAVING 사용.")
    st.markdown('''<div class="syn">SELECT 컬럼 FROM 테이블<br>WHERE  조건식;  ← TRUE인 행만 반환</div>''', unsafe_allow_html=True)

    exs = [
        ("예제 1 — 특정 기준연월",
         "202306 기준 데이터만 조회합니다.",
         "SELECT grnt_no, basis_ym, pnsn_gv_meth_dvcd, grnt_sply_cnt\nFROM   TB_RGR011M_HSPRC\nWHERE  basis_ym = '202306';", None, None),
        ("예제 2 — 비교 연산자 (공급건수 ≥ 10,000)",
         "대규모 보증 건만 조회합니다.",
         "SELECT grnt_no, basis_ym, grnt_sply_cnt, grnt_amt\nFROM   TB_RGR011M_HSPRC\nWHERE  grnt_sply_cnt >= 10000;", None, None),
        ("예제 3 — AND 복합 조건",
         "202306 기준이면서 공급건수 5,000건 이상인 건.",
         "SELECT grnt_no, basis_ym, pnsn_gv_meth_dvcd, grnt_sply_cnt\nFROM   TB_RGR011M_HSPRC\nWHERE  basis_ym = '202306'\n  AND  grnt_sply_cnt >= 5000;", None, None),
        ("예제 4 — IN 목록 조건",
         "지급방식코드가 07(전세) 또는 08(구입)인 건만 조회합니다.",
         "SELECT grnt_no, pnsn_gv_meth_dvcd, grnt_sply_cnt\nFROM   TB_RGR011M_HSPRC\nWHERE  pnsn_gv_meth_dvcd IN ('07', '08');",
         "tip","💡 IN 대신 서브쿼리: <code>WHERE hs_loc_zone_dvcd IN (SELECT cd_id FROM TA_COA311M_CDBSC WHERE cd_grp_id='COA0083' AND cd_nm='서울')</code>"),
        ("예제 5 — BETWEEN 기간 조건",
         "202101~202212 기간 데이터를 조회합니다.",
         "SELECT grnt_no, basis_ym, grnt_sply_cnt\nFROM   TB_RGR011M_HSPRC\nWHERE  basis_ym BETWEEN '202101' AND '202212';",
         "tip","💡 YYYYMM 형식은 TEXT이지만 사전순 비교로 BETWEEN이 올바르게 동작합니다."),
        ("예제 6 — IS NULL / IS NOT NULL",
         "KB거래평균가격이 NULL인 데이터를 찾습니다.",
         "SELECT grnt_no, basis_ym, kab_trd_avg_prc, kb_trd_avg_prc\nFROM   TB_RGR011M_HSPRC\nWHERE  kab_trd_avg_prc IS NULL;",
         "wrn","⚠️ <code>= NULL</code>은 항상 FALSE. 반드시 <code>IS NULL</code>을 사용하세요."),
        ("예제 7 — COA311M 유효 코드만 필터",
         "del_yn=N(유효)이고 COA0083 그룹(지역)의 코드만 조회합니다.",
         "SELECT cd_id, cd_nm, cd_expn_cont\nFROM   TA_COA311M_CDBSC\nWHERE  cd_grp_id = 'COA0083'\n  AND  del_yn = 'N'\nORDER BY idct_seq;", None, None),
    ]
    for title, desc, sql, btype, bmsg in exs:
        with st.expander(title, expanded=True):
            st.caption(desc)
            if btype == "wrn": st.markdown(f'<div class="wrn">{bmsg}</div>', unsafe_allow_html=True)
            elif btype == "tip": st.markdown(f'<div class="tip">{bmsg}</div>', unsafe_allow_html=True)
            st.code(sql, language="sql")
            _result(sql, f"s3_{title}")

    st.divider()
    _practice(
        "TB_RGR011M_HSPRC 에서 basis_ym이 '202201'~'202312' 사이이고 지급방식코드가 '07'(전세)이며 공급건수 10,000건 이상인 건을 공급건수 내림차순으로 조회하세요.",
        """SELECT grnt_no, basis_ym, pnsn_gv_meth_dvcd, grnt_sply_cnt
FROM   TB_RGR011M_HSPRC
WHERE  basis_ym BETWEEN '202201' AND '202312'
  AND  pnsn_gv_meth_dvcd = '07'
  AND  grnt_sply_cnt >= 10000
ORDER BY grnt_sply_cnt DESC;""",
        "p3", hint="BETWEEN, =, >= 세 조건을 AND로 연결하고 ORDER BY를 추가하세요.")
    nav_footer(3)


# ════════════════════════════════════════════════════════════════
# 4. LIKE
# ════════════════════════════════════════════════════════════════
elif idx == 4:
    st.markdown('<div class="sec">③ LIKE — 패턴 검색</div>', unsafe_allow_html=True)
    st.markdown("`LIKE`는 문자열 컬럼에서 특정 패턴에 맞는 값을 검색합니다. 정확한 값을 모를 때 사용합니다.")
    c1, c2 = st.columns(2)
    with c1:
        st.markdown('''<div class="syn">WHERE 컬럼 LIKE '패턴'<br><br>%  : 0개 이상의 임의 문자<br>_  : 정확히 1개의 임의 문자</div>''', unsafe_allow_html=True)
    with c2:
        st.markdown("""
| 패턴 | 의미 | 예시 |
|------|------|------|
| `'G-2023%'` | G-2023으로 시작 | G-2023-001 ✅ |
| `'%-001'` | -001로 끝남 | G-2022-001 ✅ |
| `'%서%'` | 서 포함 | 서울 ✅ |
| `'G-____-%'` | G- + 4자 + - | 모든 보증 ✅ |
        """)

    exs = [
        ("예제 1 — 보증번호 연도 검색",
         "보증번호가 'G-2023'으로 시작하는 건만 조회합니다. 앞이 고정된 패턴은 인덱스 활용 가능합니다.",
         "SELECT grnt_no, basis_ym, pnsn_gv_meth_dvcd, grnt_sply_cnt\nFROM   TB_RGR011M_HSPRC\nWHERE  grnt_no LIKE 'G-2023%';",
         "tip","💡 <code>LIKE '값%'</code>(앞 고정) → 인덱스 사용 가능 / <code>LIKE '%값%'</code> → Full Scan"),
        ("예제 2 — 특정 번호로 끝나는 검색",
         "일련번호가 -001로 끝나는 건을 모든 기간에서 찾습니다.",
         "SELECT grnt_no, basis_ym, grnt_sply_cnt\nFROM   TB_RGR011M_HSPRC\nWHERE  grnt_no LIKE '%-001';", None, None),
        ("예제 3 — 지역명 포함 검색 (COA311M)",
         "지역명에 '서'가 포함된 코드를 찾습니다.",
         "SELECT cd_id, cd_nm, cd_expn_cont\nFROM   TA_COA311M_CDBSC\nWHERE  cd_grp_id = 'COA0083' AND cd_nm LIKE '%서%';",
         "wrn","⚠️ <code>LIKE '%값%'</code>는 Full Table Scan. 대용량 테이블에서는 주의하세요."),
        ("예제 4 — _ 와일드카드 글자 수 고정",
         "보증번호 형식 G-YYYYMM-NNN 패턴 확인.",
         "SELECT grnt_no FROM TB_RGR011M_HSPRC\nWHERE  grnt_no LIKE 'G-____-___';", None, None),
    ]
    for title, desc, sql, btype, bmsg in exs:
        with st.expander(title, expanded=True):
            st.caption(desc)
            if btype == "wrn": st.markdown(f'<div class="wrn">{bmsg}</div>', unsafe_allow_html=True)
            elif btype == "tip": st.markdown(f'<div class="tip">{bmsg}</div>', unsafe_allow_html=True)
            st.code(sql, language="sql")
            _result(sql, f"s4_{title}")

    st.divider()
    _practice(
        "TA_COA311M_CDBSC 에서 cd_grp_id가 'COA0083'이고 del_yn='N'인 코드 중 cd_expn_cont(코드설명)에 '광역'이 포함된 지역만 조회하세요.",
        """SELECT cd_id, cd_nm, cd_expn_cont
FROM   TA_COA311M_CDBSC
WHERE  cd_grp_id = 'COA0083'
  AND  del_yn = 'N'
  AND  cd_expn_cont LIKE '%광역%'
ORDER BY idct_seq;""",
        "p4", hint="cd_expn_cont LIKE '%광역%' 조건을 AND로 추가하세요.")
    nav_footer(4)


# ════════════════════════════════════════════════════════════════
# 5. ORDER BY
# ════════════════════════════════════════════════════════════════
elif idx == 5:
    st.markdown('<div class="sec">④ ORDER BY — 정렬</div>', unsafe_allow_html=True)
    st.markdown("`ORDER BY`는 결과를 원하는 기준으로 정렬합니다. SQL 실행 순서상 **가장 마지막**에 적용되므로 SELECT 별칭을 사용할 수 있습니다.")
    st.markdown('''<div class="syn">ORDER BY 컬럼1 [ASC|DESC], 컬럼2 [ASC|DESC]<br><br>ASC : 오름차순 (기본값, 생략 가능)<br>DESC: 내림차순</div>''', unsafe_allow_html=True)

    exs = [
        ("예제 1 — 공급건수 내림차순",
         "가장 많이 공급된 건부터 정렬합니다.",
         "SELECT grnt_no, basis_ym, grnt_sply_cnt\nFROM   TB_RGR011M_HSPRC\nORDER BY grnt_sply_cnt DESC;"),
        ("예제 2 — 다중 정렬 (기준연월↑ + 보증금액↓)",
         "기준연월 오름차순, 같은 연월 내에서는 보증금액이 큰 순.",
         "SELECT grnt_no, basis_ym, grnt_amt\nFROM   TB_RGR011M_HSPRC\nORDER BY basis_ym ASC, grnt_amt DESC;"),
        ("예제 3 — SELECT 별칭으로 정렬",
         "AS 별칭을 ORDER BY에서 사용합니다.",
         "SELECT basis_ym AS 기준연월, SUM(grnt_sply_cnt) AS 총공급건수\nFROM   TB_RGR011M_HSPRC\nGROUP BY basis_ym\nORDER BY 총공급건수 DESC;"),
        ("예제 4 — COA311M idct_seq 기준 정렬",
         "idct_seq(정렬순서) 컬럼으로 코드 목록을 순서대로 가져옵니다.",
         "SELECT cd_id, cd_nm, idct_seq\nFROM   TA_COA311M_CDBSC\nWHERE  cd_grp_id = 'COA0083' AND del_yn = 'N'\nORDER BY idct_seq ASC;"),
    ]
    for title, desc, sql in exs:
        with st.expander(title, expanded=True):
            st.caption(desc)
            st.code(sql, language="sql")
            _result(sql, f"s5_{title}")

    st.divider()
    _practice(
        "TB_RGR011M_HSPRC 에서 basis_ym이 '202306'인 건을 보증금액(grnt_amt) 내림차순, 같은 금액이면 공급건수 내림차순으로 정렬하여 조회하세요.",
        """SELECT grnt_no, basis_ym, grnt_amt, grnt_sply_cnt
FROM   TB_RGR011M_HSPRC
WHERE  basis_ym = '202306'
ORDER BY grnt_amt DESC, grnt_sply_cnt DESC;""",
        "p5", hint="ORDER BY에 두 컬럼을 콤마로 구분해 작성하세요.")
    nav_footer(5)


# ════════════════════════════════════════════════════════════════
# 6. GROUP BY
# ════════════════════════════════════════════════════════════════
elif idx == 6:
    st.markdown('<div class="sec">⑤ GROUP BY — 집계</div>', unsafe_allow_html=True)
    st.markdown("`GROUP BY`는 특정 컬럼 값이 같은 행들을 그룹으로 묶고 집계합니다. `HAVING`은 집계 결과를 필터링합니다.")
    st.markdown('''<div class="syn">SELECT   그룹컬럼, 집계함수(컬럼)<br>FROM     테이블<br>[WHERE   행 필터]   ← 집계 전<br>GROUP BY 그룹컬럼<br>[HAVING  집계 필터] ← 집계 후<br>[ORDER BY ...];</div>''', unsafe_allow_html=True)

    c1, c2 = st.columns(2)
    with c1:
        st.markdown("""
| 구분 | WHERE | HAVING |
|------|-------|--------|
| 적용 시점 | GROUP BY **이전** | GROUP BY **이후** |
| 필터 대상 | 개별 **행** | 집계 **그룹** |
| 집계함수 | ❌ 불가 | ✅ 가능 |
        """)
    with c2:
        st.markdown('''<div class="wrn">⚠️ <b>GROUP BY 규칙</b><br>SELECT의 비집계 컬럼은 반드시 GROUP BY에 포함해야 합니다.<br><br>✅ <code>SELECT basis_ym, pnsn_gv_meth_dvcd, COUNT(*)</code><br><code>GROUP BY basis_ym, pnsn_gv_meth_dvcd</code><br><br>❌ <code>GROUP BY basis_ym</code> ← pnsn_gv_meth_dvcd 누락!</div>''', unsafe_allow_html=True)

    exs = [
        ("예제 1 — 기준연월별 총 공급건수",
         "연월별로 공급건수·보증건수·감정가 합계를 구합니다.",
         """SELECT
    basis_ym                      AS 기준연월,
    COUNT(*)                      AS 보증건수,
    SUM(grnt_sply_cnt)            AS 총공급건수,
    ROUND(SUM(grnt_amt), 1)       AS "총보증금액(억)",
    ROUND(SUM(aprs_eval_amt), 1)  AS "총감정가(억)"
FROM   TB_RGR011M_HSPRC
GROUP BY basis_ym
ORDER BY basis_ym;"""),
        ("예제 2 — 지급방식별 통계",
         "지급방식코드별 평균·최대·최소 공급건수를 비교합니다.",
         """SELECT
    pnsn_gv_meth_dvcd             AS 지급방식코드,
    COUNT(*)                      AS 데이터건수,
    ROUND(AVG(grnt_sply_cnt), 0)  AS 평균공급건수,
    MAX(grnt_sply_cnt)            AS 최대공급건수,
    MIN(grnt_sply_cnt)            AS 최소공급건수
FROM   TB_RGR011M_HSPRC
GROUP BY pnsn_gv_meth_dvcd
ORDER BY 평균공급건수 DESC;"""),
        ("예제 3 — HAVING으로 집계 필터",
         "평균 공급건수가 5,000건 이상인 지급방식만 조회합니다.",
         """SELECT
    pnsn_gv_meth_dvcd,
    ROUND(AVG(grnt_sply_cnt), 0) AS 평균공급건수
FROM   TB_RGR011M_HSPRC
GROUP BY pnsn_gv_meth_dvcd
HAVING AVG(grnt_sply_cnt) >= 5000
ORDER BY 평균공급건수 DESC;"""),
        ("예제 4 — WHERE + GROUP BY + HAVING",
         "202201 이후 데이터 중 지역코드별 공급건수 합계가 30,000건 이상인 지역.",
         """SELECT
    hs_loc_zone_dvcd             AS 지역코드,
    SUM(grnt_sply_cnt)           AS 총공급건수,
    ROUND(SUM(grnt_amt), 1)      AS "총보증금액(억)"
FROM   TB_RGR011M_HSPRC
WHERE  basis_ym >= '202201'
GROUP BY hs_loc_zone_dvcd
HAVING SUM(grnt_sply_cnt) >= 30000
ORDER BY 총공급건수 DESC;"""),
    ]
    for title, desc, sql in exs:
        with st.expander(title, expanded=True):
            st.caption(desc)
            st.code(sql, language="sql")
            _result(sql, f"s6_{title}")

    st.divider()
    _practice(
        "202101~202306 기간 데이터에서 기준연월·지급방식코드별로 공급건수 합계를 구하고, 합계가 15,000건 이상인 경우만 내림차순으로 조회하세요.",
        """SELECT
    basis_ym               AS 기준연월,
    pnsn_gv_meth_dvcd      AS 지급방식코드,
    SUM(grnt_sply_cnt)     AS 총공급건수
FROM   TB_RGR011M_HSPRC
WHERE  basis_ym BETWEEN '202101' AND '202306'
GROUP BY basis_ym, pnsn_gv_meth_dvcd
HAVING SUM(grnt_sply_cnt) >= 15000
ORDER BY 총공급건수 DESC;""",
        "p6", hint="GROUP BY에 두 컬럼을 함께 쓰고 HAVING으로 합계를 필터하세요.")
    nav_footer(6)


# ════════════════════════════════════════════════════════════════
# 7. JOIN
# ════════════════════════════════════════════════════════════════
elif idx == 7:
    st.markdown('<div class="sec">⑥ JOIN — 테이블 결합</div>', unsafe_allow_html=True)
    st.markdown("""
`JOIN`은 공통 컬럼(키)을 기준으로 두 테이블을 결합합니다.
`TB_RGR011M_HSPRC`에는 숫자 코드(`hs_loc_zone_dvcd = '11'`)만 있고 지역명은 없습니다.
지역명을 보려면 `TA_COA311M_CDBSC`를 JOIN해야 합니다.
    """)
    st.markdown('''<div class="syn">SELECT a.컬럼, b.컬럼<br>FROM   테이블A a<br>[JOIN종류] 테이블B b ON a.키 = b.키;<br><br>⚠️ ON 조건 누락 시 카테시안 곱 발생!</div>''', unsafe_allow_html=True)

    st.divider()
    t_inn, t_lft, t_typ = st.tabs(["INNER JOIN", "LEFT JOIN", "JOIN 종류 비교"])

    with t_inn:
        c1, c2 = st.columns([1, 1])
        with c1:
            st.markdown("**개념**: ON 조건이 일치하는 행만 반환합니다.")
            st.code("""
RGR011M           COA311M (COA0083)
┌─────────────┐   ┌───────────┐
│ cd_id='11'  │──▶│ cd_id='11'│ ✅ 포함
│ cd_id='41'  │──▶│ cd_id='41'│ ✅ 포함
│ cd_id='99'  │ ✗ │ (없음)    │ ❌ 제외
└─────────────┘   └───────────┘
""", language="text")
            st.markdown("**사용**: 코드가 반드시 마스터에 있을 때")
        with c2:
            sql = """-- 지역명 JOIN (COA0083 그룹, 유효 코드만)
SELECT
    R.grnt_no       AS 보증번호,
    R.basis_ym      AS 기준연월,
    C.cd_nm         AS 지역명,
    R.grnt_sply_cnt AS 공급건수
FROM TB_RGR011M_HSPRC R
INNER JOIN TA_COA311M_CDBSC C
    ON  R.hs_loc_zone_dvcd = C.cd_id
    AND C.cd_grp_id = 'COA0083'
    AND C.del_yn    = 'N'
ORDER BY R.basis_ym, R.grnt_sply_cnt DESC;"""
            st.code(sql, language="sql")
            _result(sql, "j_inner")

    with t_lft:
        c1, c2 = st.columns([1, 1])
        with c1:
            st.markdown("**개념**: 왼쪽 테이블 전체 유지, 오른쪽 매칭 없으면 NULL.")
            st.code("""
RGR011M (왼쪽)  COA311M (오른쪽)
┌─────────────┐  ┌───────────┐
│ cd_id='11'  │─▶│ 있음      │ ✅ 포함
│ cd_id='99'  │─▶│ (없음)    │ ✅ NULL로 포함
└─────────────┘  └───────────┘
""", language="text")
            st.markdown("**사용**: 코드 누락 데이터도 포함해 전수 확인할 때")
        with c2:
            sql = """SELECT
    R.grnt_no                       AS 보증번호,
    R.hs_loc_zone_dvcd              AS 지역코드,
    COALESCE(C.cd_nm, '코드없음')  AS 지역명
FROM TB_RGR011M_HSPRC R
LEFT JOIN TA_COA311M_CDBSC C
    ON  R.hs_loc_zone_dvcd = C.cd_id
    AND C.cd_grp_id = 'COA0083'
    AND C.del_yn    = 'N'
ORDER BY R.basis_ym;"""
            st.code(sql, language="sql")
            _result(sql, "j_left")

    with t_typ:
        st.markdown("""
| JOIN 종류 | 왼쪽 | 오른쪽 | 매칭 없는 행 |
|-----------|------|--------|-------------|
| `INNER JOIN` | 매칭만 | 매칭만 | 제외 |
| `LEFT JOIN` | **전체** | 매칭만 | 오른쪽 = NULL |
| `RIGHT JOIN` | 매칭만 | **전체** | 왼쪽 = NULL |
| `FULL OUTER` | **전체** | **전체** | 상대방 = NULL |
        """)
        st.markdown('''<div class="wrn">⚠️ <b>COA311M JOIN 시 주의사항</b><br>
1. <code>cd_grp_id = 'COA0083'</code> (또는 COA0021) 조건 필수 — 없으면 여러 그룹과 중복 조인됨<br>
2. <code>del_yn = 'N'</code> 조건 추가 — 삭제된 코드 제외<br>
3. 두 컬럼(지역 + 지급방식) JOIN 시 각각 별도 JOIN으로 연결</div>''', unsafe_allow_html=True)
        sql = """-- 지역명 + 지급방식명 동시 JOIN
SELECT
    R.grnt_no       AS 보증번호,
    R.basis_ym      AS 기준연월,
    CZ.cd_nm        AS 지역명,
    CM.cd_nm        AS 지급방식명,
    R.grnt_sply_cnt AS 공급건수,
    R.grnt_amt      AS "보증금액(억)"
FROM TB_RGR011M_HSPRC R
JOIN TA_COA311M_CDBSC CZ
    ON  R.hs_loc_zone_dvcd   = CZ.cd_id
    AND CZ.cd_grp_id = 'COA0083' AND CZ.del_yn = 'N'
JOIN TA_COA311M_CDBSC CM
    ON  R.pnsn_gv_meth_dvcd  = CM.cd_id
    AND CM.cd_grp_id = 'COA0021' AND CM.del_yn = 'N'
WHERE  R.basis_ym = '202306'
ORDER BY R.grnt_sply_cnt DESC;"""
        with st.expander("예제 — 지역명 + 지급방식명 동시 JOIN", expanded=True):
            st.code(sql, language="sql")
            _result(sql, "j_double")

    st.divider()
    _practice(
        "COA311M과 RGR011M을 JOIN하여 수도권(서울·경기·인천) 지역의 202306 기준 보증 건을\n지역명·지급방식코드·공급건수로 조회하고, 공급건수 내림차순으로 정렬하세요.\n(힌트: 수도권 지역코드는 11, 41, 28)",
        """SELECT
    C.cd_nm         AS 지역명,
    R.pnsn_gv_meth_dvcd AS 지급방식코드,
    R.grnt_sply_cnt AS 공급건수
FROM TB_RGR011M_HSPRC R
JOIN TA_COA311M_CDBSC C
    ON  R.hs_loc_zone_dvcd = C.cd_id
    AND C.cd_grp_id = 'COA0083'
    AND C.del_yn    = 'N'
WHERE  R.basis_ym = '202306'
  AND  C.cd_id IN ('11', '41', '28')
ORDER BY R.grnt_sply_cnt DESC;""",
        "p7", hint="JOIN 후 WHERE에서 basis_ym = '202306' 과 cd_id IN ('11','41','28') 를 AND로 연결하세요.")
    nav_footer(7)


# ════════════════════════════════════════════════════════════════
# 8. ANSI 함수
# ════════════════════════════════════════════════════════════════
elif idx == 8:
    st.markdown('<div class="sec">⑦ ANSI 함수 — ROUND · COALESCE · CASE WHEN · NULLIF</div>', unsafe_allow_html=True)
    st.markdown("ANSI SQL 표준 함수들은 Oracle·MS-SQL·Trino 등 거의 모든 DB에서 동일하게 동작합니다.")

    t1, t2, t3, t4 = st.tabs(["🔢 ROUND & 수치함수", "🔄 COALESCE & NULLIF", "🔀 CASE WHEN", "🧩 복합 활용"])

    with t1:
        st.markdown("### ROUND — 반올림")
        st.markdown('''<div class="syn">ROUND(값, 소수점자리)<br>ROUND(3.14159, 2)  →  3.14<br>ROUND(3.14159, 0)  →  3<br>ROUND(3567, -2)    →  3600  (음수: 정수 자리)</div>''', unsafe_allow_html=True)
        sql_r = """SELECT
    grnt_no                           AS 보증번호,
    grnt_amt                          AS "보증금액(억,원본)",
    ROUND(grnt_amt, 1)                AS "보증금액(억,1자리)",
    ROUND(grnt_amt * 10000, 0)        AS "보증금액(만원)",
    aprs_eval_amt                     AS "감정가(억,원본)",
    ROUND(aprs_eval_amt, 2)           AS "감정가(억,2자리)",
    ROUND(aprs_eval_amt * 10000, -2)  AS "감정가(만원,백단위)"
FROM TB_RGR011M_HSPRC
WHERE basis_ym = '202306'
ORDER BY grnt_amt DESC;"""
        st.code(sql_r, language="sql")
        _result(sql_r, "fn_round")

        st.divider()
        st.markdown("### 기타 수치 함수")
        st.markdown("""
| 함수 | 설명 | 예시 |
|------|------|------|
| `ABS(n)` | 절댓값 | `ABS(-3.5)` → 3.5 |
| `CEIL(n)` | 올림 | `CEIL(3.1)` → 4 |
| `FLOOR(n)` | 내림 | `FLOOR(3.9)` → 3 |
| `MOD(a,b)` | 나머지 | `MOD(7,3)` → 1 |
| `POWER(a,b)` | 거듭제곱 | `POWER(2,3)` → 8 |
        """)
        sql_num = """SELECT
    grnt_sply_cnt                   AS 공급건수,
    ABS(grnt_amt - 3000)            AS "보증금액-3000억 절댓값",
    CEIL(aprs_eval_amt)             AS "감정가(올림)",
    FLOOR(aprs_eval_amt)            AS "감정가(내림)"
FROM TB_RGR011M_HSPRC
WHERE basis_ym = '202306' LIMIT 8;"""
        st.code(sql_num, language="sql")
        _result(sql_num, "fn_num")

    with t2:
        st.markdown("### COALESCE — NULL 대체")
        st.markdown('''<div class="syn">COALESCE(값1, 값2, 값3, …)<br>← 왼쪽부터 순서대로 검사해 첫 번째 non-NULL 값을 반환합니다.<br><br>COALESCE(NULL, NULL, 3)  →  3<br>COALESCE('A', NULL, 'C')  →  'A'</div>''', unsafe_allow_html=True)
        sql_coal = """SELECT
    grnt_no                                          AS 보증번호,
    kab_trd_avg_prc                                  AS "KB거래가(원본)",
    kb_trd_avg_prc                                   AS "KB시세(원본)",
    COALESCE(kab_trd_avg_prc, kb_trd_avg_prc, 0)    AS "시세(우선순위)",
    ROUND(COALESCE(kab_trd_avg_prc, kb_trd_avg_prc, 0) * 10000, 0) AS "시세(만원)"
FROM TB_RGR011M_HSPRC
ORDER BY grnt_no;"""
        st.markdown('''<div class="tip">💡 실무 패턴: KB거래가 없으면 KB시세, 그것도 없으면 0으로 대체. 집계 시 NULL로 인한 오류를 방지합니다.</div>''', unsafe_allow_html=True)
        st.code(sql_coal, language="sql")
        _result(sql_coal, "fn_coal")

        st.divider()
        st.markdown("### NULLIF — 조건부 NULL 변환")
        st.markdown('''<div class="syn">NULLIF(값1, 값2)<br>← 값1 = 값2 이면 NULL, 아니면 값1 반환<br>주로 0 나누기 방지에 사용합니다.</div>''', unsafe_allow_html=True)
        sql_ni = """SELECT
    basis_ym,
    SUM(grnt_sply_cnt)                                  AS 공급건수,
    SUM(grnt_amt)                                        AS "보증금액합(억)",
    ROUND(SUM(grnt_amt) / NULLIF(SUM(grnt_sply_cnt), 0), 4) AS "건당평균보증금액(억)"
FROM TB_RGR011M_HSPRC
GROUP BY basis_ym
ORDER BY basis_ym;"""
        st.code(sql_ni, language="sql")
        _result(sql_ni, "fn_nullif")

    with t3:
        st.markdown("### CASE WHEN — 조건 분기")
        st.markdown('''<div class="syn">-- 검색형 (조건식 직접 작성)<br>CASE WHEN 조건1 THEN 값1<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;WHEN 조건2 THEN 값2<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;ELSE 기본값<br>END<br><br>-- 단순형 (컬럼=값 비교)<br>CASE 컬럼 WHEN '값1' THEN 결과1<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;WHEN '값2' THEN 결과2<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;ELSE 기본값 END</div>''', unsafe_allow_html=True)

        sql_cw1 = """-- 단순형: 지급방식코드 → 한글명
SELECT
    grnt_no,
    pnsn_gv_meth_dvcd AS 코드,
    CASE pnsn_gv_meth_dvcd
        WHEN '07' THEN '전세보증'
        WHEN '08' THEN '구입자금보증'
        WHEN '09' THEN '중도금보증'
        ELSE '기타'
    END               AS 지급방식명,
    grnt_sply_cnt     AS 공급건수
FROM TB_RGR011M_HSPRC
WHERE basis_ym = '202306'
ORDER BY grnt_sply_cnt DESC;"""
        with st.expander("예제 1 — 코드 → 한글명 변환 (단순형)", expanded=True):
            st.code(sql_cw1, language="sql")
            _result(sql_cw1, "fn_cw1")

        sql_cw2 = """-- 검색형: 보증금액 구간 분류
SELECT
    grnt_no,
    grnt_amt AS "보증금액(억)",
    CASE
        WHEN grnt_amt >= 4000 THEN '대형 (4000억+)'
        WHEN grnt_amt >= 2000 THEN '중형 (2000~4000억)'
        WHEN grnt_amt >= 1000 THEN '소형 (1000~2000억)'
        ELSE                       '극소 (1000억 미만)'
    END AS 규모구분
FROM TB_RGR011M_HSPRC
WHERE basis_ym = '202306'
ORDER BY grnt_amt DESC;"""
        with st.expander("예제 2 — 금액 구간 분류 (검색형)", expanded=True):
            st.code(sql_cw2, language="sql")
            _result(sql_cw2, "fn_cw2")

        sql_cw3 = """-- CASE WHEN + SUM → 피벗 집계 (수도권/지방)
SELECT
    basis_ym AS 기준연월,
    SUM(CASE WHEN hs_loc_zone_dvcd IN ('11','41','28') THEN grnt_sply_cnt ELSE 0 END) AS 수도권,
    SUM(CASE WHEN hs_loc_zone_dvcd NOT IN ('11','41','28') THEN grnt_sply_cnt ELSE 0 END) AS 지방,
    SUM(grnt_sply_cnt) AS 전국
FROM TB_RGR011M_HSPRC
GROUP BY basis_ym
ORDER BY basis_ym;"""
        with st.expander("예제 3 — 수도권/지방 피벗 집계", expanded=True):
            st.code(sql_cw3, language="sql")
            _result(sql_cw3, "fn_cw3")

        sql_cw4 = """-- CASE WHEN + GROUP BY → KB시세 출처 분류
SELECT
    basis_ym AS 기준연월,
    CASE kab_sise_rsch_dy
        WHEN '0' THEN '국민은행인터넷시세'
        ELSE            '부동산원인터넷시세'
    END AS 시세출처,
    COUNT(*) AS 건수,
    ROUND(AVG(COALESCE(kab_trd_avg_prc, kb_trd_avg_prc, 0)), 3) AS "평균시세(억)"
FROM TB_RGR011M_HSPRC
GROUP BY basis_ym, kab_sise_rsch_dy
ORDER BY basis_ym, 시세출처;"""
        with st.expander("예제 4 — 시세 출처별 집계", expanded=True):
            st.code(sql_cw4, language="sql")
            _result(sql_cw4, "fn_cw4")

    with t4:
        st.markdown("### 복합 함수 활용 예제")
        sql_comp = """-- COALESCE + ROUND + CASE WHEN + JOIN 조합
SELECT
    R.grnt_no                                       AS 보증번호,
    R.basis_ym                                      AS 기준연월,
    CZ.cd_nm                                        AS 지역명,
    CASE R.pnsn_gv_meth_dvcd
        WHEN '07' THEN '전세보증'
        WHEN '08' THEN '구입자금보증'
        WHEN '09' THEN '중도금보증'
        ELSE '기타'
    END                                             AS 지급방식명,
    R.grnt_sply_cnt                                 AS 공급건수,
    ROUND(R.grnt_amt, 1)                            AS "보증금액(억)",
    ROUND(COALESCE(R.kab_trd_avg_prc,
                   R.kb_trd_avg_prc, 0) * 10000, 0) AS "시세(만원)",
    CASE
        WHEN R.kab_sise_rsch_dy = '0' THEN '국민은행'
        ELSE '부동산원'
    END                                             AS 시세출처,
    CASE
        WHEN R.grnt_sply_cnt >= 15000 THEN '대형'
        WHEN R.grnt_sply_cnt >=  5000 THEN '중형'
        ELSE '소형'
    END                                             AS 규모구분
FROM TB_RGR011M_HSPRC R
JOIN TA_COA311M_CDBSC CZ
    ON  R.hs_loc_zone_dvcd = CZ.cd_id
    AND CZ.cd_grp_id = 'COA0083' AND CZ.del_yn = 'N'
WHERE  R.basis_ym = '202306'
ORDER BY R.grnt_sply_cnt DESC;"""
        st.code(sql_comp, language="sql")
        _result(sql_comp, "fn_comp")

    st.divider()
    _practice(
        "basis_ym이 202206인 데이터에서 보증번호·지역명(JOIN)·지급방식명(CASE WHEN)·공급건수·시세(만원, COALESCE + ROUND)를 조회하세요.\n시세 우선순위: kab_trd_avg_prc → kb_trd_avg_prc → 0",
        """SELECT
    R.grnt_no                                        AS 보증번호,
    C.cd_nm                                          AS 지역명,
    CASE R.pnsn_gv_meth_dvcd
        WHEN '07' THEN '전세보증'
        WHEN '08' THEN '구입자금보증'
        WHEN '09' THEN '중도금보증'
        ELSE '기타'
    END                                              AS 지급방식명,
    R.grnt_sply_cnt                                  AS 공급건수,
    ROUND(COALESCE(R.kab_trd_avg_prc,
                   R.kb_trd_avg_prc, 0) * 10000, 0) AS "시세(만원)"
FROM TB_RGR011M_HSPRC R
JOIN TA_COA311M_CDBSC C
    ON  R.hs_loc_zone_dvcd = C.cd_id
    AND C.cd_grp_id = 'COA0083' AND C.del_yn = 'N'
WHERE  R.basis_ym = '202206'
ORDER BY R.grnt_sply_cnt DESC;""",
        "p8", hint="JOIN 후 CASE WHEN으로 지급방식 변환, COALESCE + ROUND로 시세 처리하세요.")
    nav_footer(8)


# ════════════════════════════════════════════════════════════════
# 9. 복합 쿼리 실전
# ════════════════════════════════════════════════════════════════
elif idx == 9:
    st.markdown('<div class="sec">🚀 복합 쿼리 실전</div>', unsafe_allow_html=True)
    st.markdown("지금까지 배운 모든 키워드를 조합합니다. **직접 작성 후 정답과 비교**하세요.")
    st.markdown('''<div class="tip">💡 풀이 순서: ① 필요 테이블 파악 → ② JOIN 구조 → ③ WHERE 조건 → ④ GROUP BY/HAVING → ⑤ SELECT 컬럼 & CASE/COALESCE → ⑥ ORDER BY</div>''', unsafe_allow_html=True)
    st.divider()

    _practice(
        "📌 실전 1 — 기준연월·지역명·지급방식명별 현황 리포트\n\n"
        "basis_ym, 지역명(JOIN), 지급방식명(CASE WHEN)별로 공급건수 합계, 보증금액 합계(억,1자리), 시세 평균(만원,COALESCE+ROUND)을 구하고 기준연월 내림차순 → 공급건수 내림차순으로 정렬하세요.",
        """SELECT
    R.basis_ym                                        AS 기준연월,
    CZ.cd_nm                                          AS 지역명,
    CASE R.pnsn_gv_meth_dvcd
        WHEN '07' THEN '전세보증' WHEN '08' THEN '구입자금보증'
        WHEN '09' THEN '중도금보증' ELSE '기타'
    END                                               AS 지급방식명,
    SUM(R.grnt_sply_cnt)                              AS 총공급건수,
    ROUND(SUM(R.grnt_amt), 1)                         AS "총보증금액(억)",
    ROUND(AVG(COALESCE(R.kab_trd_avg_prc,
                       R.kb_trd_avg_prc, 0)) * 10000, 0) AS "평균시세(만원)"
FROM TB_RGR011M_HSPRC R
JOIN TA_COA311M_CDBSC CZ
    ON  R.hs_loc_zone_dvcd = CZ.cd_id
    AND CZ.cd_grp_id = 'COA0083' AND CZ.del_yn = 'N'
GROUP BY R.basis_ym, CZ.cd_nm, R.pnsn_gv_meth_dvcd
ORDER BY R.basis_ym DESC, 총공급건수 DESC;""",
        "adv1", hint="JOIN 후 3개 컬럼 GROUP BY, SUM·ROUND·COALESCE 집계.")

    st.divider()
    _practice(
        "📌 실전 2 — 전체 평균 초과 공급건수 지역 (서브쿼리)\n\n"
        "기준연월별 지역코드별 공급건수 합계를 구하고, 그 합계가 전체 데이터 평균 공급건수보다 높은 레코드만 지역명과 함께 조회하세요.",
        """SELECT
    R.basis_ym              AS 기준연월,
    C.cd_nm                 AS 지역명,
    SUM(R.grnt_sply_cnt)    AS 총공급건수
FROM TB_RGR011M_HSPRC R
JOIN TA_COA311M_CDBSC C
    ON  R.hs_loc_zone_dvcd = C.cd_id
    AND C.cd_grp_id = 'COA0083' AND C.del_yn = 'N'
GROUP BY R.basis_ym, R.hs_loc_zone_dvcd, C.cd_nm
HAVING SUM(R.grnt_sply_cnt) > (
    SELECT AVG(grnt_sply_cnt) FROM TB_RGR011M_HSPRC
)
ORDER BY 총공급건수 DESC;""",
        "adv2", hint="HAVING 조건에 서브쿼리 (SELECT AVG(grnt_sply_cnt) FROM TB_RGR011M_HSPRC)를 사용하세요.")

    st.divider()
    _practice(
        "📌 실전 3 — 시세 구분별 건수 피벗 + 보증금액 구간 분류\n\n"
        "basis_ym별로 부동산원 시세 건수, 국민은행 시세 건수, 전체 건수를 구하고\n보증금액 평균이 3,000억 이상인 연월만 조회하세요.",
        """SELECT
    basis_ym AS 기준연월,
    SUM(CASE WHEN kab_sise_rsch_dy <> '0' THEN 1 ELSE 0 END) AS 부동산원건수,
    SUM(CASE WHEN kab_sise_rsch_dy  = '0' THEN 1 ELSE 0 END) AS 국민은행건수,
    COUNT(*)                                                    AS 전체건수,
    ROUND(AVG(grnt_amt), 1)                                     AS "평균보증금액(억)"
FROM TB_RGR011M_HSPRC
GROUP BY basis_ym
HAVING AVG(grnt_amt) >= 3000
ORDER BY basis_ym;""",
        "adv3", hint="CASE WHEN + SUM으로 피벗, HAVING AVG(grnt_amt) >= 3000으로 필터.")
    nav_footer(9)


# ════════════════════════════════════════════════════════════════
# 10. 자유 실습 & 연습문제
# ════════════════════════════════════════════════════════════════
elif idx == 10:
    st.markdown('<div class="sec">✏️ 자유 실습 & 연습문제</div>', unsafe_allow_html=True)

    t_free, t_quiz = st.tabs(["🔓 자유 실습", "📝 연습문제 (정답 포함)"])

    with t_free:
        c1, c2 = st.columns(2)
        with c1:
            st.markdown("**📋 주요 컬럼 참조**")
            st.markdown("""
**TA_COA311M_CDBSC**
- `cd_grp_id` : 코드그룹 (COA0083=지역/COA0021=지급방식)
- `cd_id`, `cd_nm` : 코드ID, 코드명
- `del_yn` : 삭제여부 (N=유효)
- `idct_seq` : 정렬순서, `cd_expn_cont` : 설명

**TB_RGR011M_HSPRC**
- `basis_ym` : 기준연월 (YYYYMM)
- `grnt_no` : 보증번호
- `hs_loc_zone_dvcd` → COA311M.cd_id (COA0083)
- `pnsn_gv_meth_dvcd` → COA311M.cd_id (COA0021)
- `grnt_sply_cnt` : 공급건수
- `grnt_amt` : 보증금액(억), `aprs_eval_amt` : 감정가(억)
- `kab_trd_avg_prc` : KB거래가(억), `kb_trd_avg_prc` : KB시세(억)
- `kab_sise_rsch_dy` : 시세구분 (0=국민은행)
            """)
        with c2:
            st.markdown("**⚡ 빠른 참조**")
            st.code("""-- JOIN 기본 패턴
FROM TB_RGR011M_HSPRC R
JOIN TA_COA311M_CDBSC C
  ON  R.hs_loc_zone_dvcd = C.cd_id
  AND C.cd_grp_id = 'COA0083'
  AND C.del_yn = 'N'

-- 시세 COALESCE
COALESCE(kab_trd_avg_prc, kb_trd_avg_prc, 0) * 10000

-- 지급방식 CASE WHEN
CASE pnsn_gv_meth_dvcd
  WHEN '07' THEN '전세보증'
  WHEN '08' THEN '구입자금보증'
  WHEN '09' THEN '중도금보증'
  ELSE '기타' END

-- 기간 조건
WHERE basis_ym BETWEEN '202001' AND '202312'""", language="sql")

        presets = [
            "직접 입력",
            "SELECT cd_grp_id, cd_id, cd_nm FROM TA_COA311M_CDBSC WHERE del_yn = 'N' ORDER BY cd_grp_id, idct_seq",
            "SELECT * FROM TB_RGR011M_HSPRC LIMIT 10",
            "SELECT basis_ym, SUM(grnt_sply_cnt) AS 총공급건수 FROM TB_RGR011M_HSPRC GROUP BY basis_ym ORDER BY basis_ym",
            "SELECT R.grnt_no, C.cd_nm, R.pnsn_gv_meth_dvcd, R.grnt_sply_cnt FROM TB_RGR011M_HSPRC R JOIN TA_COA311M_CDBSC C ON R.hs_loc_zone_dvcd = C.cd_id AND C.cd_grp_id = 'COA0083' AND C.del_yn = 'N' WHERE R.basis_ym = '202306'",
        ]
        preset = st.selectbox("예제 불러오기", presets)
        usr = st.text_area("SQL 입력", value="" if preset == "직접 입력" else preset,
                           height=160, placeholder="SELECT ...")
        if st.button("▶ 실행", type="primary"):
            if usr.strip():
                df, err = run_sql(usr)
                st.error(f"오류: {err}") if err else (
                    st.success(f"✅ {len(df)}행") or
                    st.dataframe(df, use_container_width=True, hide_index=True))
            else:
                st.warning("SQL을 입력해주세요.")

    with t_quiz:
        st.markdown("직접 풀고 **📋 정답 보기**로 확인하세요.")
        st.divider()

        quizzes = [
            ("1", "🟢 기초",
             "TB_RGR011M_HSPRC 에서 basis_ym='202306' 인 데이터를 보증금액 내림차순으로 조회하세요.\n(보증번호·지역코드·지급방식코드·공급건수·보증금액 포함)",
             "WHERE basis_ym = '202306' 후 ORDER BY grnt_amt DESC",
             """SELECT grnt_no, hs_loc_zone_dvcd, pnsn_gv_meth_dvcd,
       grnt_sply_cnt, ROUND(grnt_amt,1) AS "보증금액(억)"
FROM   TB_RGR011M_HSPRC
WHERE  basis_ym = '202306'
ORDER BY grnt_amt DESC;"""),
            ("2", "🟢 기초",
             "TA_COA311M_CDBSC 에서 유효(del_yn='N')한 지역코드(COA0083) 중 cd_nm이 '대'로 시작하는 지역만 조회하세요.",
             "cd_grp_id = 'COA0083' AND del_yn = 'N' AND cd_nm LIKE '대%'",
             """SELECT cd_id, cd_nm, cd_expn_cont, idct_seq
FROM   TA_COA311M_CDBSC
WHERE  cd_grp_id = 'COA0083'
  AND  del_yn = 'N'
  AND  cd_nm LIKE '대%'
ORDER BY idct_seq;"""),
            ("3", "🟡 중급",
             "기준연월·지급방식코드별 공급건수 합계를 구하되, 지급방식코드를 CASE WHEN으로 한글명으로 변환하여 조회하세요. 합계 내림차순 정렬.",
             "GROUP BY basis_ym, pnsn_gv_meth_dvcd 후 CASE WHEN으로 한글 변환",
             """SELECT
    basis_ym AS 기준연월,
    CASE pnsn_gv_meth_dvcd
        WHEN '07' THEN '전세보증'
        WHEN '08' THEN '구입자금보증'
        WHEN '09' THEN '중도금보증'
        ELSE '기타'
    END AS 지급방식명,
    SUM(grnt_sply_cnt) AS 총공급건수
FROM   TB_RGR011M_HSPRC
GROUP BY basis_ym, pnsn_gv_meth_dvcd
ORDER BY 총공급건수 DESC;"""),
            ("4", "🟡 중급",
             "COA311M과 RGR011M을 JOIN하여 수도권(cd_id IN ('11','41','28')) 지역의 202306 기준 보증 현황을\n지역명·지급방식코드·공급건수로 조회하고 공급건수 내림차순 정렬하세요.",
             "INNER JOIN + WHERE basis_ym='202306' AND cd_id IN ('11','41','28')",
             """SELECT C.cd_nm AS 지역명, R.pnsn_gv_meth_dvcd AS 지급방식코드,
       R.grnt_sply_cnt AS 공급건수
FROM TB_RGR011M_HSPRC R
JOIN TA_COA311M_CDBSC C
    ON  R.hs_loc_zone_dvcd = C.cd_id
    AND C.cd_grp_id = 'COA0083' AND C.del_yn = 'N'
WHERE  R.basis_ym = '202306'
  AND  C.cd_id IN ('11','41','28')
ORDER BY R.grnt_sply_cnt DESC;"""),
            ("5", "🔴 심화",
             "기준연월별로 수도권 공급건수, 지방 공급건수, 전국 공급건수, 수도권 비율(%)을 구하고\n수도권 비율이 60% 이상인 연월만 조회하세요.",
             "CASE WHEN 피벗 + HAVING + ROUND, 수도권 = hs_loc_zone_dvcd IN ('11','41','28')",
             """SELECT
    basis_ym AS 기준연월,
    SUM(CASE WHEN hs_loc_zone_dvcd IN ('11','41','28') THEN grnt_sply_cnt ELSE 0 END) AS 수도권,
    SUM(CASE WHEN hs_loc_zone_dvcd NOT IN ('11','41','28') THEN grnt_sply_cnt ELSE 0 END) AS 지방,
    SUM(grnt_sply_cnt) AS 전국,
    ROUND(100.0 * SUM(CASE WHEN hs_loc_zone_dvcd IN ('11','41','28') THEN grnt_sply_cnt ELSE 0 END)
          / NULLIF(SUM(grnt_sply_cnt), 0), 1) AS "수도권비율(%)"
FROM TB_RGR011M_HSPRC
GROUP BY basis_ym
HAVING ROUND(100.0 * SUM(CASE WHEN hs_loc_zone_dvcd IN ('11','41','28') THEN grnt_sply_cnt ELSE 0 END)
             / NULLIF(SUM(grnt_sply_cnt), 0), 1) >= 60
ORDER BY basis_ym;"""),
        ]

        for no, lvl, q, hint, ans in quizzes:
            st.markdown(f"### 문제 {no} &nbsp; {lvl}")
            _practice(q, ans, f"qz{no}", hint=hint)
            st.divider()

        st.markdown('''<div class="tip">
✅ <b>실수 방지 최종 체크리스트</b><br>
• NULL 비교 → <code>IS NULL</code> (<code>= NULL</code>은 항상 FALSE)<br>
• COA311M JOIN → <code>cd_grp_id = 'COA0083'</code> AND <code>del_yn = 'N'</code> 조건 필수<br>
• 집계 조건 → <code>WHERE</code> 아닌 <code>HAVING</code><br>
• GROUP BY → SELECT의 비집계 컬럼 모두 포함<br>
• CASE WHEN → 반드시 <code>END</code>로 닫기, ELSE 생략 시 NULL 반환<br>
• JOIN → <code>ON</code> 조건 누락 시 카테시안 곱 발생<br>
• 0 나누기 방지 → <code>NULLIF(분모, 0)</code> 사용<br>
• LIKE <code>'%값%'</code> → 대용량 테이블에서 Full Scan 주의
</div>''', unsafe_allow_html=True)
    nav_footer(10)

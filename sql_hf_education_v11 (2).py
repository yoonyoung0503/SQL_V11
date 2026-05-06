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
-- TA_COA311M_CDBSC : 공통코드 마스터 (이미지 실제 컬럼 기준)
CREATE TABLE TA_COA311M_CDBSC (
    cd_ung_no     TEXT,   -- 고유번호
    cd_grp_id     TEXT,   -- 코드그룹ID  (RGR0015 / SEA0541 / HTA0018 / BMC0002)
    up_cd_grp_id  TEXT,   -- 상위 코드그룹ID
    valid_strt_dy TEXT,   -- 유효시작일자 (YYYYMMDD)
    valid_end_dy  TEXT,   -- 유효종료일자 (YYYYMMDD)
    cd_nm         TEXT,   -- 코드명
    cd_expn_cont  TEXT,   -- 코드설명
    cd_id         TEXT,   -- 코드ID  ← JOIN 키
    up_cd_id      TEXT,   -- 상위 코드ID
    cd_val        REAL,   -- 코드값 (decimal, NULL 가능)
    min_val       TEXT,   -- 최솟값 (NULL 가능)
    max_val       TEXT,   -- 최댓값 (NULL 가능)
    del_yn        TEXT    -- 삭제여부 (Y=삭제, N=유효)
);

-- TB_RGR011M_HSPRC : 시세정보 (이미지 실제 컬럼 기준)
CREATE TABLE TB_RGR011M_HSPRC (
    basis_ym          TEXT,   -- 기준연월 (YYYYMM)
    grnt_no           TEXT,   -- 보증번호
    gmt_no            TEXT,   -- 보증상품번호
    aprs_eval_mthd_cd TEXT,   -- 심사평가방법코드 → COA311M.cd_id (COA0099)
    hf_bldg_no        TEXT,   -- HF건물번호
    kab_size_rsrh_dy  TEXT,   -- KAB시세조사일자 (YYYYMMDD)
    kab_trd_bttrn_prc REAL,   -- KAB매매하한가격 (억원)
    kab_trd_top_prc   REAL,   -- KAB매매상한가격 (억원)
    kab_trd_avg_prc   REAL,   -- KAB매매평균가격 (억원)
    kb_trd_top_prc    REAL,   -- KB매매상한가격  (억원)
    kb_trd_avg_prc    REAL,   -- KB매매평균가격  (억원)
    kb_trd_bttrn_prc  REAL    -- KB매매하한가격  (억원)
);

-- ── COA311M 데이터 ────────────────────────────────────────────────────────
-- (cd_ung_no, cd_grp_id, up_cd_grp_id, valid_strt_dy, valid_end_dy,
--  cd_nm, cd_expn_cont, cd_id, up_cd_id, cd_val, min_val, max_val, del_yn)

-- RGR0015 그룹
INSERT INTO TA_COA311M_CDBSC VALUES
 ('U001','RGR0015','EXD0176','20000101','99991231','서울특별시',   '서울특별시',            '11','00',NULL,NULL,NULL,'N'),
 ('U002','RGR0015','EXD0176','20000101','99991231','경기도',       '경기도',                '41','00',NULL,NULL,NULL,'N'),
 ('U003','RGR0015','EXD0176','20000101','99991231','인천광역시',   '인천광역시',            '28','00',NULL,NULL,NULL,'N'),
 ('U004','RGR0015','EXD0176','20000101','99991231','부산광역시',   '부산광역시',            '26','00',NULL,NULL,NULL,'N'),
 ('U005','RGR0015','EXD0176','20000101','99991231','대구광역시',   '대구광역시',            '27','00',NULL,NULL,NULL,'N'),
 ('U006','RGR0015','EXD0176','20000101','99991231','광주광역시',   '광주광역시',            '29','00',NULL,NULL,NULL,'N'),
 ('U007','RGR0015','EXD0176','20000101','99991231','대전광역시',   '대전광역시',            '30','00',NULL,NULL,NULL,'N'),
 ('U008','RGR0015','EXD0176','20000101','99991231','강원특별자치도','강원특별자치도',        '51','00',NULL,NULL,NULL,'N'),
 ('U009','RGR0015','EXD0176','20000101','20201231','미분류',       '기타지역(미사용)',      '99','00',NULL,NULL,NULL,'Y');

-- SEA0541 그룹
INSERT INTO TA_COA311M_CDBSC VALUES
 ('U101','SEA0541','EXD0176','20000101','99991231','전세보증',      '전세자금보증',          '07','00',NULL,NULL,NULL,'N'),
 ('U102','SEA0541','EXD0176','20000101','99991231','구입자금보증',  '주택구입자금보증',      '08','00',NULL,NULL,NULL,'N'),
 ('U103','SEA0541','EXD0176','20000101','99991231','중도금보증',    '중도금대출보증',        '09','00',NULL,NULL,NULL,'N'),
 ('U104','SEA0541','EXD0176','20000101','20221231','임차보증',      '임차보증금대출(폐지)',  '10','00',NULL,NULL,NULL,'Y');

-- HTA0018 그룹 (JOIN 실습용 — aprs_eval_mthd_cd='01' 단일값)
INSERT INTO TA_COA311M_CDBSC VALUES
 ('U201','HTA0018','EXD0176','20000101','99991231','심사평가방법01','심사평가방법코드01','01','00',NULL,NULL,NULL,'N');

-- BMC0002 그룹 (ROUND·COALESCE 실습용 — cd_val 보유)
INSERT INTO TA_COA311M_CDBSC VALUES
 ('U301','BMC0002','EXD0176','20000101','99991231','우수','우수등급(최상위)', 'A','00',4.5,'3.50','5.00','N'),
 ('U302','BMC0002','EXD0176','20000101','99991231','양호','양호등급',         'B','00',3.5,'2.50','3.49','N'),
 ('U303','BMC0002','EXD0176','20000101','99991231','보통','보통등급',         'C','00',2.5,'1.50','2.49','N'),
 ('U304','BMC0002','EXD0176','20000101','99991231','미흡','미흡등급',         'D','00',1.5, NULL, NULL, 'N'),
 ('U305','BMC0002','EXD0176','20000101','20231231','불량','불량등급(폐지)',    'E','00',0.5, NULL,'1.49','Y');

-- ── TB_RGR011M_HSPRC 시세 데이터 (JOIN 실습용) ───────────────────────────
-- (basis_ym, grnt_no, gmt_no, aprs_eval_mthd_cd, hf_bldg_no, kab_size_rsrh_dy,
--  kab_trd_bttrn_prc, kab_trd_top_prc, kab_trd_avg_prc,
--  kb_trd_top_prc, kb_trd_avg_prc, kb_trd_bttrn_prc)
INSERT INTO TB_RGR011M_HSPRC VALUES
 ('202306','RQAD2007000149','P001','01','B001','20230601', 3.210, 4.250, 3.820, 4.050, 3.640, 3.100),
 ('202306','RQAD2007000150','P002','01','B002','20230615', 5.100, 6.350, 5.790, 6.020, 5.510, 4.890),
 ('202306','RQAD2007000152','P003','01','B003','20230601', 2.800, 3.510, 3.120, 3.330, 2.910, 2.500),
 ('202306','RQAD2007000153','P004','01','B004','20230620', 7.500, 9.010, 8.210, 8.810, 8.010, 7.200),
 ('202306','RQAD2007000154','P005','01','B005','20230615', 4.200, 5.130, 4.710, 4.920, 4.510, 4.010),
 ('202212','RQAD2007000155','P006','01','B001','20221201', 3.010, 4.010, 3.510, 3.810, 3.310, 2.810),
 ('202212','RQAD2007000156','P007','01','B002','20221215', 6.010, 7.210, 6.710, 7.010, 6.510, 5.810),
 ('202212','RQAD2007000157','P008','01','B003','20221210', 4.510, 5.510, 5.010, 5.310, 4.810, 4.210);
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
    st.markdown(f'''<div class="qbox"><b>실습 문제</b><br>{q}</div>''', unsafe_allow_html=True)
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
            st.markdown('''<div class="okbox">정답</div>''', unsafe_allow_html=True)
            st.code(ans, language="sql")
            df, err = run_sql(ans)
            if not err:
                st.caption(f"결과 {len(df)}행")
                st.dataframe(df, use_container_width=True, hide_index=True)


# ════════════════════════════════════════════════════════════════
# 탭 정의  —  아이콘 + 짧은 레이블  (줄바꿈 방지)
# ════════════════════════════════════════════════════════════════
TABS = [
    ("1",  "개요·문법"),
    ("2",  "실습환경"),
    ("3",  "SELECT"),
    ("4",  "WHERE"),
    ("5",  "LIKE"),
    ("6",  "ORDER BY"),
    ("7",  "GROUP BY"),
    ("8",  "JOIN"),
    ("9",  "ANSI 함수"),
    ("10", "복합실전"),
    ("11", "연습과제"),
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
    <p class="hdr-title">HF SQL 교육자료</p>
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
# 0. 개요 & 기본 문법
# ════════════════════════════════════════════════════════════════
if idx == 0:
    st.markdown('<div class="sec">SQL 개요 & 기본 문법</div>', unsafe_allow_html=True)
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
| `INTEGER` | 정수 | (COA311M에는 없음) |
| `REAL / FLOAT` | 실수 | cd_val, kab_trd_avg_prc |
| `TEXT / VARCHAR` | 문자열 | cd_id, cd_nm, valid_strt_dy |
| `NULL` | 값 없음 (0·빈문자와 다름) | cd_val, min_val, max_val 일부 |
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
| `=` | 같다 | `cd_grp_id = 'RGR0015'` |
| `<>` / `!=` | 다르다 | `del_yn <> 'Y'` |
| `>` / `<` / `>=` / `<=` | 비교 | `cd_val >= 3.0` |
| `BETWEEN a AND b` | 범위 (양 끝 포함) | `valid_strt_dy BETWEEN '20000101' AND '20091231'` |
| `IN (...)` | 목록 중 하나 | `cd_grp_id IN ('RGR0015','SEA0541')` |
| `IS NULL` / `IS NOT NULL` | NULL 체크 | `min_val IS NULL` |
| `AND` / `OR` / `NOT` | 논리 연산 | `... AND ... OR ...` |
| `LIKE` | 패턴 | `cd_nm LIKE '%광역%'` |
        """)
        st.markdown('''<div class="tip">우선순위: NOT > AND > OR — 괄호로 명시적으로 묶는 것을 권장합니다.</div>''', unsafe_allow_html=True)
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
# 1. 실습 환경
# ════════════════════════════════════════════════════════════════
elif idx == 1:
    st.markdown('<div class="sec">실습 환경 & 데이터 구조</div>', unsafe_allow_html=True)

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
                st.markdown('<div class="wrn">Connection String을 먼저 입력해주세요.</div>', unsafe_allow_html=True)
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
            st.markdown('<div class="tip">위 코드를 <code>sql_hf_education_v7.py</code> 상단의 설정 영역에 붙여넣으면 앱 재시작 후 내부망 DB로 동작합니다.</div>', unsafe_allow_html=True)

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

        st.markdown('<div class="tip">내부망 방화벽 환경에서는 IT팀에 <b>아웃바운드 포트 허용</b>을 요청하세요.<br>(Trino: 8443, MSSQL: 1433, PG: 5432)</div>', unsafe_allow_html=True)

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
┌──────────────────────────────────────────────────────────────────┐
│ cd_ung_no     TEXT   고유번호                                     │
│ cd_grp_id     TEXT   코드그룹ID  (RGR0015 / SEA0541 │
│                                  HTA0018 / BMC0002)│
│ up_cd_grp_id  TEXT   상위 코드그룹ID                             │
│ valid_strt_dy TEXT   유효시작일자 (YYYYMMDD)                     │
│ valid_end_dy  TEXT   유효종료일자 (YYYYMMDD)                     │
│ cd_nm         TEXT   코드명  ('서울특별시', '전세보증' …)        │
│ cd_expn_cont  TEXT   코드설명                                    │
│ cd_id         TEXT   코드ID  ← JOIN 키                          │
│ up_cd_id      TEXT   상위 코드ID                                 │
│ cd_val        REAL   코드값 (decimal, NULL 가능)                 │
│ min_val       TEXT   최솟값 (NULL 가능)                          │
│ max_val       TEXT   최댓값 (NULL 가능)                          │
│ del_yn        TEXT   삭제여부 (Y=삭제, N=유효)  ← 항상 필터     │
└──────────────────────────────┬───────────────────────────────────┘
                               │ cd_id (cd_grp_id = 'HTA0018')
                               ▼
TB_RGR011M_HSPRC  (시세정보)
┌──────────────────────────────────────────────────────────────────┐
│ basis_ym          TEXT   기준연월 (YYYYMM)                       │
│ grnt_no           TEXT   보증번호                                │
│ gmt_no            TEXT   보증상품번호                            │
│ aprs_eval_mthd_cd TEXT   심사평가방법코드 → COA311M.cd_id (COA0099)│
│ hf_bldg_no        TEXT   HF건물번호                              │
│ kab_size_rsrh_dy  TEXT   KAB시세조사일자 (YYYYMMDD)             │
│ kab_trd_bttrn_prc REAL   KAB매매하한가격 (억원)                 │
│ kab_trd_top_prc   REAL   KAB매매상한가격 (억원)                 │
│ kab_trd_avg_prc   REAL   KAB매매평균가격 (억원)                 │
│ kb_trd_top_prc    REAL   KB매매상한가격  (억원)                  │
│ kb_trd_avg_prc    REAL   KB매매평균가격  (억원)                  │
│ kb_trd_bttrn_prc  REAL   KB매매하한가격  (억원)                  │
└──────────────────────────────────────────────────────────────────┘
""", language="text")

    st.divider()

    # ── 섹션 D: 샘플 데이터 확인 ─────────────────────────────
    st.markdown("### ④ 샘플 데이터 확인")
    t1, t2 = st.tabs(["📋 TA_COA311M_CDBSC", "📋 TB_RGR011M_HSPRC"])
    with t1:
        df, _ = run_sql("SELECT cd_grp_id, cd_id, cd_nm, valid_strt_dy, valid_end_dy, del_yn, cd_expn_cont, cd_val FROM TA_COA311M_CDBSC ORDER BY cd_grp_id, cd_id")
        st.dataframe(df, use_container_width=True, hide_index=True)
        st.caption("cd_grp_id: RGR0015 / SEA0541 / HTA0018 / BMC0002 | del_yn=Y 이면 삭제된 코드")
    with t2:
        df, _ = run_sql("SELECT basis_ym, grnt_no, gmt_no, aprs_eval_mthd_cd, hf_bldg_no, kab_size_rsrh_dy, kab_trd_avg_prc, kb_trd_avg_prc FROM TB_RGR011M_HSPRC ORDER BY basis_ym, grnt_no")
        st.dataframe(df, use_container_width=True, hide_index=True)
        st.caption("basis_ym: 202212~202306 | aprs_eval_mthd_cd → COA311M(COA0099) JOIN 키 | JOIN 탭에서 실습")

    nav_footer(1)



# ════════════════════════════════════════════════════════════════
# 2. SELECT
# ════════════════════════════════════════════════════════════════
elif idx == 2:
    st.markdown('<div class="sec">SELECT — 데이터 조회</div>', unsafe_allow_html=True)
    st.markdown("`SELECT`는 테이블에서 원하는 데이터를 읽어오는 핵심 명령입니다. 이 탭의 모든 예제는 **TA_COA311M_CDBSC** 테이블을 사용합니다.")
    st.markdown('''<div class="syn">SELECT [DISTINCT] 컬럼1, 컬럼2, … ← 조회할 컬럼 (*=전체)<br>FROM   테이블명;</div>''', unsafe_allow_html=True)

    exs = [
        ("예제 1 — SELECT * (전체 컬럼)",
         "테이블의 모든 컬럼을 가져옵니다. 구조 파악용으로 사용하세요.",
         "SELECT *\nFROM   TA_COA311M_CDBSC\nLIMIT  5;",
         "wrn","<b>실무 주의</b><br><code>SELECT *</code> 대신 필요한 컬럼만 명시하세요. 성능·가독성 모두 향상됩니다."),
        ("예제 2 — 특정 컬럼만 조회",
         "코드그룹ID·코드ID·코드명·삭제여부만 선택합니다.",
         "SELECT cd_grp_id, cd_id, cd_nm, del_yn\nFROM   TA_COA311M_CDBSC;",
         None, None),
        ("예제 3 — AS 별칭 + 산술 연산",
         "한글 별칭을 붙이고 코드값(cd_val)을 소수점 없이 반올림합니다. (COA0055 평가등급 그룹은 cd_val 보유)",
         """SELECT
    cd_grp_id                    AS 코드그룹ID,
    cd_id                        AS 코드ID,
    cd_nm                        AS 코드명,
    cd_val                       AS 코드값,
    ROUND(cd_val, 0)             AS "코드값(정수)"
FROM TA_COA311M_CDBSC
WHERE cd_val IS NOT NULL;""",
         "tip","AS 별칭은 ORDER BY에서 사용 가능, WHERE에서는 불가합니다."),
        ("예제 4 — DISTINCT 중복 제거",
         "어떤 코드그룹이 존재하는지 고유값만 조회합니다.",
         "SELECT DISTINCT cd_grp_id AS 코드그룹ID\nFROM   TA_COA311M_CDBSC;",
         None, None),
        ("예제 5 — 유효 코드만 조회 (del_yn 필터)",
         "del_yn='N'인 유효 코드만 표시합니다. COA311M 조회 시 항상 추가해야 하는 조건입니다.",
         """SELECT cd_grp_id, cd_id, cd_nm, cd_expn_cont
FROM   TA_COA311M_CDBSC
WHERE  del_yn = 'N'
ORDER BY cd_grp_id, cd_id;""",
         "tip","COA311M 조회 시 del_yn = 'N' 조건을 항상 추가해 삭제된 코드를 제외하세요."),
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
        "TA_COA311M_CDBSC에서 코드그룹ID(cd_grp_id)·코드ID(cd_id)·코드명(cd_nm)을 조회하되, "
        "코드값(cd_val)과 코드값을 반올림한 정수(ROUND, 소수점 없음)를 함께 표시하세요. "
        "코드값(cd_val)이 있는(IS NOT NULL) 행만 대상입니다.",
        """SELECT
    cd_grp_id         AS 코드그룹ID,
    cd_id             AS 코드ID,
    cd_nm             AS 코드명,
    cd_val            AS 코드값,
    ROUND(cd_val, 0)  AS "코드값(정수)"
FROM TA_COA311M_CDBSC
WHERE cd_val IS NOT NULL;""",
        "p2", hint="WHERE cd_val IS NOT NULL 조건을 추가하고 ROUND(cd_val, 0)으로 정수화하세요.")
    nav_footer(2)


# ════════════════════════════════════════════════════════════════
# 3. WHERE
# ════════════════════════════════════════════════════════════════
elif idx == 3:
    st.markdown('<div class="sec">WHERE — 조건 필터</div>', unsafe_allow_html=True)
    st.markdown("`WHERE`는 FROM으로 불러온 데이터 중 조건을 만족하는 **행(Row)만** 남기는 필터입니다. 이 탭의 모든 예제는 **TA_COA311M_CDBSC** 테이블을 사용합니다.")
    st.markdown('''<div class="syn">SELECT 컬럼 FROM 테이블<br>WHERE  조건식;  ← TRUE인 행만 반환</div>''', unsafe_allow_html=True)

    exs = [
        ("예제 1 — 특정 코드그룹 조회",
         "COA0083(지역) 그룹의 코드만 조회합니다.",
         "SELECT cd_id, cd_nm, cd_expn_cont, del_yn\nFROM   TA_COA311M_CDBSC\nWHERE  cd_grp_id = 'RGR0015';", None, None),
        ("예제 2 — 삭제된 코드만 조회 (del_yn='Y')",
         "사용 중지된 코드 현황을 파악합니다.",
         "SELECT cd_grp_id, cd_id, cd_nm, valid_end_dy\nFROM   TA_COA311M_CDBSC\nWHERE  del_yn = 'Y';", None, None),
        ("예제 3 — AND 복합 조건",
         "COA0083 그룹이면서 유효(del_yn='N')한 코드만 조회합니다.",
         "SELECT cd_id, cd_nm, valid_strt_dy, valid_end_dy\nFROM   TA_COA311M_CDBSC\nWHERE  cd_grp_id = 'RGR0015'\n  AND  del_yn = 'N';", None, None),
        ("예제 4 — IN 목록 조건",
         "여러 코드그룹을 한 번에 조회합니다.",
         "SELECT cd_grp_id, cd_id, cd_nm\nFROM   TA_COA311M_CDBSC\nWHERE  cd_grp_id IN ('RGR0015', 'SEA0541')\nORDER BY cd_grp_id, cd_id;",
         "tip","IN 대신 서브쿼리도 가능합니다: <code>WHERE cd_grp_id IN (SELECT ...)</code>"),
        ("예제 5 — BETWEEN (유효시작일자 범위)",
         "유효시작일자(valid_strt_dy)가 2000년 이내인 코드만 조회합니다.",
         "SELECT cd_grp_id, cd_id, cd_nm, valid_strt_dy\nFROM   TA_COA311M_CDBSC\nWHERE  valid_strt_dy BETWEEN '20000101' AND '20001231';",
         "tip","YYYYMMDD 형식은 TEXT이지만 사전순 비교로 BETWEEN이 올바르게 동작합니다."),
        ("예제 6 — IS NULL (최솟값 없는 코드)",
         "최솟값(min_val)이 NULL인 코드를 찾습니다. COA0055 평가등급 그룹에 NULL 예시가 있습니다.",
         "SELECT cd_grp_id, cd_id, cd_nm, min_val, max_val\nFROM   TA_COA311M_CDBSC\nWHERE  min_val IS NULL;",
         "wrn","<code>= NULL</code>은 항상 FALSE. 반드시 <code>IS NULL</code>을 사용하세요."),
        ("예제 7 — COA0021 유효 코드만",
         "지급방식 코드 중 현재 유효한 것만 조회합니다.",
         "SELECT cd_id, cd_nm, cd_expn_cont\nFROM   TA_COA311M_CDBSC\nWHERE  cd_grp_id = 'SEA0541'\n  AND  del_yn = 'N'\nORDER BY cd_id;", None, None),
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
        "TA_COA311M_CDBSC에서 유효종료일자(valid_end_dy)가 '99991231'(무기한)이고 "
        "삭제여부(del_yn)='N'이며, 코드그룹ID(cd_grp_id)가 'RGR0015' 또는 'SEA0541'인 코드를 "
        "코드명(cd_nm) 오름차순으로 조회하세요. (코드그룹ID·코드ID·코드명·코드설명 포함)",
        """SELECT cd_grp_id, cd_id, cd_nm, cd_expn_cont
FROM   TA_COA311M_CDBSC
WHERE  valid_end_dy = '99991231'
  AND  del_yn = 'N'
  AND  cd_grp_id IN ('RGR0015', 'SEA0541')
ORDER BY cd_nm ASC;""",
        "p3", hint="valid_end_dy='99991231', del_yn='N', IN() 세 조건을 AND로 연결하세요.")
    nav_footer(3)


# ════════════════════════════════════════════════════════════════
# 4. LIKE
# ════════════════════════════════════════════════════════════════
elif idx == 4:
    st.markdown('<div class="sec">LIKE — 패턴 검색</div>', unsafe_allow_html=True)
    st.markdown("`LIKE`는 문자열 컬럼에서 특정 패턴에 맞는 값을 검색합니다. 이 탭의 모든 예제는 **TA_COA311M_CDBSC** 테이블을 사용합니다.")
    c1, c2 = st.columns(2)
    with c1:
        st.markdown('''<div class="syn">WHERE 컬럼 LIKE '패턴'<br><br>%  : 0개 이상의 임의 문자<br>_  : 정확히 1개의 임의 문자</div>''', unsafe_allow_html=True)
    with c2:
        st.markdown("""
| 패턴 | 의미 | 예시 |
|------|------|------|
| `'서%'` | 서로 시작 | 서울특별시 ✅ |
| `'%광역%'` | 광역 포함 | 광주광역시 ✅ |
| `'%도'` | 도로 끝남 | 경기도 ✅ |
| `'__'` | 정확히 2글자 | 11, 41 ✅ |
        """)

    exs = [
        ("예제 1 — 코드명 앞 고정 검색",
         "코드명(cd_nm)이 '서'로 시작하는 지역 코드를 조회합니다.",
         "SELECT cd_id, cd_nm, cd_expn_cont\nFROM   TA_COA311M_CDBSC\nWHERE  cd_grp_id = 'RGR0015' AND cd_nm LIKE '서%';",
         "tip","<code>LIKE '값%'</code>(앞 고정) → 인덱스 사용 가능 / <code>LIKE '%값%'</code> → Full Scan"),
        ("예제 2 — 코드명 중간 포함 검색",
         "코드명에 '광역'이 포함된 지역을 찾습니다.",
         "SELECT cd_id, cd_nm, cd_expn_cont\nFROM   TA_COA311M_CDBSC\nWHERE  cd_nm LIKE '%광역%' AND del_yn = 'N';",
         "wrn","<code>LIKE '%값%'</code>는 Full Table Scan. 대용량 테이블에서는 주의하세요."),
        ("예제 3 — 코드설명 검색",
         "코드설명(cd_expn_cont)에 '보증'이 포함된 코드를 찾습니다.",
         "SELECT cd_grp_id, cd_id, cd_nm, cd_expn_cont\nFROM   TA_COA311M_CDBSC\nWHERE  cd_expn_cont LIKE '%보증%' AND del_yn = 'N';", None, None),
        ("예제 4 — _ 와일드카드 (글자 수 고정)",
         "코드ID(cd_id)가 정확히 2자리인 코드를 조회합니다.",
         "SELECT cd_grp_id, cd_id, cd_nm\nFROM   TA_COA311M_CDBSC\nWHERE  cd_id LIKE '__'\nORDER BY cd_grp_id, cd_id;", None, None),
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
        "TA_COA311M_CDBSC에서 코드그룹ID(cd_grp_id)='BMC0002'이고 삭제여부(del_yn)='N'인 코드 중 "
        "코드설명(cd_expn_cont)에 '등급'이 포함된 코드를 조회하세요. "
        "(코드ID·코드명·코드설명·코드값 포함, 코드값 내림차순)",
        """SELECT cd_id, cd_nm, cd_expn_cont, cd_val
FROM   TA_COA311M_CDBSC
WHERE  cd_grp_id = 'BMC0002'
  AND  del_yn = 'N'
  AND  cd_expn_cont LIKE '%등급%'
ORDER BY cd_val DESC;""",
        "p4", hint="cd_expn_cont LIKE '%등급%' 조건을 AND로 추가하세요.")
    nav_footer(4)


# ════════════════════════════════════════════════════════════════
# 5. ORDER BY
# ════════════════════════════════════════════════════════════════
elif idx == 5:
    st.markdown('<div class="sec">ORDER BY — 정렬</div>', unsafe_allow_html=True)
    st.markdown("`ORDER BY`는 결과를 원하는 기준으로 정렬합니다. SQL 실행 순서상 **가장 마지막**에 적용되므로 SELECT 별칭을 사용할 수 있습니다. 이 탭의 모든 예제는 **TA_COA311M_CDBSC** 테이블을 사용합니다.")
    st.markdown('''<div class="syn">ORDER BY 컬럼1 [ASC|DESC], 컬럼2 [ASC|DESC]<br><br>ASC : 오름차순 (기본값, 생략 가능)<br>DESC: 내림차순</div>''', unsafe_allow_html=True)

    exs = [
        ("예제 1 — 코드명 오름차순",
         "COA0083 지역 코드를 코드명(cd_nm) 오름차순으로 정렬합니다.",
         "SELECT cd_id, cd_nm, cd_expn_cont\nFROM   TA_COA311M_CDBSC\nWHERE  cd_grp_id = 'RGR0015' AND del_yn = 'N'\nORDER BY cd_nm ASC;"),
        ("예제 2 — 다중 정렬 (그룹↑ + 코드ID↑)",
         "코드그룹 오름차순, 같은 그룹 안에서는 코드ID 오름차순으로 정렬합니다.",
         "SELECT cd_grp_id, cd_id, cd_nm\nFROM   TA_COA311M_CDBSC\nWHERE  del_yn = 'N'\nORDER BY cd_grp_id ASC, cd_id ASC;"),
        ("예제 3 — SELECT 별칭으로 정렬",
         "집계 결과에 한글 별칭을 붙이고 ORDER BY에서 사용합니다.",
         "SELECT cd_grp_id, COUNT(*) AS 코드수\nFROM   TA_COA311M_CDBSC\nWHERE  del_yn = 'N'\nGROUP BY cd_grp_id\nORDER BY 코드수 DESC;"),
        ("예제 4 — cd_val(코드값) 내림차순",
         "COA0055 평가등급 그룹을 코드값(cd_val) 내림차순으로 정렬합니다.",
         "SELECT cd_id, cd_nm, cd_val\nFROM   TA_COA311M_CDBSC\nWHERE  cd_grp_id = 'BMC0002' AND del_yn = 'N'\nORDER BY cd_val DESC;"),
    ]
    for title, desc, sql in exs:
        with st.expander(title, expanded=True):
            st.caption(desc)
            st.code(sql, language="sql")
            _result(sql, f"s5_{title}")

    st.divider()
    _practice(
        "TA_COA311M_CDBSC에서 삭제여부(del_yn)='N'인 전체 유효 코드를 "
        "코드그룹ID(cd_grp_id) 오름차순, 같은 그룹 내에서는 코드명(cd_nm) 오름차순으로 정렬하세요. "
        "코드그룹ID·코드ID·코드명·유효시작일자(valid_strt_dy)를 포함하여 조회하세요.",
        """SELECT cd_grp_id, cd_id, cd_nm, valid_strt_dy
FROM   TA_COA311M_CDBSC
WHERE  del_yn = 'N'
ORDER BY cd_grp_id ASC, cd_nm ASC;""",
        "p5", hint="ORDER BY에 cd_grp_id ASC, cd_nm ASC를 차례로 작성하세요.")
    nav_footer(5)


# ════════════════════════════════════════════════════════════════
# 6. GROUP BY
# ════════════════════════════════════════════════════════════════
elif idx == 6:
    st.markdown('<div class="sec">GROUP BY — 집계</div>', unsafe_allow_html=True)
    st.markdown("`GROUP BY`는 특정 컬럼 값이 같은 행들을 그룹으로 묶고 집계합니다. 이 탭의 모든 예제는 **TA_COA311M_CDBSC** 테이블을 사용합니다.")
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
        st.markdown('''<div class="wrn"><b>GROUP BY 규칙</b><br>SELECT의 비집계 컬럼은 반드시 GROUP BY에 포함해야 합니다.<br><br>✅ <code>SELECT cd_grp_id, del_yn, COUNT(*) ... GROUP BY cd_grp_id, del_yn</code><br>❌ <code>GROUP BY cd_grp_id</code> ← del_yn 누락!</div>''', unsafe_allow_html=True)

    exs = [
        ("예제 1 — 코드그룹별 전체 코드 수",
         "각 코드그룹에 몇 개의 코드가 있는지 집계합니다.",
         """SELECT
    cd_grp_id        AS 코드그룹ID,
    COUNT(*)         AS 전체코드수,
    COUNT(cd_val)    AS "코드값있는수"
FROM   TA_COA311M_CDBSC
GROUP BY cd_grp_id
ORDER BY 전체코드수 DESC;"""),
        ("예제 2 — 그룹+삭제여부별 집계",
         "코드그룹과 삭제여부를 함께 그룹화하여 유효/삭제 코드 수를 비교합니다.",
         """SELECT
    cd_grp_id    AS 코드그룹ID,
    del_yn       AS 삭제여부,
    COUNT(*)     AS 코드수
FROM   TA_COA311M_CDBSC
GROUP BY cd_grp_id, del_yn
ORDER BY cd_grp_id, del_yn;"""),
        ("예제 3 — HAVING으로 집계 필터",
         "코드가 3개 이상인 그룹만 조회합니다.",
         """SELECT
    cd_grp_id    AS 코드그룹ID,
    COUNT(*)     AS 전체코드수
FROM   TA_COA311M_CDBSC
GROUP BY cd_grp_id
HAVING COUNT(*) >= 3
ORDER BY 전체코드수 DESC;"""),
        ("예제 4 — WHERE + GROUP BY + HAVING",
         "유효(del_yn='N') 코드 중 그룹별 코드 수가 2개 이상인 그룹만 조회합니다.",
         """SELECT
    cd_grp_id        AS 코드그룹ID,
    COUNT(*)         AS 유효코드수
FROM   TA_COA311M_CDBSC
WHERE  del_yn = 'N'
GROUP BY cd_grp_id
HAVING COUNT(*) >= 2
ORDER BY 유효코드수 DESC;"""),
    ]
    for title, desc, sql in exs:
        with st.expander(title, expanded=True):
            st.caption(desc)
            st.code(sql, language="sql")
            _result(sql, f"s6_{title}")

    st.divider()
    _practice(
        "TA_COA311M_CDBSC에서 코드그룹ID(cd_grp_id)별로 전체 코드 수와 유효 코드 수(del_yn='N')를 집계하고, "
        "유효 코드가 2개 이상인 그룹만 전체 코드 수 내림차순으로 조회하세요.",
        """SELECT
    cd_grp_id                                          AS 코드그룹ID,
    COUNT(*)                                            AS 전체코드수,
    SUM(CASE WHEN del_yn = 'N' THEN 1 ELSE 0 END)      AS 유효코드수
FROM   TA_COA311M_CDBSC
GROUP BY cd_grp_id
HAVING SUM(CASE WHEN del_yn = 'N' THEN 1 ELSE 0 END) >= 2
ORDER BY 전체코드수 DESC;""",
        "p6", hint="HAVING에서 CASE WHEN을 이용해 유효코드만 카운트하거나, SUM()으로 계산하세요.")
    nav_footer(6)


# ════════════════════════════════════════════════════════════════
# 7. JOIN
# ════════════════════════════════════════════════════════════════
elif idx == 7:
    st.markdown('<div class="sec">JOIN — 테이블 결합</div>', unsafe_allow_html=True)
    st.markdown("""
`JOIN`은 공통 컬럼(키)을 기준으로 두 테이블을 결합합니다.
`TB_RGR011M_HSPRC`에는 심사평가방법코드(`aprs_eval_mthd_cd`)만 있고 코드명은 없습니다.
코드명을 보려면 `TA_COA311M_CDBSC`를 JOIN해야 합니다.
    """)
    st.markdown('''<div class="tip"><b>JOIN 키 구조</b><br>
TB_RGR011M_HSPRC.<code>aprs_eval_mthd_cd</code> = TA_COA311M_CDBSC.<code>cd_id</code><br>
실제 데이터: aprs_eval_mthd_cd = <code>'01'</code> ↔ COA311M cd_id = <code>'01'</code> (HTA0018 그룹)<br>
AND TA_COA311M_CDBSC.<code>del_yn = 'N'</code> (유효 코드만)
</div>''', unsafe_allow_html=True)
    st.markdown('''<div class="syn">SELECT a.컬럼, b.컬럼<br>FROM   테이블A a<br>[JOIN종류] 테이블B b ON a.키 = b.키 [AND 추가조건];<br><br>⚠️ ON 조건 누락 시 카테시안 곱 발생!</div>''', unsafe_allow_html=True)

    st.divider()
    t_inn, t_lft, t_typ = st.tabs(["INNER JOIN", "LEFT JOIN", "JOIN 종류 비교"])

    with t_inn:
        c1, c2 = st.columns([1, 1])
        with c1:
            st.markdown("**개념**: ON 조건이 일치하는 행만 반환합니다.")
            st.code("""
RGR011M (aprs_eval_mthd_cd)   COA311M (cd_id, COA0099)
┌──────────────────────┐       ┌──────────────┐
│ aprs_eval_mthd_cd='01' │──▶│ cd_id='M01'  │ ✅ 포함
│ aprs_eval_mthd_cd='M02' │──▶│ cd_id='M02'  │ ✅ 포함
│ aprs_eval_mthd_cd='M04' │ ✗ │ del_yn='Y'   │ ❌ 제외
└──────────────────────┘       └──────────────┘
""", language="text")
            st.markdown("**사용**: 코드가 반드시 마스터에 있을 때")
        with c2:
            sql = """-- 심사평가방법명 JOIN
-- aprs_eval_mthd_cd('01') = COA311M.cd_id('01'), cd_grp_id='HTA0018'
SELECT
    R.grnt_no             AS 보증번호,
    R.basis_ym            AS 기준연월,
    C.cd_nm               AS 심사평가방법명,
    R.kab_trd_avg_prc     AS "KAB평균가격(억)",
    R.kb_trd_avg_prc      AS "KB평균가격(억)"
FROM TB_RGR011M_HSPRC R
INNER JOIN TA_COA311M_CDBSC C
    ON  R.aprs_eval_mthd_cd = C.cd_id
    AND C.cd_grp_id = 'HTA0018'
    AND C.del_yn    = 'N'
ORDER BY R.basis_ym, R.kab_trd_avg_prc DESC;"""
            st.code(sql, language="sql")
            _result(sql, "j_inner")

    with t_lft:
        c1, c2 = st.columns([1, 1])
        with c1:
            st.markdown("**개념**: 왼쪽 테이블 전체 유지, 오른쪽 매칭 없으면 NULL.")
            st.code("""
RGR011M (왼쪽)         COA311M (오른쪽)
┌──────────────────┐   ┌────────────────────────┐
│ aprs_eval='01'   │──▶│ cd_id='01', del_yn='N' │ ✅ 포함
│ (매칭 없는 행)   │──▶│ (없음)                 │ ✅ NULL로 포함
└──────────────────┘   └────────────────────────┘
""", language="text")
            st.markdown("**사용**: 코드 누락·삭제 데이터도 포함해 전수 확인할 때")
        with c2:
            sql = """SELECT
    R.grnt_no                          AS 보증번호,
    R.aprs_eval_mthd_cd                AS 심사평가방법코드,
    COALESCE(C.cd_nm, '코드없음')      AS 심사평가방법명,
    R.kab_trd_avg_prc                  AS "KAB평균가격(억)"
FROM TB_RGR011M_HSPRC R
LEFT JOIN TA_COA311M_CDBSC C
    ON  R.aprs_eval_mthd_cd = C.cd_id
    AND C.cd_grp_id = 'HTA0018'
    AND C.del_yn    = 'N'
ORDER BY R.basis_ym, R.grnt_no;"""
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
        st.markdown('''<div class="wrn"><b>COA311M JOIN 시 주의사항</b><br>
1. <code>cd_grp_id = 'HTA0018'</code> 조건 필수 — 없으면 여러 그룹과 중복 조인됨<br>
2. <code>del_yn = 'N'</code> 조건 추가 — 삭제된 코드 제외<br>
3. ON 조건에 두 조건을 AND로 함께 작성</div>''', unsafe_allow_html=True)

        sql = """-- 기준연월별 시세 현황 + 심사평가방법명 JOIN
SELECT
    R.basis_ym                       AS 기준연월,
    C.cd_nm                          AS 심사평가방법명,
    COUNT(*)                         AS 건수,
    ROUND(AVG(R.kab_trd_avg_prc), 3) AS "KAB평균(억)",
    ROUND(AVG(R.kb_trd_avg_prc),  3) AS "KB평균(억)"
FROM TB_RGR011M_HSPRC R
JOIN TA_COA311M_CDBSC C
    ON  R.aprs_eval_mthd_cd = C.cd_id
    AND C.cd_grp_id = 'HTA0018'
    AND C.del_yn    = 'N'
GROUP BY R.basis_ym, C.cd_nm
ORDER BY R.basis_ym DESC;"""
        with st.expander("예제 — 기준연월·심사평가방법별 시세 현황", expanded=True):
            st.code(sql, language="sql")
            _result(sql, "j_agg")

    st.divider()
    _practice(
        "TA_COA311M_CDBSC(COA0099 그룹)와 TB_RGR011M_HSPRC를 INNER JOIN하여 "
        "보증번호(grnt_no)·기준연월(basis_ym)·심사평가방법명(cd_nm)·"
        "KAB매매평균가격(kab_trd_avg_prc)·KB매매평균가격(kb_trd_avg_prc)을 조회하세요. "
        "유효(del_yn='N') 코드만 사용하고, KAB매매평균가격 내림차순으로 정렬하세요.",
        """SELECT
    R.grnt_no             AS 보증번호,
    R.basis_ym            AS 기준연월,
    C.cd_nm               AS 심사평가방법명,
    R.kab_trd_avg_prc     AS "KAB평균가격(억)",
    R.kb_trd_avg_prc      AS "KB평균가격(억)"
FROM TB_RGR011M_HSPRC R
JOIN TA_COA311M_CDBSC C
    ON  R.aprs_eval_mthd_cd = C.cd_id
    AND C.cd_grp_id = 'HTA0018'
    AND C.del_yn    = 'N'
ORDER BY R.kab_trd_avg_prc DESC;""",
        "p7", hint="ON 조건에 aprs_eval_mthd_cd = cd_id AND cd_grp_id='HTA0018' AND del_yn='N'을 작성하세요.")
    nav_footer(7)


# ════════════════════════════════════════════════════════════════
# 8. ANSI 함수
# ════════════════════════════════════════════════════════════════
elif idx == 8:
    st.markdown('<div class="sec">ANSI 함수 — ROUND · COALESCE · CASE WHEN · NULLIF</div>', unsafe_allow_html=True)
    st.markdown("ANSI SQL 표준 함수들은 Oracle·MS-SQL·Trino 등 거의 모든 DB에서 동일하게 동작합니다. 이 탭의 모든 예제는 **TA_COA311M_CDBSC** 테이블을 사용합니다.")

    t1, t2, t3, t4 = st.tabs(["ROUND & 수치함수", "COALESCE & NULLIF", "CASE WHEN", "복합 활용"])

    with t1:
        st.markdown("### ROUND — 반올림")
        st.markdown('''<div class="syn">ROUND(값, 소수점자리)<br>ROUND(4.567, 1)  →  4.6<br>ROUND(4.567, 0)  →  5<br>ROUND(1234, -2)  →  1200  (음수: 정수 자리)</div>''', unsafe_allow_html=True)
        sql_r = """SELECT
    cd_id                          AS 코드ID,
    cd_nm                          AS 코드명,
    cd_val                         AS "코드값(원본)",
    ROUND(cd_val, 0)               AS "코드값(정수)",
    ROUND(cd_val, 1)               AS "코드값(1자리)",
    ROUND(cd_val * 2, 1)           AS "코드값(2배,1자리)"
FROM TA_COA311M_CDBSC
WHERE cd_grp_id = 'BMC0002'
ORDER BY cd_val DESC;"""
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
    cd_id, cd_nm,
    cd_val                         AS 코드값,
    ABS(cd_val - 3.0)              AS "3.0과의 차이(절댓값)",
    CEIL(cd_val)                   AS "올림",
    FLOOR(cd_val)                  AS "내림"
FROM TA_COA311M_CDBSC
WHERE cd_grp_id = 'BMC0002';"""
        st.code(sql_num, language="sql")
        _result(sql_num, "fn_num")

    with t2:
        st.markdown("### COALESCE — NULL 대체")
        st.markdown('''<div class="syn">COALESCE(값1, 값2, 값3, …)<br>← 왼쪽부터 순서대로 검사해 첫 번째 non-NULL 값을 반환합니다.<br><br>COALESCE(NULL, NULL, '기본값')  →  '기본값'</div>''', unsafe_allow_html=True)
        sql_coal = """SELECT
    cd_grp_id                       AS 코드그룹ID,
    cd_id                           AS 코드ID,
    cd_nm                           AS 코드명,
    min_val                         AS "최솟값(원본)",
    max_val                         AS "최댓값(원본)",
    COALESCE(min_val, '정보없음')   AS "최솟값(NULL→정보없음)",
    COALESCE(max_val, '정보없음')   AS "최댓값(NULL→정보없음)"
FROM TA_COA311M_CDBSC
WHERE cd_grp_id = 'BMC0002'
ORDER BY cd_val DESC;"""
        st.markdown('''<div class="tip">실무 패턴: NULL인 경우 기본값으로 대체해 집계·표시 오류를 방지합니다.</div>''', unsafe_allow_html=True)
        st.code(sql_coal, language="sql")
        _result(sql_coal, "fn_coal")

        st.divider()
        st.markdown("### NULLIF — 조건부 NULL 변환")
        st.markdown('''<div class="syn">NULLIF(값1, 값2)<br>← 값1 = 값2 이면 NULL, 아니면 값1 반환<br>주로 0 나누기 방지에 사용합니다.</div>''', unsafe_allow_html=True)
        sql_ni = """SELECT
    cd_grp_id,
    COUNT(*)                                            AS 전체코드수,
    COUNT(CASE WHEN del_yn = 'N' THEN 1 END)           AS 유효코드수,
    ROUND(100.0 * COUNT(CASE WHEN del_yn = 'N' THEN 1 END)
          / NULLIF(COUNT(*), 0), 1)                     AS "유효율(%)"
FROM TA_COA311M_CDBSC
GROUP BY cd_grp_id
ORDER BY cd_grp_id;"""
        st.code(sql_ni, language="sql")
        _result(sql_ni, "fn_nullif")

    with t3:
        st.markdown("### CASE WHEN — 조건 분기")
        st.markdown('''<div class="syn">-- 검색형<br>CASE WHEN 조건1 THEN 값1<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;WHEN 조건2 THEN 값2<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;ELSE 기본값<br>END<br><br>-- 단순형<br>CASE 컬럼 WHEN '값1' THEN 결과1<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;WHEN '값2' THEN 결과2<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;ELSE 기본값 END</div>''', unsafe_allow_html=True)

        sql_cw1 = """-- 단순형: del_yn → 한글 상태 변환
SELECT
    cd_grp_id,
    cd_id,
    cd_nm,
    CASE del_yn
        WHEN 'N' THEN '유효'
        WHEN 'Y' THEN '삭제'
        ELSE '미정'
    END               AS 상태
FROM TA_COA311M_CDBSC
ORDER BY cd_grp_id, del_yn;"""
        with st.expander("예제 1 — del_yn → 한글 상태 변환 (단순형)", expanded=True):
            st.code(sql_cw1, language="sql")
            _result(sql_cw1, "fn_cw1")

        sql_cw2 = """-- 검색형: 코드값(cd_val) 구간 분류
SELECT
    cd_id,
    cd_nm,
    cd_val,
    CASE
        WHEN cd_val >= 4.0 THEN '최상위 (4.0+)'
        WHEN cd_val >= 3.0 THEN '상위 (3.0~4.0)'
        WHEN cd_val >= 2.0 THEN '중위 (2.0~3.0)'
        ELSE                    '하위 (2.0 미만)'
    END AS 등급구분
FROM TA_COA311M_CDBSC
WHERE cd_grp_id = 'BMC0002'
ORDER BY cd_val DESC;"""
        with st.expander("예제 2 — 코드값 구간 분류 (검색형)", expanded=True):
            st.code(sql_cw2, language="sql")
            _result(sql_cw2, "fn_cw2")

        sql_cw3 = """-- CASE WHEN + GROUP BY : 코드그룹별 유효/삭제 피벗
SELECT
    cd_grp_id                                                AS 코드그룹ID,
    SUM(CASE WHEN del_yn = 'N' THEN 1 ELSE 0 END)           AS 유효,
    SUM(CASE WHEN del_yn = 'Y' THEN 1 ELSE 0 END)           AS 삭제,
    COUNT(*)                                                  AS 전체
FROM TA_COA311M_CDBSC
GROUP BY cd_grp_id
ORDER BY 전체 DESC;"""
        with st.expander("예제 3 — 유효/삭제 피벗 집계", expanded=True):
            st.code(sql_cw3, language="sql")
            _result(sql_cw3, "fn_cw3")

    with t4:
        st.markdown("### 복합 함수 활용 예제")
        sql_comp = """-- COALESCE + ROUND + CASE WHEN 조합
SELECT
    cd_grp_id                                AS 코드그룹ID,
    cd_id                                    AS 코드ID,
    cd_nm                                    AS 코드명,
    CASE del_yn WHEN 'N' THEN '유효' ELSE '삭제' END AS 상태,
    ROUND(cd_val, 1)                         AS "코드값(1자리)",
    COALESCE(min_val, '없음')               AS 최솟값,
    COALESCE(max_val, '없음')               AS 최댓값,
    CASE
        WHEN cd_val >= 4.0 THEN '최상위'
        WHEN cd_val >= 3.0 THEN '상위'
        WHEN cd_val >= 2.0 THEN '중위'
        WHEN cd_val IS NULL THEN '-'
        ELSE '하위'
    END                                      AS 등급구분
FROM TA_COA311M_CDBSC
ORDER BY cd_grp_id, cd_val DESC;"""
        st.code(sql_comp, language="sql")
        _result(sql_comp, "fn_comp")

    st.divider()
    _practice(
        "TA_COA311M_CDBSC에서 코드그룹ID(cd_grp_id)·코드ID(cd_id)·코드명(cd_nm)을 조회하되, "
        "삭제여부(del_yn)를 CASE WHEN으로 '유효'/'삭제'로 변환하고, "
        "최솟값(min_val)이 NULL인 경우 '정보없음'으로 대체(COALESCE)하여 표시하세요. "
        "코드그룹ID 오름차순, 코드값(cd_val) 내림차순으로 정렬하세요.",
        """SELECT
    cd_grp_id                                                  AS 코드그룹ID,
    cd_id                                                      AS 코드ID,
    cd_nm                                                      AS 코드명,
    CASE del_yn WHEN 'N' THEN '유효' WHEN 'Y' THEN '삭제' ELSE '미정' END AS 상태,
    COALESCE(min_val, '정보없음')                              AS 최솟값
FROM TA_COA311M_CDBSC
ORDER BY cd_grp_id ASC, cd_val DESC;""",
        "p8", hint="CASE del_yn WHEN ... END와 COALESCE(min_val, '정보없음')을 조합하세요.")
    nav_footer(8)


# ════════════════════════════════════════════════════════════════
# 9. 복합 쿼리 실전
# ════════════════════════════════════════════════════════════════
elif idx == 9:
    st.markdown('<div class="sec">복합 쿼리 실전</div>', unsafe_allow_html=True)
    st.markdown("지금까지 배운 모든 키워드를 **TA_COA311M_CDBSC** 기반으로 조합합니다. **직접 작성 후 정답과 비교**하세요.")
    st.markdown('''<div class="tip">풀이 순서: 1) 필요 컬럼 파악 → 2) WHERE 조건 → 3) GROUP BY/HAVING → 4) SELECT 컬럼 & CASE/COALESCE → 5) ORDER BY</div>''', unsafe_allow_html=True)
    st.divider()

    _practice(
        "실전 1 — 코드그룹별 유효/삭제 현황 + 유효율\n\n"
        "코드그룹ID(cd_grp_id)별로 전체 코드 수, 유효 코드 수(del_yn='N'), 삭제 코드 수(del_yn='Y'), "
        "유효율(%)을 구하세요. 유효율은 ROUND(유효코드수/전체코드수*100, 1)로 계산하고 NULLIF로 0 나누기를 방지하세요. "
        "전체 코드 수 내림차순으로 정렬하세요.",
        """SELECT
    cd_grp_id                                                          AS 코드그룹ID,
    COUNT(*)                                                            AS 전체코드수,
    SUM(CASE WHEN del_yn = 'N' THEN 1 ELSE 0 END)                      AS 유효코드수,
    SUM(CASE WHEN del_yn = 'Y' THEN 1 ELSE 0 END)                      AS 삭제코드수,
    ROUND(100.0 * SUM(CASE WHEN del_yn = 'N' THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 1)                                     AS "유효율(%)"
FROM TA_COA311M_CDBSC
GROUP BY cd_grp_id
ORDER BY 전체코드수 DESC;""",
        "adv1", hint="CASE WHEN + SUM 피벗, NULLIF(COUNT(*), 0)으로 0 나누기 방지, ROUND 조합.")

    st.divider()
    _practice(
        "실전 2 — 유효기간 기반 필터 + CASE WHEN 분류\n\n"
        "삭제여부(del_yn)='N'인 유효 코드 중 유효종료일자(valid_end_dy)를 기준으로 "
        "'99991231'이면 '무기한', 그 외면 '기한있음'으로 구분하세요. "
        "구분별 코드 수를 집계하고, 코드그룹ID(cd_grp_id)도 함께 표시하세요.",
        """SELECT
    cd_grp_id                                              AS 코드그룹ID,
    CASE valid_end_dy
        WHEN '99991231' THEN '무기한'
        ELSE '기한있음'
    END                                                    AS 유효기간구분,
    COUNT(*)                                               AS 코드수
FROM TA_COA311M_CDBSC
WHERE del_yn = 'N'
GROUP BY cd_grp_id, valid_end_dy
ORDER BY cd_grp_id, 유효기간구분;""",
        "adv2", hint="CASE valid_end_dy WHEN '99991231' THEN '무기한' ELSE '기한있음' END.")

    st.divider()
    _practice(
        "실전 3 — 평균 이상 코드값 조회 (서브쿼리)\n\n"
        "COA0055 평가등급 그룹의 유효(del_yn='N') 코드 중 코드값(cd_val)이 "
        "전체 COA0055 그룹 평균 코드값 이상인 코드만 조회하세요. "
        "코드ID·코드명·코드값·최솟값(NULL이면 '없음')을 표시하세요.",
        """SELECT
    cd_id                          AS 코드ID,
    cd_nm                          AS 코드명,
    cd_val                         AS 코드값,
    COALESCE(min_val, '없음')      AS 최솟값
FROM TA_COA311M_CDBSC
WHERE cd_grp_id = 'BMC0002'
  AND del_yn = 'N'
  AND cd_val >= (
      SELECT AVG(cd_val)
      FROM TA_COA311M_CDBSC
      WHERE cd_grp_id = 'BMC0002' AND cd_val IS NOT NULL
  )
ORDER BY cd_val DESC;""",
        "adv3", hint="HAVING 대신 WHERE에서 서브쿼리 (SELECT AVG(cd_val) FROM ...) 사용하세요.")
    nav_footer(9)


# ════════════════════════════════════════════════════════════════
# 10. 자유 실습 & 연습과제
# ════════════════════════════════════════════════════════════════
elif idx == 10:
    st.markdown('<div class="sec">자유 실습 & 연습과제</div>', unsafe_allow_html=True)

    t_free, t_quiz = st.tabs(["자유 실습", "연습과제 (정답 포함)"])

    with t_free:
        c1, c2 = st.columns(2)
        with c1:
            st.markdown("**주요 컬럼 참조**")
            st.markdown("""
**TA_COA311M_CDBSC**
- `cd_ung_no` : 고유번호
- `cd_grp_id` : 코드그룹ID (RGR0015/SEA0541/HTA0018/BMC0002)
- `cd_id`, `cd_nm` : 코드ID, 코드명
- `up_cd_grp_id`, `up_cd_id` : 상위코드 정보
- `valid_strt_dy`, `valid_end_dy` : 유효기간 (YYYYMMDD)
- `cd_expn_cont` : 코드설명
- `cd_val` : 코드값 (REAL, NULL 가능)
- `min_val`, `max_val` : 최솟/최댓값 (NULL 가능)
- `del_yn` : 삭제여부 (N=유효, Y=삭제)

**TB_RGR011M_HSPRC** (JOIN 탭에서 주로 사용)
- `basis_ym`, `grnt_no`, `gmt_no` : 기준연월·보증번호·상품번호
- `aprs_eval_mthd_cd` → COA311M.cd_id (COA0099) JOIN 키
- `kab_trd_bttrn/top/avg_prc` : KAB 하한/상한/평균가격 (억원)
- `kb_trd_top/avg/bttrn_prc` : KB 상한/평균/하한가격 (억원)
            """)
        with c2:
            st.markdown("**빠른 참조 쿼리**")
            st.code("""-- COA311M 유효 코드 전체 조회
SELECT cd_grp_id, cd_id, cd_nm, del_yn
FROM TA_COA311M_CDBSC
WHERE del_yn = 'N'
ORDER BY cd_grp_id, cd_id;

-- COA0055 평가등급 (cd_val 보유)
SELECT cd_id, cd_nm, cd_val, min_val, max_val
FROM TA_COA311M_CDBSC
WHERE cd_grp_id = 'BMC0002';

-- JOIN 기본 패턴
FROM TB_RGR011M_HSPRC R
JOIN TA_COA311M_CDBSC C
  ON  R.aprs_eval_mthd_cd = C.cd_id
  AND C.cd_grp_id = 'HTA0018'
  AND C.del_yn = 'N'""", language="sql")

        presets = [
            "직접 입력",
            "SELECT cd_grp_id, cd_id, cd_nm, del_yn FROM TA_COA311M_CDBSC WHERE del_yn = 'N' ORDER BY cd_grp_id, cd_id",
            "SELECT DISTINCT cd_grp_id FROM TA_COA311M_CDBSC ORDER BY cd_grp_id",
            "SELECT cd_grp_id, COUNT(*) AS 전체, SUM(CASE WHEN del_yn='N' THEN 1 ELSE 0 END) AS 유효 FROM TA_COA311M_CDBSC GROUP BY cd_grp_id",
            "SELECT R.grnt_no, C.cd_nm, R.kab_trd_avg_prc FROM TB_RGR011M_HSPRC R JOIN TA_COA311M_CDBSC C ON R.aprs_eval_mthd_cd = C.cd_id AND C.cd_grp_id='HTA0018' AND C.del_yn='N'",
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
             "TA_COA311M_CDBSC에서 코드그룹ID(cd_grp_id)='RGR0015'이고 삭제여부(del_yn)='N'인 코드를 "
             "코드명(cd_nm) 오름차순으로 조회하세요. (코드ID·코드명·코드설명·유효시작일자 포함)",
             "WHERE cd_grp_id='RGR0015' AND del_yn='N' 후 ORDER BY cd_nm ASC",
             """SELECT cd_id, cd_nm, cd_expn_cont, valid_strt_dy
FROM   TA_COA311M_CDBSC
WHERE  cd_grp_id = 'RGR0015'
  AND  del_yn = 'N'
ORDER BY cd_nm ASC;"""),
            ("2", "🟢 기초",
             "TA_COA311M_CDBSC에서 삭제여부(del_yn)='N'인 유효 코드 중 코드설명(cd_expn_cont)에 "
             "'광역'이 포함된 지역만 조회하세요.",
             "del_yn='N' AND cd_expn_cont LIKE '%광역%'",
             """SELECT cd_id, cd_nm, cd_expn_cont
FROM   TA_COA311M_CDBSC
WHERE  del_yn = 'N'
  AND  cd_expn_cont LIKE '%광역%'
ORDER BY cd_id;"""),
            ("3", "🟡 중급",
             "코드그룹ID(cd_grp_id)별로 전체 코드 수와 유효 코드 수를 집계하고, "
             "삭제여부(del_yn)를 CASE WHEN으로 '유효'/'삭제'로 변환한 상태별 코드 수도 함께 표시하세요. "
             "전체 코드 수 내림차순 정렬.",
             "GROUP BY cd_grp_id 후 CASE WHEN 피벗 + COUNT",
             """SELECT
    cd_grp_id                                              AS 코드그룹ID,
    COUNT(*)                                                AS 전체코드수,
    SUM(CASE WHEN del_yn = 'N' THEN 1 ELSE 0 END)          AS 유효코드수,
    SUM(CASE WHEN del_yn = 'Y' THEN 1 ELSE 0 END)          AS 삭제코드수
FROM   TA_COA311M_CDBSC
GROUP BY cd_grp_id
ORDER BY 전체코드수 DESC;"""),
            ("4", "🟡 중급",
             "COA311M(COA0099 그룹)과 RGR011M을 JOIN하여 "
             "기준연월(basis_ym)='202306'인 데이터의 보증번호(grnt_no)·심사평가방법명(cd_nm)·"
             "KAB매매평균가격(kab_trd_avg_prc)·KB매매평균가격(kb_trd_avg_prc)을 조회하세요. "
             "KAB매매평균가격 내림차순 정렬.",
             "INNER JOIN + WHERE basis_ym='202306'",
             """SELECT
    R.grnt_no              AS 보증번호,
    C.cd_nm                AS 심사평가방법명,
    R.kab_trd_avg_prc      AS "KAB평균가격(억)",
    R.kb_trd_avg_prc       AS "KB평균가격(억)"
FROM TB_RGR011M_HSPRC R
JOIN TA_COA311M_CDBSC C
    ON  R.aprs_eval_mthd_cd = C.cd_id
    AND C.cd_grp_id = 'HTA0018'
    AND C.del_yn    = 'N'
WHERE  R.basis_ym = '202306'
ORDER BY R.kab_trd_avg_prc DESC;"""),
            ("5", "🔴 심화",
             "COA0055 평가등급 그룹 전체(유효+삭제 모두)의 코드값(cd_val) 현황을 분석하세요:\n"
             "코드ID·코드명·코드값·상태(유효/삭제)·등급구분(코드값 기준 최상위/상위/중위/하위)·"
             "최솟값(NULL이면 '정보없음')을 표시하고, 유효율(%)도 하단에 집계하세요.",
             "CASE WHEN (2개) + COALESCE + 서브쿼리 조합",
             """-- 코드별 상세
SELECT
    cd_id,
    cd_nm,
    cd_val                             AS 코드값,
    CASE del_yn WHEN 'N' THEN '유효' ELSE '삭제' END AS 상태,
    CASE
        WHEN cd_val >= 4.0 THEN '최상위'
        WHEN cd_val >= 3.0 THEN '상위'
        WHEN cd_val >= 2.0 THEN '중위'
        ELSE '하위'
    END                                AS 등급구분,
    COALESCE(min_val, '정보없음')      AS 최솟값
FROM TA_COA311M_CDBSC
WHERE cd_grp_id = 'BMC0002'
ORDER BY cd_val DESC;"""),
        ]

        for no, lvl, q, hint, ans in quizzes:
            st.markdown(f"### 문제 {no} &nbsp; {lvl}")
            _practice(q, ans, f"qz{no}", hint=hint)
            st.divider()

        st.markdown('''<div class="tip">
<b>실수 방지 최종 체크리스트</b><br>
• NULL 비교 → <code>IS NULL</code> (<code>= NULL</code>은 항상 FALSE)<br>
• COA311M JOIN → <code>cd_grp_id = 'HTA0018'</code> AND <code>del_yn = 'N'</code> 조건 필수<br>
• 집계 조건 → <code>WHERE</code> 아닌 <code>HAVING</code><br>
• GROUP BY → SELECT의 비집계 컬럼 모두 포함<br>
• CASE WHEN → 반드시 <code>END</code>로 닫기, ELSE 생략 시 NULL 반환<br>
• JOIN → <code>ON</code> 조건 누락 시 카테시안 곱 발생<br>
• 0 나누기 방지 → <code>NULLIF(분모, 0)</code> 사용
</div>''', unsafe_allow_html=True)
    nav_footer(10)

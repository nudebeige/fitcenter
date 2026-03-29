# app.py - FitCenter 회원 관리 프로그램
# 최종판: Excel 다운로드 + 멤버십 연장 + 출석 통계 + 신규회원 출석 통합

import streamlit as st
import pandas as pd
import sqlite3
import os
import plotly.express as px
from datetime import date, datetime
import io

st.set_page_config(
    page_title="FitCenter 회원 관리",
    page_icon="🏋️",
    layout="wide"
)

# ── 한국어 변환 사전 ─────────────────────────────────
STATUS_KR = {
    "Active":    "활성",
    "Inactive":  "비활성",
    "Paused":    "일시정지",
    "Trialing":  "체험중",
    "Pending":   "대기중",
    "Guest":     "게스트",
    "Completed": "완료",
    "Suspended": "정지",
    "Dropped":   "탈퇴",
}
STATUS_EN  = {v: k for k, v in STATUS_KR.items()}
MEMBERSHIP_KR = {
    "Monthly":    "월간권",
    "Annual":     "연간권",
    "Trial":      "체험권",
    "Flex Plan":  "자유이용권",
    "Single Use": "1회이용권",
    "Seasonal":   "시즌권",
    "Premium":    "프리미엄",
    "Basic":      "기본권",
}
AUTOPAY_KR = {"ON": "자동결제", "OFF": "수동결제"}
GENDER_KR  = {
    "Male": "남성", "Female": "여성",
    "Non-binary": "논바이너리", "Other": "기타",
}
DROP_REASONS = [
    "비용 부담", "이사 / 거리 문제", "시간 부족",
    "건강 문제", "서비스 불만족", "목표 달성 후 종료", "기타",
]
WEEKDAY_KR = {
    "Monday": "월요일", "Tuesday": "화요일", "Wednesday": "수요일",
    "Thursday": "목요일", "Friday": "금요일",
    "Saturday": "토요일", "Sunday": "일요일",
}

def to_kr(df, col, mapping):
    if col in df.columns:
        df[col] = df[col].map(mapping).fillna(df[col])
    return df

# ── 로그인 설정 ──────────────────────────────────────
PASSWORDS = {"관리자": "admin1234", "데스크": "desk1234"}

def load_passwords():
    try:
        conn = get_conn()
        df = pd.read_sql_query(
            "SELECT username, password FROM staff_passwords", conn
        )
        conn.close()
        for _, row in df.iterrows():
            PASSWORDS[row["username"]] = row["password"]
    except Exception:
        pass

def check_login():
    if "logged_in" not in st.session_state:
        st.session_state.logged_in = False
    if "username" not in st.session_state:
        st.session_state.username = ""
    if st.session_state.logged_in:
        return True

    st.markdown("## 🏋️ FitCenter 회원 관리 시스템")
    st.markdown("---")
    col = st.columns([1, 2, 1])[1]
    with col:
        st.markdown("### 로그인")
        username = st.selectbox("사용자", list(PASSWORDS.keys()))
        password = st.text_input("비밀번호", type="password")
        if st.button("로그인", type="primary", use_container_width=True):
            if PASSWORDS.get(username) == password:
                st.session_state.logged_in = True
                st.session_state.username  = username
                st.rerun()
            else:
                st.error("비밀번호가 올바르지 않습니다.")
    return False

# ── DB 연결 ─────────────────────────────────────────
def get_conn():
    db_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "fitcenter.db"
    )
    return sqlite3.connect(db_path)

def run_query(sql, params=()):
    conn = get_conn()
    df = pd.read_sql_query(sql, conn, params=params)
    conn.close()
    return df

def init_tables():
    conn = get_conn()
    conn.execute("""CREATE TABLE IF NOT EXISTS body_metrics (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        member_name TEXT, measured_date TEXT,
        weight REAL, height REAL, bmi REAL,
        memo TEXT, created_at TEXT DEFAULT CURRENT_TIMESTAMP)""")
    conn.execute("""CREATE TABLE IF NOT EXISTS attendance_logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        member_name TEXT, checkin_date TEXT,
        checkin_time TEXT, created_at TEXT DEFAULT CURRENT_TIMESTAMP)""")
    conn.execute("""CREATE TABLE IF NOT EXISTS new_members (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        fullname TEXT, gender TEXT, birthdate TEXT, phone TEXT,
        membership TEXT, start_date TEXT, end_date TEXT,
        fee INTEGER, autopay TEXT, goal TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP)""")
    conn.execute("""CREATE TABLE IF NOT EXISTS drop_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        member_name TEXT, drop_date TEXT, drop_reason TEXT,
        memo TEXT, created_at TEXT DEFAULT CURRENT_TIMESTAMP)""")
    conn.execute("""CREATE TABLE IF NOT EXISTS staff_passwords (
        username TEXT PRIMARY KEY, password TEXT)""")
    conn.execute("""CREATE TABLE IF NOT EXISTS membership_extensions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        member_name TEXT, old_enddate TEXT, new_enddate TEXT,
        extension_months INTEGER, fee INTEGER,
        extended_at TEXT DEFAULT CURRENT_TIMESTAMP)""")
    try:
        conn.execute("ALTER TABLE members ADD COLUMN dropreason TEXT")
    except Exception:
        pass
    conn.commit()
    conn.close()

# ── Excel 다운로드 헬퍼 ──────────────────────────────
def to_excel(df):
    """데이터프레임을 Excel 바이트로 변환"""
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="회원목록")
    return buf.getvalue()

# ── 모든 출석 가능 회원 이름 목록 (기존 + 신규) ────────
def get_all_member_names():
    """기존 회원 + 신규 등록 회원 이름 통합"""
    existing = run_query("SELECT DISTINCT fullname FROM members")["fullname"].tolist()
    new_ones = run_query("SELECT DISTINCT fullname FROM new_members")["fullname"].tolist()
    all_names = sorted(set(existing + new_ones))
    return all_names

# 초기화 순서 중요: 테이블 먼저 → CSV 이관 → 비밀번호 로드
def auto_import_csv():
    try:
        # 1. 테이블 먼저 생성
        init_tables()

        # 2. members 테이블이 비어있는지 확인
        conn = get_conn()
        count = conn.execute(
            "SELECT COUNT(*) FROM members"
        ).fetchone()[0]
        conn.close()

        # 3. 비어있으면 CSV에서 데이터 읽어서 넣기
        if count == 0:
            base = os.path.dirname(os.path.abspath(__file__))
            db1_path = os.path.join(base, "db1.csv")
            db2_path = os.path.join(base, "db2.csv")

            if os.path.exists(db1_path) and os.path.exists(db2_path):
                db1 = pd.read_csv(db1_path)
                db2 = pd.read_csv(db2_path)
                db1.columns = [c.lower() for c in db1.columns]
                db2.columns = [c.lower() for c in db2.columns]

                conn = get_conn()
                db1.to_sql("members", conn,
                           if_exists="replace", index=False)
                db2.to_sql("class_reservations", conn,
                           if_exists="replace", index=False)
                conn.close()
    except Exception as e:
        st.error(f"DB 초기화 오류: {e}")

auto_import_csv()
load_passwords()

if not check_login():
    st.stop()

# ── 사이드바 ─────────────────────────────────────────
st.sidebar.title("🏋️ FitCenter")
st.sidebar.caption(f"접속자: {st.session_state.username}")
st.sidebar.caption(f"오늘: {date.today().strftime('%Y년 %m월 %d일')}")
st.sidebar.caption(f"현재 시각: {datetime.now().strftime('%H:%M')}")
st.sidebar.markdown("---")
menu = st.sidebar.radio(
    "메뉴",
    ["회원 현황", "회원 목록", "회원 수정 / 탈퇴",
     "멤버십 연장", "출석 체크", "출석 통계",
     "건강 기록 입력", "신규 회원 등록"]
)
st.sidebar.markdown("---")
with st.sidebar.expander("⚙️ 비밀번호 변경"):
    cur  = st.text_input("현재 비밀번호",   type="password", key="cur_pw")
    nw   = st.text_input("새 비밀번호",     type="password", key="new_pw")
    nw2  = st.text_input("새 비밀번호 확인", type="password", key="new_pw2")
    if st.button("변경 저장", key="pw_save"):
        uname = st.session_state.username
        if PASSWORDS.get(uname) != cur:
            st.error("현재 비밀번호가 틀렸습니다.")
        elif len(nw) < 6:
            st.error("6자 이상 입력해주세요.")
        elif nw != nw2:
            st.error("새 비밀번호가 일치하지 않습니다.")
        else:
            conn = get_conn()
            conn.execute(
                "INSERT OR REPLACE INTO staff_passwords VALUES (?,?)",
                (uname, nw)
            )
            conn.commit()
            conn.close()
            PASSWORDS[uname] = nw
            st.success("✅ 변경 완료!")
if st.sidebar.button("로그아웃"):
    st.session_state.logged_in = False
    st.session_state.username  = ""
    st.rerun()

# ══════════════════════════════════════════════════
# 메뉴 1: 회원 현황
# ══════════════════════════════════════════════════
if menu == "회원 현황":
    st.title("회원 현황")
    st.markdown("---")

    total   = run_query("SELECT COUNT(*) n FROM members").iloc[0]["n"]
    active  = run_query("SELECT COUNT(*) n FROM members WHERE status='Active'").iloc[0]["n"]
    paused  = run_query("SELECT COUNT(*) n FROM members WHERE status='Paused'").iloc[0]["n"]
    dropped = run_query("SELECT COUNT(*) n FROM members WHERE status='Dropped'").iloc[0]["n"]
    today_n = run_query(
        "SELECT COUNT(*) n FROM attendance_logs WHERE checkin_date=?",
        params=(date.today().isoformat(),)
    ).iloc[0]["n"]

    c1,c2,c3,c4,c5 = st.columns(5)
    c1.metric("전체 회원",  f"{total:,}명")
    c2.metric("활성 회원",  f"{active:,}명")
    c3.metric("일시정지",   f"{paused:,}명")
    c4.metric("탈퇴 회원",  f"{dropped:,}명")
    c5.metric("오늘 방문",  f"{today_n}명")

    st.markdown("---")
    st.subheader("⚠️ 만료 임박 회원 TOP 10")
    df_exp = run_query("""
        SELECT fullname AS 이름, status AS 상태,
               membershiplabel AS 멤버십, enddate AS 종료일,
               lastattdate AS 최근방문
        FROM members WHERE status='Active'
        ORDER BY enddate ASC LIMIT 10
    """)
    df_exp = to_kr(df_exp, "상태", STATUS_KR)
    st.dataframe(df_exp, hide_index=True, use_container_width=True)

    st.markdown("---")
    st.subheader("🔔 장기 미방문 회원 TOP 15 (최근방문 오래된 순)")
    df_absent = run_query("""
        SELECT fullname AS 이름, status AS 상태,
               lastattdate AS 최근방문일, atttotal AS 총방문횟수
        FROM members
        WHERE status='Active' AND lastattdate IS NOT NULL AND lastattdate != ''
        ORDER BY lastattdate ASC LIMIT 15
    """)
    df_absent = to_kr(df_absent, "상태", STATUS_KR)
    st.dataframe(df_absent, hide_index=True, use_container_width=True)

    st.markdown("---")
    col_l, col_r = st.columns(2)
    with col_l:
        st.subheader("멤버십 종류별 현황")
        df_ms = run_query("""
            SELECT membershipcategory AS 종류, COUNT(*) AS 회원수
            FROM members GROUP BY membershipcategory ORDER BY 회원수 DESC
        """)
        df_ms["종류"] = df_ms["종류"].map(MEMBERSHIP_KR).fillna(df_ms["종류"])
        fig1 = px.bar(df_ms, x="종류", y="회원수", color="회원수",
                      color_continuous_scale="Blues")
        fig1.update_layout(showlegend=False, height=300,
                           margin=dict(l=0,r=0,t=20,b=0))
        st.plotly_chart(fig1, use_container_width=True)
    with col_r:
        st.subheader("상태별 분포")
        df_st = run_query("""
            SELECT status AS 상태, COUNT(*) AS 인원
            FROM members GROUP BY status
        """)
        df_st["상태"] = df_st["상태"].map(STATUS_KR).fillna(df_st["상태"])
        fig2 = px.pie(df_st, names="상태", values="인원",
                      color_discrete_sequence=px.colors.sequential.Blues_r)
        fig2.update_layout(height=300, margin=dict(l=0,r=0,t=20,b=0))
        st.plotly_chart(fig2, use_container_width=True)

    df_dr = run_query("""
        SELECT drop_reason AS 탈퇴사유, COUNT(*) AS 건수
        FROM drop_history GROUP BY drop_reason ORDER BY 건수 DESC
    """)
    if len(df_dr) > 0:
        st.markdown("---")
        st.subheader("탈퇴 사유 분포")
        fig4 = px.bar(df_dr, x="탈퇴사유", y="건수")
        fig4.update_layout(height=260, margin=dict(l=0,r=0,t=10,b=0))
        st.plotly_chart(fig4, use_container_width=True)

# ══════════════════════════════════════════════════
# 메뉴 2: 회원 목록 + Excel 다운로드
# ══════════════════════════════════════════════════
elif menu == "회원 목록":
    st.title("회원 목록")
    st.markdown("---")

    col_s, col_f = st.columns([3, 1])
    with col_s:
        search = st.text_input("이름 검색", placeholder="이름을 입력하세요...")
    with col_f:
        status_f = st.selectbox(
            "상태 필터",
            ["전체 (탈퇴 제외)", "활성", "비활성", "일시정지",
             "체험중", "대기중", "탈퇴 회원만"]
        )

    # 검색어나 필터 없으면 최근 100명만 표시 (속도 개선)
    if search:
        df = run_query("""
            SELECT fullname AS 이름, genderidentity AS 성별,
                   age AS 나이, status AS 상태,
                   membershiplabel AS 멤버십, membershipcategory AS 멤버십종류,
                   begindate AS 시작일, enddate AS 종료일,
                   lastattdate AS 최근방문, atttotal AS 총방문횟수,
                   autopay AS 결제방식
            FROM members
            WHERE fullname LIKE ?
        """, params=(f"%{search}%",))
    else:
        df = run_query("""
            SELECT fullname AS 이름, genderidentity AS 성별,
                   age AS 나이, status AS 상태,
                   membershiplabel AS 멤버십, membershipcategory AS 멤버십종류,
                   begindate AS 시작일, enddate AS 종료일,
                   lastattdate AS 최근방문, atttotal AS 총방문횟수,
                   autopay AS 결제방식
            FROM members
            ORDER BY rowid DESC
            LIMIT 100
        """)
    df = to_kr(df, "상태",      STATUS_KR)
    df = to_kr(df, "멤버십종류", MEMBERSHIP_KR)
    df = to_kr(df, "결제방식",   AUTOPAY_KR)
    df = to_kr(df, "성별",      GENDER_KR)

    if search:
        df = df[df["이름"].str.contains(search, case=False, na=False)]
    if status_f == "전체 (탈퇴 제외)":
        df = df[df["상태"] != "탈퇴"]
    elif status_f == "탈퇴 회원만":
        df = df[df["상태"] == "탈퇴"]
    else:
        df = df[df["상태"] == status_f]

    col_cap, col_btn = st.columns([3, 1])
    with col_cap:
        if search:
            st.caption(f"검색 결과: {len(df):,}명")
        else:
            st.caption(f"최근 등록 100명 표시 중 (전체 검색하려면 이름을 입력하세요)")
    with col_btn:
        # Excel 다운로드 버튼
        excel_data = to_excel(df)
        st.download_button(
            label="📥 Excel 다운로드",
            data=excel_data,
            file_name=f"회원목록_{date.today()}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )

    st.dataframe(df, use_container_width=True, height=400, hide_index=True)

    # 회원 상세
    st.markdown("---")
    st.subheader("회원 상세 보기")
    name_input = st.text_input("이름을 정확히 입력하세요",
                               placeholder="예: Donald Walker")
    if name_input:
        detail = run_query(
            "SELECT * FROM members WHERE fullname=?", params=(name_input,)
        )
        if len(detail) == 0:
            st.warning("해당 이름의 회원을 찾을 수 없습니다.")
        else:
            row = detail.iloc[0]
            st.success(f"{row['fullname']} 님의 상세 정보")
            d1,d2,d3 = st.columns(3)
            d1.metric("상태",    STATUS_KR.get(row.get("status",""), "-"))
            d2.metric("총 방문", f"{int(row.get('atttotal',0))}회")
            d3.metric("결제방식", AUTOPAY_KR.get(row.get("autopay",""), "-"))

            detail_show = detail.copy()
            detail_show = to_kr(detail_show, "status",             STATUS_KR)
            detail_show = to_kr(detail_show, "membershipcategory", MEMBERSHIP_KR)
            detail_show = to_kr(detail_show, "autopay",            AUTOPAY_KR)
            detail_show = to_kr(detail_show, "genderidentity",     GENDER_KR)
            st.dataframe(detail_show.T.rename(columns={0:"값"}),
                         use_container_width=True)

            bm = run_query(
                """SELECT measured_date AS 날짜, weight AS 체중, bmi AS BMI
                   FROM body_metrics WHERE member_name=?
                   ORDER BY measured_date""",
                params=(name_input,)
            )
            if len(bm) >= 2:
                st.subheader("체중 변화")
                fig_bm = px.line(bm, x="날짜", y="체중", markers=True)
                fig_bm.update_layout(height=240,
                                     margin=dict(l=0,r=0,t=10,b=0))
                st.plotly_chart(fig_bm, use_container_width=True)
            elif len(bm) == 1:
                st.info(f"최근 체중: {bm.iloc[0]['체중']}kg  |  BMI: {bm.iloc[0]['BMI']}")

# ══════════════════════════════════════════════════
# 메뉴 3: 회원 수정 / 탈퇴
# ══════════════════════════════════════════════════
elif menu == "회원 수정 / 탈퇴":
    st.title("회원 수정 / 탈퇴 처리")
    st.markdown("---")

    name_mod = st.text_input("수정할 회원 이름 입력",
                             placeholder="예: Donald Walker")
    if name_mod:
        member = run_query(
            "SELECT * FROM members WHERE fullname=?", params=(name_mod,)
        )
        if len(member) == 0:
            st.warning("해당 이름의 회원을 찾을 수 없습니다.")
        else:
            row = member.iloc[0]
            current_status = STATUS_KR.get(row.get("status",""), row.get("status",""))
            st.info(
                f"현재 상태: **{current_status}**  |  "
                f"멤버십: {row.get('membershiplabel','-')}  |  "
                f"종료일: {row.get('enddate','-')}"
            )
            tab1, tab2 = st.tabs(["상태 변경", "탈퇴 처리"])

            with tab1:
                st.subheader("멤버십 상태 변경")
                new_status_kr = st.selectbox(
                    "변경할 상태",
                    ["활성", "비활성", "일시정지", "체험중", "대기중"]
                )
                if st.button("상태 변경 저장", type="primary"):
                    new_status_en = STATUS_EN.get(new_status_kr, new_status_kr)
                    conn = get_conn()
                    conn.execute(
                        "UPDATE members SET status=? WHERE fullname=?",
                        (new_status_en, name_mod)
                    )
                    conn.commit()
                    conn.close()
                    st.success(
                        f"✅ '{current_status}' → '{new_status_kr}' 변경 완료!"
                    )

            with tab2:
                st.subheader("탈퇴 처리")
                st.error("⚠️ 데이터는 보존되고 상태만 '탈퇴'로 변경됩니다.")
                drop_reason = st.selectbox("탈퇴 사유 *", DROP_REASONS)
                drop_memo   = st.text_area("추가 메모 (선택)")
                drop_date   = st.date_input("탈퇴일", value=date.today())
                confirm     = st.checkbox(
                    f"'{name_mod}' 님을 탈퇴 처리하는 것에 동의합니다."
                )
                if st.button("탈퇴 처리 완료", type="primary",
                             disabled=not confirm):
                    conn = get_conn()
                    conn.execute(
                        "UPDATE members SET status='Dropped', dropreason=? WHERE fullname=?",
                        (drop_reason, name_mod)
                    )
                    conn.execute(
                        """INSERT INTO drop_history
                           (member_name,drop_date,drop_reason,memo)
                           VALUES (?,?,?,?)""",
                        (name_mod, drop_date.isoformat(), drop_reason, drop_memo)
                    )
                    conn.commit()
                    conn.close()
                    st.success(f"✅ {name_mod} 님 탈퇴 처리 완료.")

    st.markdown("---")
    st.subheader("탈퇴 이력")
    df_drop = run_query("""
        SELECT member_name AS 이름, drop_date AS 탈퇴일,
               drop_reason AS 사유, memo AS 메모
        FROM drop_history ORDER BY drop_date DESC
    """)
    if len(df_drop) == 0:
        st.caption("탈퇴 이력이 없습니다.")
    else:
        st.dataframe(df_drop, hide_index=True, use_container_width=True)

# ══════════════════════════════════════════════════
# 메뉴 4: 멤버십 연장
# ══════════════════════════════════════════════════
elif menu == "멤버십 연장":
    st.title("멤버십 연장")
    st.markdown("---")

    name_ext = st.text_input("회원 이름 입력", placeholder="예: Donald Walker")

    if name_ext:
        member = run_query(
            "SELECT * FROM members WHERE fullname=?", params=(name_ext,)
        )
        if len(member) == 0:
            st.warning("해당 이름의 회원을 찾을 수 없습니다.")
        else:
            row = member.iloc[0]
            current_end = row.get("enddate", "")
            status_kr   = STATUS_KR.get(row.get("status",""), "-")

            st.info(
                f"현재 상태: **{status_kr}**  |  "
                f"멤버십: {row.get('membershiplabel','-')}  |  "
                f"현재 종료일: **{current_end}**"
            )

            st.subheader("연장 정보 입력")
            col1, col2 = st.columns(2)
            with col1:
                ext_months = st.selectbox(
                    "연장 기간", ["1개월", "3개월", "6개월", "12개월"]
                )
            with col2:
                ext_fee = st.number_input(
                    "결제 금액 (원)", min_value=0, step=10000
                )

            # 새 종료일 자동 계산
            months_map = {"1개월": 1, "3개월": 3, "6개월": 6, "12개월": 12}
            try:
                from dateutil.relativedelta import relativedelta
                # 현재 종료일 파싱 (MM/DD/YYYY 형식)
                if "/" in current_end:
                    parts    = current_end.split("/")
                    base_dt  = date(int(parts[2]), int(parts[0]), int(parts[1]))
                else:
                    base_dt  = date.fromisoformat(current_end)
                new_end = base_dt + relativedelta(months=months_map[ext_months])
                st.success(f"연장 후 종료일: **{new_end.strftime('%Y년 %m월 %d일')}**")
            except Exception:
                new_end = None
                st.warning("종료일 형식을 확인할 수 없습니다. 직접 입력해주세요.")
                new_end_manual = st.date_input("새 종료일 직접 입력",
                                              value=date.today())
                new_end = new_end_manual

            autopay_new = st.radio("자동결제", ["ON", "OFF"], horizontal=True)

            if st.button("연장 처리 완료", type="primary"):
                new_end_str = new_end.strftime("%m/%d/%Y")
                conn = get_conn()
                # members 테이블 종료일 업데이트
                conn.execute(
                    "UPDATE members SET enddate=?, autopay=?, status='Active' WHERE fullname=?",
                    (new_end_str, autopay_new, name_ext)
                )
                # 연장 이력 기록
                conn.execute(
                    """INSERT INTO membership_extensions
                       (member_name, old_enddate, new_enddate, extension_months, fee)
                       VALUES (?,?,?,?,?)""",
                    (name_ext, current_end, new_end_str,
                     months_map[ext_months], ext_fee)
                )
                conn.commit()
                conn.close()
                st.success(
                    f"✅ {name_ext} 님 멤버십 연장 완료!  "
                    f"{current_end} → {new_end_str}  |  "
                    f"결제: {ext_fee:,}원"
                )

    # 연장 이력
    st.markdown("---")
    st.subheader("연장 이력")
    df_ext = run_query("""
        SELECT member_name AS 이름, old_enddate AS 기존종료일,
               new_enddate AS 새종료일, extension_months AS 연장개월,
               fee AS 결제금액, extended_at AS 처리일시
        FROM membership_extensions ORDER BY id DESC LIMIT 20
    """)
    if len(df_ext) == 0:
        st.caption("연장 이력이 없습니다.")
    else:
        st.dataframe(df_ext, hide_index=True, use_container_width=True)

# ══════════════════════════════════════════════════
# 메뉴 5: 출석 체크 (신규 회원 통합)
# ══════════════════════════════════════════════════
elif menu == "출석 체크":
    st.title("출석 체크")
    st.markdown("---")

    name_checkin = st.text_input(
        "회원 이름 입력 후 엔터",
        placeholder="예: Donald Walker",
        key="checkin_input"
    )

    if name_checkin:
        # 기존 회원 먼저 조회
        member = run_query(
            "SELECT * FROM members WHERE fullname=?", params=(name_checkin,)
        )
        # 없으면 신규 등록 회원에서 조회
        is_new = False
        if len(member) == 0:
            new_m = run_query(
                "SELECT * FROM new_members WHERE fullname=?",
                params=(name_checkin,)
            )
            if len(new_m) > 0:
                is_new = True
                nm_row = new_m.iloc[0]
            else:
                st.error("❌ 회원을 찾을 수 없습니다. (기존 회원 + 신규 등록 모두 확인)")
                nm_row = None
        else:
            nm_row = None

        # 탈퇴 회원 차단
        if len(member) > 0 and member.iloc[0].get("status") == "Dropped":
            st.error("❌ 탈퇴한 회원입니다. 출석 처리가 불가합니다.")
        elif len(member) > 0 or is_new:
            today  = date.today().isoformat()
            now    = datetime.now().strftime("%H:%M")
            already = run_query(
                "SELECT * FROM attendance_logs WHERE member_name=? AND checkin_date=?",
                params=(name_checkin, today)
            )
            if len(already) > 0:
                st.warning(f"⚠️ {name_checkin} 님은 오늘 이미 출석하셨습니다.")
            else:
                conn = get_conn()
                conn.execute(
                    "INSERT INTO attendance_logs (member_name,checkin_date,checkin_time) VALUES (?,?,?)",
                    (name_checkin, today, now)
                )
                conn.commit()
                conn.close()
                st.success(f"✅ {name_checkin} 님 출석 완료! ({today} {now})")

            if is_new:
                st.info(
                    f"신규 등록 회원  |  "
                    f"멤버십: {nm_row.get('membership','-')}  |  "
                    f"종료일: {nm_row.get('end_date','-')}"
                )
            else:
                row = member.iloc[0]
                status_kr = STATUS_KR.get(row.get("status",""), "-")
                st.info(
                    f"멤버십: {row.get('membershiplabel','-')}  |  "
                    f"상태: {status_kr}  |  "
                    f"총 방문: {int(row.get('atttotal',0))}회"
                )

    st.markdown("---")
    st.subheader("오늘 방문자")
    today_log = run_query(
        """SELECT member_name AS 이름, checkin_time AS 시간
           FROM attendance_logs WHERE checkin_date=?
           ORDER BY id DESC""",
        params=(date.today().isoformat(),)
    )
    if len(today_log) == 0:
        st.caption("오늘 출석 기록이 없습니다.")
    else:
        st.caption(f"오늘 방문: {len(today_log)}명")
        st.dataframe(today_log, hide_index=True, use_container_width=True)

# ══════════════════════════════════════════════════
# 메뉴 6: 출석 통계
# ══════════════════════════════════════════════════
elif menu == "출석 통계":
    st.title("출석 통계")
    st.markdown("---")

    df_log = run_query("""
        SELECT member_name, checkin_date, checkin_time
        FROM attendance_logs
        ORDER BY checkin_date DESC
    """)

    if len(df_log) == 0:
        st.info("출석 기록이 없습니다. 출석 체크 메뉴에서 기록을 추가해보세요.")
    else:
        df_log["checkin_date"] = pd.to_datetime(df_log["checkin_date"])
        df_log["요일"] = df_log["checkin_date"].dt.day_name().map(WEEKDAY_KR)
        df_log["시간대"] = df_log["checkin_time"].str[:2] + "시"
        df_log["날짜"]  = df_log["checkin_date"].dt.strftime("%Y-%m-%d")

        col1, col2 = st.columns(2)

        # 요일별 방문 통계
        with col1:
            st.subheader("요일별 방문 현황")
            weekday_order = ["월요일","화요일","수요일","목요일",
                             "금요일","토요일","일요일"]
            df_wd = (df_log.groupby("요일")
                           .size()
                           .reset_index(name="방문수")
                           .sort_values(
                               "요일",
                               key=lambda x: x.map({v:i for i,v in enumerate(weekday_order)})
                           ))
            fig_wd = px.bar(df_wd, x="요일", y="방문수",
                            color="방문수", color_continuous_scale="Blues",
                            category_orders={"요일": weekday_order})
            fig_wd.update_layout(showlegend=False, height=320,
                                 margin=dict(l=0,r=0,t=20,b=0))
            st.plotly_chart(fig_wd, use_container_width=True)

        # 시간대별 방문 통계
        with col2:
            st.subheader("시간대별 방문 현황")
            df_hr = (df_log.groupby("시간대")
                           .size()
                           .reset_index(name="방문수")
                           .sort_values("시간대"))
            fig_hr = px.bar(df_hr, x="시간대", y="방문수",
                            color="방문수", color_continuous_scale="Teal")
            fig_hr.update_layout(showlegend=False, height=320,
                                 margin=dict(l=0,r=0,t=20,b=0))
            st.plotly_chart(fig_hr, use_container_width=True)

        # 날짜별 방문 추이
        st.markdown("---")
        st.subheader("날짜별 방문 추이")
        df_daily = (df_log.groupby("날짜")
                          .size()
                          .reset_index(name="방문수"))
        fig_dl = px.line(df_daily, x="날짜", y="방문수", markers=True)
        fig_dl.update_layout(height=280, margin=dict(l=0,r=0,t=10,b=0))
        st.plotly_chart(fig_dl, use_container_width=True)

        # 자주 방문한 회원 TOP 10
        st.markdown("---")
        st.subheader("이 달 방문 TOP 10")
        this_month = date.today().strftime("%Y-%m")
        df_top = run_query(f"""
            SELECT member_name AS 이름, COUNT(*) AS 방문수
            FROM attendance_logs
            WHERE checkin_date LIKE '{this_month}%'
            GROUP BY member_name
            ORDER BY 방문수 DESC
            LIMIT 10
        """)
        if len(df_top) > 0:
            st.dataframe(df_top, hide_index=True, use_container_width=True)
        else:
            st.caption("이번 달 출석 기록이 없습니다.")

# ══════════════════════════════════════════════════
# 메뉴 7: 건강 기록 입력
# ══════════════════════════════════════════════════
elif menu == "건강 기록 입력":
    st.title("건강 기록 입력")
    st.markdown("---")
    st.caption("체중과 키를 입력하면 BMI가 자동 계산됩니다.")

    with st.form("bmi_form"):
        member_name  = st.text_input("회원 이름", placeholder="예: Donald Walker")
        col_w, col_h = st.columns(2)
        with col_w:
            weight = st.number_input("체중 (kg)", min_value=20.0,
                                     max_value=300.0, step=0.1)
        with col_h:
            height = st.number_input("키 (cm)", min_value=100.0,
                                     max_value=250.0, step=0.1)
        measured_date = st.date_input("측정일", value=date.today())
        memo          = st.text_area("메모 (선택)")
        submitted     = st.form_submit_button("저장")

    if submitted:
        if not member_name:
            st.error("회원 이름을 입력해주세요.")
        else:
            # 기존 + 신규 회원 모두 확인
            check_old = run_query(
                "SELECT fullname FROM members WHERE fullname=?",
                params=(member_name,)
            )
            check_new = run_query(
                "SELECT fullname FROM new_members WHERE fullname=?",
                params=(member_name,)
            )
            if len(check_old) == 0 and len(check_new) == 0:
                st.error("해당 이름의 회원이 없습니다.")
            else:
                bmi = round(weight / ((height / 100) ** 2), 1)
                conn = get_conn()
                conn.execute(
                    """INSERT INTO body_metrics
                       (member_name,measured_date,weight,height,bmi,memo)
                       VALUES (?,?,?,?,?,?)""",
                    (member_name, measured_date.isoformat(),
                     weight, height, bmi, memo)
                )
                conn.commit()
                conn.close()
                st.success("✅ 저장 완료!")
                st.metric("계산된 BMI", f"{bmi}")
                if   bmi < 18.5: st.info("저체중")
                elif bmi < 23:   st.success("정상")
                elif bmi < 25:   st.warning("과체중")
                else:            st.error("비만")

# ══════════════════════════════════════════════════
# 메뉴 8: 신규 회원 등록
# ══════════════════════════════════════════════════
elif menu == "신규 회원 등록":
    st.title("신규 회원 등록")
    st.markdown("---")

    with st.form("new_member_form"):
        st.subheader("기본 정보")
        col1, col2 = st.columns(2)
        with col1:
            fullname  = st.text_input("이름 *", placeholder="홍길동")
            birthdate = st.date_input("생년월일 *",
                                      value=date(1990,1,1),
                                      min_value=date(1920,1,1),
                                      max_value=date.today())
            phone     = st.text_input("전화번호", placeholder="010-0000-0000")
        with col2:
            gender = st.selectbox("성별 *", ["여성","남성","논바이너리","기타"])
            goal   = st.selectbox("운동 목표",
                                  ["체중 감량","근력 증가","체력 향상","기타"])

        st.subheader("멤버십 정보")
        col3, col4 = st.columns(2)
        with col3:
            membership = st.selectbox(
                "멤버십 상품 *",
                ["월간권","연간권","체험권","자유이용권","1회이용권"]
            )
            start_date = st.date_input("시작일 *", value=date.today())
        with col4:
            duration = st.selectbox("기간", ["1개월","3개월","6개월","12개월"])
            fee      = st.number_input("결제 금액 (원)", min_value=0, step=10000)

        autopay    = st.radio("자동결제", ["ON","OFF"], horizontal=True)
        submitted2 = st.form_submit_button("등록 완료")

    if submitted2:
        if not fullname:
            st.error("이름을 입력해주세요.")
        else:
            from dateutil.relativedelta import relativedelta
            months_map = {"1개월":1,"3개월":3,"6개월":6,"12개월":12}
            end_date   = start_date + relativedelta(months=months_map[duration])
            conn = get_conn()
            conn.execute(
                """INSERT INTO new_members
                   (fullname,gender,birthdate,phone,membership,
                    start_date,end_date,fee,autopay,goal)
                   VALUES (?,?,?,?,?,?,?,?,?,?)""",
                (fullname, gender, birthdate.isoformat(), phone,
                 membership, start_date.isoformat(), end_date.isoformat(),
                 fee, autopay, goal)
            )
            conn.commit()
            conn.close()
            st.success(f"✅ {fullname} 님 등록 완료!")
            st.info(
                f"멤버십: {membership}  |  시작일: {start_date}  |  "
                f"종료일: {end_date}  |  결제: {fee:,}원"
            )

    # 신규 등록 회원 목록
    st.markdown("---")
    st.subheader("신규 등록 회원 목록")
    df_new = run_query("""
        SELECT fullname AS 이름, gender AS 성별,
               membership AS 멤버십, start_date AS 시작일,
               end_date AS 종료일, fee AS 결제금액,
               goal AS 목표, created_at AS 등록일시
        FROM new_members ORDER BY id DESC
    """)
    if len(df_new) == 0:
        st.caption("신규 등록 회원이 없습니다.")
    else:
        # Excel 다운로드
        st.download_button(
            label="📥 신규 회원 Excel 다운로드",
            data=to_excel(df_new),
            file_name=f"신규회원_{date.today()}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
        st.dataframe(df_new, hide_index=True, use_container_width=True)
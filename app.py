"""
11thproject 적합 지연 모니터링 대시보드 - FastAPI
"""
import os
import json
import re
from pathlib import Path
from datetime import datetime, timedelta
from fastapi import FastAPI, UploadFile, File, Query
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, JSONResponse
from typing import Optional
import pandas as pd

app = FastAPI(title="해외영업 적합지연 모니터링")

BASE_DIR = Path(__file__).parent
STATIC_DIR = BASE_DIR / "static"
STATIC_DIR.mkdir(exist_ok=True)
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

# 최신 업로드 파일 우선, 없으면 원본
_latest_xls = BASE_DIR / "품질검사대상_latest.xls"
_latest_xlsx = BASE_DIR / "품질검사대상_latest.xlsx"
_original = BASE_DIR / "품질검사대상 - 2026-03-25T183027.167.xls"
if _latest_xls.exists():
    DATA_FILE = _latest_xls
elif _latest_xlsx.exists():
    DATA_FILE = _latest_xlsx
else:
    DATA_FILE = _original
MANAGER_FILE = BASE_DIR / "완제품 해외담당.xlsx"
HISTORY_FILE = BASE_DIR / "delay_history.json"

# 2026 한국 공휴일
HOLIDAYS = {
    datetime(2026,1,1).date(), datetime(2026,2,16).date(), datetime(2026,2,17).date(),
    datetime(2026,2,18).date(), datetime(2026,3,1).date(), datetime(2026,5,5).date(),
    datetime(2026,5,24).date(), datetime(2026,6,6).date(), datetime(2026,8,15).date(),
    datetime(2026,9,24).date(), datetime(2026,9,25).date(), datetime(2026,9,26).date(),
    datetime(2026,10,3).date(), datetime(2026,10,9).date(), datetime(2026,12,25).date(),
}

def parse_ref_date(date_str: Optional[str] = None):
    if date_str:
        try:
            return datetime.strptime(date_str, "%Y-%m-%d")
        except ValueError:
            pass
    return datetime.now()  # 기본값: 오늘


def working_days_between(start, end):
    if pd.isna(start) or pd.isna(end):
        return None
    s = start.date() if hasattr(start, 'date') else start
    e = end.date() if hasattr(end, 'date') else end
    count = 0
    cur = s + timedelta(days=1)
    while cur <= e:
        if cur.weekday() < 5 and cur not in HOLIDAYS:
            count += 1
        cur += timedelta(days=1)
    return count


def extract_code(product_code):
    if pd.isna(product_code) or not isinstance(product_code, str):
        return None
    if len(product_code) >= 4 and product_code[0] == '9':
        letters = ""
        for ch in product_code[1:]:
            if ch.isalpha():
                letters += ch.upper()
                if len(letters) == 3:
                    return letters
    return None


def load_managers():
    if not MANAGER_FILE.exists():
        return {}
    df = pd.read_excel(str(MANAGER_FILE))
    df['담당자'] = df['담당자'].ffill()
    mapping = {}
    for _, row in df.iterrows():
        code = str(row['고객사']).strip()
        mgr = row['담당자']
        if pd.notna(mgr) and code:
            mapping[code] = mgr
    return mapping


def load_data(filepath=None, ref_date=None):
    fp = filepath or str(DATA_FILE)
    rd = ref_date or datetime.now()
    df = pd.read_excel(fp)
    df['입고일자'] = pd.to_datetime(df['입고일자'], errors='coerce')
    df['판정일자'] = pd.to_datetime(df['판정일자'], errors='coerce')
    df['고객사코드'] = df['품목코드'].apply(extract_code)

    mgr_map = load_managers()
    df['해외담당자'] = df['고객사코드'].map(mgr_map)

    df['경과WD'] = df['입고일자'].apply(lambda x: working_days_between(x, rd))

    def grade(wd):
        if wd is None: return "정보없음"
        if wd > 5: return "5일초과"
        if wd >= 3: return "3일초과"
        return "정상"

    df['지연등급'] = df['경과WD'].apply(grade)

    # 완제품 해외담당에 등록된 고객사코드만 필터
    mgr_map = load_managers()
    if mgr_map:
        valid_codes = set(mgr_map.keys())
        df = df[df['고객사코드'].isin(valid_codes)].copy()

    return df


@app.get("/", response_class=HTMLResponse)
async def index():
    return (STATIC_DIR / "index.html").read_text(encoding="utf-8")


@app.get("/api/data")
async def get_data(ref_date: Optional[str] = Query(None)):
    rd = parse_ref_date(ref_date)
    df = load_data(ref_date=rd)
    testing = df[df['판정결과'] == '시험중'].copy()

    total = len(testing)
    normal = len(testing[testing['지연등급'] == '정상'])
    over3 = len(testing[testing['지연등급'] == '3일초과'])
    over5 = len(testing[testing['지연등급'] == '5일초과'])

    # 담당자별 통계 (고객사코드별 세부 포함)
    mgr_stats = {}
    for _, r in testing.iterrows():
        mgr = r['해외담당자'] if pd.notna(r['해외담당자']) else '미배정'
        cust = r['고객사코드'] if pd.notna(r['고객사코드']) else '기타'
        if mgr not in mgr_stats:
            mgr_stats[mgr] = {'total': 0, 'normal': 0, 'over3': 0, 'over5': 0, 'by_cust': {}}
        mgr_stats[mgr]['total'] += 1
        g = r['지연등급']
        if g == '정상': mgr_stats[mgr]['normal'] += 1
        elif g == '3일초과': mgr_stats[mgr]['over3'] += 1
        elif g == '5일초과': mgr_stats[mgr]['over5'] += 1
        # 고객사별 세부
        cust_nm = r['고객사'] if pd.notna(r.get('고객사')) else ''
        if cust not in mgr_stats[mgr]['by_cust']:
            mgr_stats[mgr]['by_cust'][cust] = {'total': 0, 'normal': 0, 'over3': 0, 'over5': 0, 'name': cust_nm}
        mgr_stats[mgr]['by_cust'][cust]['total'] += 1
        if g == '정상': mgr_stats[mgr]['by_cust'][cust]['normal'] += 1
        elif g == '3일초과': mgr_stats[mgr]['by_cust'][cust]['over3'] += 1
        elif g == '5일초과': mgr_stats[mgr]['by_cust'][cust]['over5'] += 1

    # 고객사코드 → 고객사명 매핑
    cust_name_map = {}
    for _, r in testing.iterrows():
        c = r['고객사코드'] if pd.notna(r['고객사코드']) else '기타'
        if c not in cust_name_map and pd.notna(r.get('고객사')):
            cust_name_map[c] = r['고객사']

    # 고객사코드별 통계
    cust_stats = {}
    for _, r in testing.iterrows():
        c = r['고객사코드'] if pd.notna(r['고객사코드']) else '기타'
        if c not in cust_stats:
            cust_stats[c] = {'total': 0, 'over3': 0, 'over5': 0, 'name': cust_name_map.get(c, '')}
        cust_stats[c]['total'] += 1
        g = r['지연등급']
        if g == '3일초과': cust_stats[c]['over3'] += 1
        elif g == '5일초과': cust_stats[c]['over5'] += 1

    # 테이블 rows
    rows = []
    for _, r in testing.iterrows():
        rows.append({
            'grade': r['지연등급'],
            'wd': int(r['경과WD']) if pd.notna(r['경과WD']) else 0,
            'manager': r['해외담당자'] if pd.notna(r['해외담당자']) else '',
            'cust_code': r['고객사코드'] if pd.notna(r['고객사코드']) else '',
            'test_no': r['시험번호'] if pd.notna(r['시험번호']) else '',
            'product_code': r['품목코드'] if pd.notna(r['품목코드']) else '',
            'product_name': r['품목명'] if pd.notna(r['품목명']) else '',
            'in_date': r['입고일자'].strftime('%Y-%m-%d') if pd.notna(r['입고일자']) else '',
            'result': r['판정결과'] if pd.notna(r['판정결과']) else '',
            'judge': r['판정담당'] if pd.notna(r['판정담당']) else '',
            'marketer': r['마케터'] if pd.notna(r['마케터']) else '',
            'customer': r['고객사'] if pd.notna(r['고객사']) else '',
            'in_type': r['입고유형'] if pd.notna(r['입고유형']) else '',
            'mgmt_type': r['관리유형'] if pd.notna(r['관리유형']) else '',
        })

    rows.sort(key=lambda x: -x['wd'])

    return {
        'ref_date': rd.strftime('%Y-%m-%d'),
        'summary': {'total': total, 'normal': normal, 'over3': over3, 'over5': over5},
        'mgr_stats': mgr_stats,
        'cust_stats': dict(sorted(cust_stats.items(), key=lambda x: -(x[1]['over3']+x[1]['over5']))),
        'rows': rows,
    }


@app.get("/api/filters")
async def get_filters(ref_date: Optional[str] = Query(None)):
    rd = parse_ref_date(ref_date)
    df = load_data(ref_date=rd)
    testing = df[df['판정결과'] == '시험중']
    managers = sorted([m for m in testing['해외담당자'].dropna().unique()])
    custs = sorted([c for c in testing['고객사코드'].dropna().unique()])
    return {'managers': managers, 'customers': custs}


@app.post("/api/upload")
async def upload_file(file: UploadFile = File(...)):
    """품질검사대상 엑셀 파일 업로드"""
    if not file.filename.endswith(('.xls', '.xlsx')):
        return JSONResponse({"ok": False, "error": "엑셀 파일만 업로드 가능합니다."}, status_code=400)
    save_path = BASE_DIR / f"품질검사대상_latest.{'xlsx' if file.filename.endswith('.xlsx') else 'xls'}"
    content = await file.read()
    with open(save_path, "wb") as f:
        f.write(content)
    # DATA_FILE을 최신 업로드로 변경
    global DATA_FILE
    DATA_FILE = save_path
    return {"ok": True, "filename": file.filename, "size": len(content)}


@app.post("/api/upload-manager")
async def upload_manager(file: UploadFile = File(...)):
    """완제품 해외담당 파일 업로드"""
    if not file.filename.endswith(('.xls', '.xlsx')):
        return JSONResponse({"ok": False, "error": "엑셀 파일만 업로드 가능합니다."}, status_code=400)
    content = await file.read()
    with open(MANAGER_FILE, "wb") as f:
        f.write(content)
    return {"ok": True, "filename": file.filename}

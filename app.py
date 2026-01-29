
import re
from pathlib import Path
from datetime import date, timedelta

import numpy as np
import pandas as pd
import streamlit as st

APP_TITLE = "Sales Dashboard (Vendor Map + Weekly Sheets)"
DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

DEFAULT_VENDOR_MAP = DATA_DIR / "vendor_map.xlsx"
DEFAULT_SALES_STORE = DATA_DIR / "sales_store.csv"

# -------------------------
# Normalization
# -------------------------
def _normalize_retailer(x: str) -> str:
    if x is None:
        return ""
    x = str(x).strip()
    aliases = {
        "home depot": "Depot",
        "depot": "Depot",
        "the home depot": "Depot",
        "lowes": "Lowe's",
        "lowe's": "Lowe's",
        "tractor supply": "Tractor Supply",
        "tsc": "Tractor Supply",
        "amazon": "Amazon",
    }
    key = re.sub(r"\s+", " ", x.lower()).strip()
    return aliases.get(key, x)

def _normalize_sku(x: str) -> str:
    if pd.isna(x):
        return ""
    return str(x).strip()

# -------------------------
# Formatting
# -------------------------
def fmt_currency(x) -> str:
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return ""
    try:
        v = float(x)
    except Exception:
        return ""
    s = f"${abs(v):,.2f}"
    return f"({s})" if v < 0 else s

def fmt_int(x) -> str:
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return ""
    try:
        v = float(x)
    except Exception:
        return ""
    return f"{int(round(v)):,.0f}"

def fmt_2(x) -> str:
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return ""
    try:
        v = float(x)
    except Exception:
        return ""
    return f"{v:,.2f}"

def _color(v) -> str:
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return "inherit"
    try:
        v = float(v)
    except Exception:
        return "inherit"
    if v > 0:
        return "green"
    if v < 0:
        return "red"
    return "inherit"

def _table_height(df: pd.DataFrame, row_px: int = 32, header_px: int = 38, max_px: int = 1100) -> int:
    if df is None:
        return 220
    n = int(df.shape[0])
    h = header_px + (n + 1) * row_px
    return int(min(max(h, 220), max_px))

def style_currency_cols(df: pd.DataFrame, diff_cols=None):
    diff_cols = diff_cols or []
    sty = df.style
    # format all non-first columns as currency
    first = df.columns[0]
    fmt = {c: (lambda v: fmt_currency(v)) for c in df.columns if c != first}
    sty = sty.format(fmt)
    for c in diff_cols:
        if c in df.columns:
            sty = sty.applymap(lambda v: f"color: {_color(v)};", subset=[c])
    return sty

# -------------------------
# Vendor map
# -------------------------
def load_vendor_map(path: Path) -> pd.DataFrame:
    df = pd.read_excel(path)
    cols = {c.lower().strip(): c for c in df.columns}

    def pick(name, fallbacks):
        for k in [name] + fallbacks:
            if k in cols:
                return cols[k]
        return None

    c_retail = pick("retailer", [])
    c_sku = pick("sku", ["item", "item sku"])
    c_vendor = pick("vendor", ["supplier"])
    c_price = pick("price", ["unit price", "cost"])

    out = pd.DataFrame({
        "Retailer": df[c_retail] if c_retail else "",
        "SKU": df[c_sku] if c_sku else "",
        "Vendor": df[c_vendor] if c_vendor else "",
        "Price": df[c_price] if c_price else np.nan,
    })

    out["Retailer"] = out["Retailer"].map(_normalize_retailer)
    out["SKU"] = out["SKU"].map(_normalize_sku)
    out["Vendor"] = out["Vendor"].astype(str).str.strip()
    out["Price"] = pd.to_numeric(out["Price"], errors="coerce")

    # preserve order per retailer
    out["MapOrder"] = 0
    for r, grp in out.groupby("Retailer", sort=False):
        for j, ix in enumerate(grp.index.tolist()):
            out.loc[ix, "MapOrder"] = j

    return out

# -------------------------
# Sales store
# -------------------------
def load_sales_store() -> pd.DataFrame:
    if DEFAULT_SALES_STORE.exists():
        df = pd.read_csv(DEFAULT_SALES_STORE)
        for c in ["StartDate", "EndDate"]:
            if c in df.columns:
                df[c] = pd.to_datetime(df[c], errors="coerce")
        df["Retailer"] = df["Retailer"].map(_normalize_retailer)
        df["SKU"] = df["SKU"].map(_normalize_sku)
        df["Units"] = pd.to_numeric(df["Units"], errors="coerce").fillna(0.0)
        return df
    return pd.DataFrame(columns=["Retailer","SKU","Units","StartDate","EndDate","SourceFile"])

def upsert_sales(existing: pd.DataFrame, new_rows: pd.DataFrame) -> pd.DataFrame:
    if existing is None or existing.empty:
        return new_rows.copy()
    if new_rows is None or new_rows.empty:
        return existing.copy()

    for c in ["StartDate","EndDate"]:
        if c in existing.columns:
            existing[c] = pd.to_datetime(existing[c], errors="coerce")
        if c in new_rows.columns:
            new_rows[c] = pd.to_datetime(new_rows[c], errors="coerce")

    key_cols = ["Retailer","SKU","StartDate","EndDate","SourceFile"]
    combined = pd.concat([existing, new_rows], ignore_index=True)
    combined = combined.drop_duplicates(subset=key_cols, keep="last")
    return combined

def append_sales_to_store(new_rows: pd.DataFrame) -> None:
    if new_rows is None or new_rows.empty:
        return
    existing = load_sales_store()
    combined = upsert_sales(existing, new_rows)
    combined.to_csv(DEFAULT_SALES_STORE, index=False)

# -------------------------
# Weekly workbook ingestion
# -------------------------
def parse_date_range_from_filename(name: str, year_hint: int):
    n = name.lower()

    m = re.search(r"(\d{4})[-_/](\d{1,2})[-_/](\d{1,2}).*?(?:thru|through|to|–|-).*?(\d{4})[-_/](\d{1,2})[-_/](\d{1,2})", n)
    if m:
        y1, mo1, d1, y2, mo2, d2 = map(int, m.groups())
        return pd.Timestamp(date(y1, mo1, d1)), pd.Timestamp(date(y2, mo2, d2))

    m = re.search(r"(\d{1,2})[-_/](\d{1,2}).*?(?:thru|through|to|–|-).*?(\d{1,2})[-_/](\d{1,2})", n)
    if m:
        mo1, d1, mo2, d2 = map(int, m.groups())
        y = int(year_hint)
        return pd.Timestamp(date(y, mo1, d1)), pd.Timestamp(date(y, mo2, d2))

    return None, None

def read_weekly_workbook(uploaded_file, year: int) -> pd.DataFrame:
    xls = pd.ExcelFile(uploaded_file)
    fname = getattr(uploaded_file, "name", "upload.xlsx")
    sdt, edt = parse_date_range_from_filename(fname, year_hint=year)
    if sdt is None:
        sdt = pd.Timestamp(date.today() - timedelta(days=7))
        edt = pd.Timestamp(date.today())

    rows = []
    for sh in xls.sheet_names:
        retailer = _normalize_retailer(sh)
        raw = pd.read_excel(xls, sheet_name=sh, header=None)
        if raw.shape[1] < 2:
            continue
        raw = raw.iloc[:, :2].copy()
        raw.columns = ["SKU","Units"]
        raw["SKU"] = raw["SKU"].map(_normalize_sku)
        raw["Units"] = pd.to_numeric(raw["Units"], errors="coerce").fillna(0.0)
        raw = raw[raw["SKU"].astype(str).str.strip().ne("")]

        for _, r in raw.iterrows():
            rows.append({
                "Retailer": retailer,
                "SKU": r["SKU"],
                "Units": float(r["Units"]),
                "StartDate": pd.to_datetime(sdt),
                "EndDate": pd.to_datetime(edt),
                "SourceFile": fname,
            })

    out = pd.DataFrame(rows)
    if not out.empty:
        out["Retailer"] = out["Retailer"].map(_normalize_retailer)
        out["SKU"] = out["SKU"].map(_normalize_sku)
        out["StartDate"] = pd.to_datetime(out["StartDate"], errors="coerce")
        out["EndDate"] = pd.to_datetime(out["EndDate"], errors="coerce")
    return out

# -------------------------
# Enrichment / metrics
# -------------------------
def enrich_sales(sales: pd.DataFrame, vmap: pd.DataFrame) -> pd.DataFrame:
    s = sales.copy()
    s["Retailer"] = s["Retailer"].map(_normalize_retailer)
    s["SKU"] = s["SKU"].map(_normalize_sku)
    s["Units"] = pd.to_numeric(s["Units"], errors="coerce").fillna(0.0).astype(float)

    m = vmap[["Retailer","SKU","Vendor","Price","MapOrder"]].copy()
    m["Retailer"] = m["Retailer"].map(_normalize_retailer)
    m["SKU"] = m["SKU"].map(_normalize_sku)
    m["Price"] = pd.to_numeric(m["Price"], errors="coerce")

    out = s.merge(m, on=["Retailer","SKU"], how="left")
    out["Sales"] = out["Units"] * out["Price"]
    return out

def wow_mom_metrics(df: pd.DataFrame) -> dict:
    out = {"total_units":0.0,"total_sales":0.0,"wow_units":None,"wow_sales":None,"mom_units":None,"mom_sales":None}
    if df is None or df.empty:
        return out
    d = df.copy()
    d["StartDate"] = pd.to_datetime(d["StartDate"], errors="coerce")
    out["total_units"] = float(d["Units"].sum())
    out["total_sales"] = float(d["Sales"].fillna(0).sum())

    periods = sorted(d["StartDate"].dropna().dt.date.unique().tolist())
    if len(periods) >= 1:
        cur_p = periods[-1]
        cur = d[d["StartDate"].dt.date == cur_p]
        cur_u = cur["Units"].sum()
        cur_s = cur["Sales"].fillna(0).sum()
        if len(periods) >= 2:
            prev_p = periods[-2]
            prev = d[d["StartDate"].dt.date == prev_p]
            prev_u = prev["Units"].sum()
            prev_s = prev["Sales"].fillna(0).sum()
        else:
            prev_u = 0.0
            prev_s = 0.0
        out["wow_units"] = float(cur_u - prev_u)
        out["wow_sales"] = float(cur_s - prev_s)

    d["MonthP"] = d["StartDate"].dt.to_period("M")
    months = sorted(d["MonthP"].dropna().unique().tolist())
    if len(months) >= 1:
        cur_m = months[-1]
        cur = d[d["MonthP"] == cur_m]
        cur_u = cur["Units"].sum()
        cur_s = cur["Sales"].fillna(0).sum()
        if len(months) >= 2:
            prev_m = months[-2]
            prev = d[d["MonthP"] == prev_m]
            prev_u = prev["Units"].sum()
            prev_s = prev["Sales"].fillna(0).sum()
        else:
            prev_u = 0.0
            prev_s = 0.0
        out["mom_units"] = float(cur_u - prev_u)
        out["mom_sales"] = float(cur_s - prev_s)

    return out

def month_label(p: pd.Period) -> str:
    return p.to_timestamp().strftime("%B %Y")

# -------------------------
# App UI
# -------------------------
st.set_page_config(page_title=APP_TITLE, layout="wide")
st.title(APP_TITLE)

with st.sidebar:
    st.header("Data Inputs")
    edit_mode = st.checkbox("Enable Edit Mode (edit Vendor/Price)", value=False)

    this_year = date.today().year
    year = st.selectbox("Year (for filename date parsing)", options=list(range(this_year-3, this_year+2)), index=3)

    st.subheader("Vendor Map")
    vm_upload = st.file_uploader("Upload Vendor Map (.xlsx)", type=["xlsx"], key="vm_up")
    a, b = st.columns(2)
    with a:
        if st.button("Use uploaded as default", disabled=vm_upload is None):
            DEFAULT_VENDOR_MAP.write_bytes(vm_upload.getbuffer())
            st.success("Saved as default vendor map.")
            st.rerun()
    with b:
        if st.button("Reload"):
            st.rerun()

    st.subheader("Weekly Sales Workbooks")
    wk_uploads = st.file_uploader("Upload weekly sales workbook(s) (.xlsx)", type=["xlsx"], accept_multiple_files=True, key="wk_up")
    if st.button("Ingest uploads", disabled=not wk_uploads):
        for f in wk_uploads:
            new_rows = read_weekly_workbook(f, year=year)
            append_sales_to_store(new_rows)
        st.success("Ingested uploads into the sales store.")
        st.rerun()

    st.divider()
    if st.button("Clear ALL stored sales data"):
        if DEFAULT_SALES_STORE.exists():
            DEFAULT_SALES_STORE.unlink()
        st.warning("Sales store cleared.")
        st.rerun()

# Load vendor map
if vm_upload is not None:
    tmp = DATA_DIR / "_session_vendor_map.xlsx"
    tmp.write_bytes(vm_upload.getbuffer())
    vmap = load_vendor_map(tmp)
elif DEFAULT_VENDOR_MAP.exists():
    vmap = load_vendor_map(DEFAULT_VENDOR_MAP)
else:
    st.info("Upload a vendor map to begin.")
    st.stop()

sales_store = load_sales_store()
df = enrich_sales(sales_store, vmap)

# KPIs across top
m_all = wow_mom_metrics(df)

st.markdown("## 📊 Overview (All Retailers)")
r1 = st.columns(3)
r2 = st.columns(3)
with r1[0]:
    st.metric("Total Units (YTD)", fmt_int(m_all["total_units"]))
with r1[1]:
    st.metric("Total Sales (YTD)", fmt_currency(m_all["total_sales"]))
with r1[2]:
    st.markdown(
        f"<div style='font-size:14px; color: gray;'>MoM Units</div>"
        f"<div style='font-size:28px; font-weight:600; color:{_color(m_all['mom_units'])};'>{fmt_int(m_all['mom_units']) if m_all['mom_units'] is not None else '—'}</div>",
        unsafe_allow_html=True
    )
with r2[0]:
    st.markdown(
        f"<div style='font-size:14px; color: gray;'>MoM Sales</div>"
        f"<div style='font-size:28px; font-weight:600; color:{_color(m_all['mom_sales'])};'>{fmt_currency(m_all['mom_sales']) if m_all['mom_sales'] is not None else '—'}</div>",
        unsafe_allow_html=True
    )
with r2[1]:
    st.markdown(
        f"<div style='font-size:14px; color: gray;'>WoW Units</div>"
        f"<div style='font-size:28px; font-weight:600; color:{_color(m_all['wow_units'])};'>{fmt_int(m_all['wow_units']) if m_all['wow_units'] is not None else '—'}</div>",
        unsafe_allow_html=True
    )
with r2[2]:
    st.markdown(
        f"<div style='font-size:14px; color: gray;'>WoW Sales</div>"
        f"<div style='font-size:28px; font-weight:600; color:{_color(m_all['wow_sales'])};'>{fmt_currency(m_all['wow_sales']) if m_all['wow_sales'] is not None else '—'}</div>",
        unsafe_allow_html=True
    )

st.divider()

tabs = st.tabs([
    "Retailer Totals",
    "Vendor Totals",
    "Unit Summary",
    "Retailer Scorecard",
    "Vendor Scorecard",
    "No Sales SKUs",
    "Edit Vendor Map",
    "Backup / Restore",
])
(tab_retail_totals, tab_vendor_totals, tab_unit_summary, tab_retail_score, tab_vendor_score, tab_no_sales, tab_edit_map, tab_backup) = tabs

def make_totals_tables(base: pd.DataFrame, group_col: str, tf_weeks: int, avg_weeks: int):
    if base.empty:
        return pd.DataFrame(), pd.DataFrame()
    base = base.copy()
    base["StartDate"] = pd.to_datetime(base["StartDate"], errors="coerce")
    periods = sorted(base["StartDate"].dropna().dt.date.unique().tolist())
    if not periods:
        return pd.DataFrame(), pd.DataFrame()

    use = periods[-tf_weeks:] if len(periods) >= tf_weeks else periods
    d = base[base["StartDate"].dt.date.isin(use)].copy()
    d["Week"] = d["StartDate"].dt.date

    sales_p = d.pivot_table(index=group_col, columns="Week", values="Sales", aggfunc="sum", fill_value=0.0).reindex(columns=use, fill_value=0.0)
    units_p = d.pivot_table(index=group_col, columns="Week", values="Units", aggfunc="sum", fill_value=0.0).reindex(columns=use, fill_value=0.0)

    if len(use) >= 2:
        sales_p["Diff"] = sales_p[use[-1]] - sales_p[use[-2]]
        units_p["Diff"] = units_p[use[-1]] - units_p[use[-2]]
    else:
        sales_p["Diff"] = 0.0
        units_p["Diff"] = 0.0

    avg_use = use[-avg_weeks:] if len(use) >= avg_weeks else use
    sales_p["Avg"] = sales_p[avg_use].replace(0, np.nan).mean(axis=1) if avg_use else 0.0
    units_p["Avg"] = units_p[avg_use].replace(0, np.nan).mean(axis=1) if avg_use else 0.0

    sales_p = sales_p.sort_index()
    units_p = units_p.sort_index()

    sales_p.loc["TOTAL"] = sales_p.sum(axis=0)
    units_p.loc["TOTAL"] = units_p.sum(axis=0)

    def wlab(c):
        try:
            return pd.Timestamp(c).strftime("%m-%d")
        except Exception:
            return c

    sales_p = sales_p.rename(columns={c: wlab(c) for c in sales_p.columns})
    units_p = units_p.rename(columns={c: wlab(c) for c in units_p.columns})

    return sales_p.reset_index(), units_p.reset_index()

# Retailer Totals
with tab_retail_totals:
    st.subheader("Retailer Totals")
    tf = st.selectbox("Timeframe", options=[2,4,8,12], index=1, key="rt_tf")
    avgw = st.selectbox("Average window", options=[4,8,12], index=0, key="rt_avg")

    sales_t, units_t = make_totals_tables(df, "Retailer", tf, avgw)
    if sales_t.empty:
        st.info("No data yet.")
    else:
        st.markdown("### Sales ($) by Week")
        st.dataframe(style_currency_cols(sales_t, diff_cols=["Diff"]), use_container_width=True, height=_table_height(sales_t), hide_index=True)

        st.markdown("### Units by Week")
        ud = units_t.copy()
        first = "Retailer"
        for c in ud.columns:
            if c == first:
                continue
            if c == "Avg":
                ud[c] = ud[c].astype(float)
            else:
                ud[c] = ud[c].map(lambda v: int(round(float(v))) if pd.notna(v) else 0)
        sty = ud.style
        if "Diff" in ud.columns:
            sty = sty.applymap(lambda v: f"color: {_color(v)};", subset=["Diff"])
        fmt = {}
        for c in ud.columns:
            if c == first:
                continue
            fmt[c] = (lambda v: fmt_2(v)) if c == "Avg" else (lambda v: fmt_int(v))
        sty = sty.format(fmt)
        st.dataframe(sty, use_container_width=True, height=_table_height(ud), hide_index=True)

# Vendor Totals
with tab_vendor_totals:
    st.subheader("Vendor Totals")
    tf = st.selectbox("Timeframe", options=[2,4,8,12], index=1, key="vt_tf")
    avgw = st.selectbox("Average window", options=[4,8,12], index=0, key="vt_avg")

    base = df.copy()
    base["Vendor"] = base["Vendor"].fillna("Unmapped")
    sales_t, units_t = make_totals_tables(base, "Vendor", tf, avgw)
    if sales_t.empty:
        st.info("No data yet.")
    else:
        st.markdown("### Sales ($) by Week")
        st.dataframe(style_currency_cols(sales_t, diff_cols=["Diff"]), use_container_width=True, height=_table_height(sales_t, max_px=1400), hide_index=True)

        st.markdown("### Units by Week")
        ud = units_t.copy()
        first = "Vendor"
        for c in ud.columns:
            if c == first:
                continue
            if c == "Avg":
                ud[c] = ud[c].astype(float)
            else:
                ud[c] = ud[c].map(lambda v: int(round(float(v))) if pd.notna(v) else 0)
        sty = ud.style
        if "Diff" in ud.columns:
            sty = sty.applymap(lambda v: f"color: {_color(v)};", subset=["Diff"])
        fmt = {}
        for c in ud.columns:
            if c == first:
                continue
            fmt[c] = (lambda v: fmt_2(v)) if c == "Avg" else (lambda v: fmt_int(v))
        sty = sty.format(fmt)
        st.dataframe(sty, use_container_width=True, height=_table_height(ud, max_px=1400), hide_index=True)

# Unit Summary
with tab_unit_summary:
    st.subheader("Unit Summary")
    retailers = sorted(vmap["Retailer"].dropna().unique().tolist())
    sel_r = st.selectbox("Retailer", options=retailers, index=0, key="us_retailer")
    tf = st.selectbox("Timeframe", options=[2,4,8,12], index=1, key="us_tf")
    avgw = st.selectbox("Average window", options=[4,8,12], index=0, key="us_avg")

    d = df[df["Retailer"] == sel_r].copy()
    d["StartDate"] = pd.to_datetime(d["StartDate"], errors="coerce")
    periods = sorted(d["StartDate"].dropna().dt.date.unique().tolist())
    use = periods[-tf:] if len(periods) >= tf else periods
    if not use:
        st.info("No data for this retailer yet.")
    else:
        d = d[d["StartDate"].dt.date.isin(use)].copy()
        d["Week"] = d["StartDate"].dt.date

        sku_order = vmap[vmap["Retailer"] == sel_r].sort_values("MapOrder")["SKU"].tolist()

        units_p = d.pivot_table(index="SKU", columns="Week", values="Units", aggfunc="sum", fill_value=0.0).reindex(columns=use, fill_value=0.0)
        units_p = units_p.loc[units_p.sum(axis=1) > 0]
        units_p = units_p.reindex([s for s in sku_order if s in units_p.index])

        if units_p.empty:
            st.info("No SKUs sold in this timeframe.")
        else:
            if len(use) >= 2:
                units_p["Diff"] = units_p[use[-1]] - units_p[use[-2]]
            else:
                units_p["Diff"] = 0.0
            avg_use = use[-avgw:] if len(use) >= avgw else use
            units_p["Avg"] = units_p[avg_use].replace(0, np.nan).mean(axis=1) if avg_use else 0.0

            units_p = units_p.rename(columns={c: pd.Timestamp(c).strftime("%m-%d") for c in use})
            units_out = units_p.reset_index()
            total = {"SKU":"TOTAL"}
            for c in units_out.columns:
                if c != "SKU":
                    total[c] = float(units_out[c].sum())
            units_out = pd.concat([units_out, pd.DataFrame([total])], ignore_index=True)

            ud = units_out.copy()
            for c in ud.columns:
                if c == "SKU":
                    continue
                if c == "Avg":
                    ud[c] = ud[c].astype(float)
                else:
                    ud[c] = ud[c].map(lambda v: int(round(float(v))) if pd.notna(v) else 0)
            sty = ud.style
            if "Diff" in ud.columns:
                sty = sty.applymap(lambda v: f"color: {_color(v)};", subset=["Diff"])
            fmt = {}
            for c in ud.columns:
                if c == "SKU":
                    continue
                fmt[c] = (lambda v: fmt_2(v)) if c == "Avg" else (lambda v: fmt_int(v))
            sty = sty.format(fmt)

            st.markdown("### Units by Week (per SKU)")
            st.dataframe(sty, use_container_width=True, height=_table_height(ud, max_px=1400), hide_index=True)

            sales_p = d.pivot_table(index="SKU", columns="Week", values="Sales", aggfunc="sum", fill_value=0.0).reindex(columns=use, fill_value=0.0)
            sales_p = sales_p.loc[sales_p.sum(axis=1) > 0]
            sales_p = sales_p.reindex([s for s in sku_order if s in sales_p.index])

            if len(use) >= 2:
                sales_p["Diff"] = sales_p[use[-1]] - sales_p[use[-2]]
            else:
                sales_p["Diff"] = 0.0
            avg_use = use[-avgw:] if len(use) >= avgw else use
            sales_p["Avg"] = sales_p[avg_use].replace(0, np.nan).mean(axis=1) if avg_use else 0.0

            sales_p = sales_p.rename(columns={c: pd.Timestamp(c).strftime("%m-%d") for c in use})
            sales_out = sales_p.reset_index()
            total = {"SKU":"TOTAL"}
            for c in sales_out.columns:
                if c != "SKU":
                    total[c] = float(sales_out[c].sum())
            sales_out = pd.concat([sales_out, pd.DataFrame([total])], ignore_index=True)

            st.markdown("### Sales ($) by Week (per SKU)")
            st.dataframe(style_currency_cols(sales_out, diff_cols=["Diff"]), use_container_width=True, height=_table_height(sales_out, max_px=1400), hide_index=True)

def monthly_totals(d: pd.DataFrame):
    if d.empty:
        return pd.DataFrame(columns=["Month","Units","Sales"])
    d = d.copy()
    d["StartDate"] = pd.to_datetime(d["StartDate"], errors="coerce")
    d["MonthP"] = d["StartDate"].dt.to_period("M")
    agg = d.groupby("MonthP", as_index=False).agg(Units=("Units","sum"), Sales=("Sales","sum"))
    agg["Month"] = agg["MonthP"].map(month_label)
    agg = agg.sort_values("MonthP")
    return agg[["Month","Units","Sales"]]

def score_kpis(d: pd.DataFrame):
    m = wow_mom_metrics(d)
    left, right = st.columns(2)
    with left:
        st.metric("YTD Units", fmt_int(m["total_units"]))
        st.markdown(
            f"<div style='font-size:13px; color: gray;'>WoW Units</div>"
            f"<div style='font-size:22px; font-weight:600; color:{_color(m['wow_units'])};'>{fmt_int(m['wow_units']) if m['wow_units'] is not None else '—'}</div>",
            unsafe_allow_html=True
        )
        st.markdown(
            f"<div style='font-size:13px; color: gray;'>MoM Units</div>"
            f"<div style='font-size:22px; font-weight:600; color:{_color(m['mom_units'])};'>{fmt_int(m['mom_units']) if m['mom_units'] is not None else '—'}</div>",
            unsafe_allow_html=True
        )
    with right:
        st.metric("YTD Sales", fmt_currency(m["total_sales"]))
        st.markdown(
            f"<div style='font-size:13px; color: gray;'>WoW Sales</div>"
            f"<div style='font-size:22px; font-weight:600; color:{_color(m['wow_sales'])};'>{fmt_currency(m['wow_sales']) if m['wow_sales'] is not None else '—'}</div>",
            unsafe_allow_html=True
        )
        st.markdown(
            f"<div style='font-size:13px; color: gray;'>MoM Sales</div>"
            f"<div style='font-size:22px; font-weight:600; color:{_color(m['mom_sales'])};'>{fmt_currency(m['mom_sales']) if m['mom_sales'] is not None else '—'}</div>",
            unsafe_allow_html=True
        )
    st.divider()

# Retailer Scorecard
with tab_retail_score:
    st.subheader("Retailer Scorecard")
    retailers = sorted(vmap["Retailer"].dropna().unique().tolist())
    sel = st.selectbox("Retailer", options=retailers, index=0, key="rs_retailer")
    d = df[df["Retailer"] == sel].copy()

    score_kpis(d)

    months_opt = st.selectbox("Monthly window", options=[3,6,12], index=0, key="rs_months")
    monthly = monthly_totals(d).tail(months_opt)
    if not monthly.empty:
        mon = monthly.copy()
        mon["Units"] = mon["Units"].map(lambda v: int(round(float(v))) if pd.notna(v) else 0)
        st.markdown("### Monthly Totals")
        st.dataframe(mon.style.format({"Units": lambda v: fmt_int(v), "Sales": lambda v: fmt_currency(v)}),
                     use_container_width=True, height=_table_height(mon), hide_index=True)

    agg = d.groupby(["SKU","Vendor"], as_index=False).agg(Units=("Units","sum"), Sales=("Sales","sum"))
    agg["Vendor"] = agg["Vendor"].fillna("Unmapped")

    top_n = 10
    bot_n = 15

    top_units = agg.sort_values(["Units","SKU"], ascending=[False, True]).head(top_n)[["SKU","Units"]]
    bot_units = agg.sort_values(["Units","SKU"], ascending=[True, True]).head(bot_n)[["SKU","Units"]]
    top_sales = agg.sort_values(["Sales","SKU"], ascending=[False, True]).head(top_n)[["SKU","Sales"]]
    bot_sales = agg.sort_values(["Sales","SKU"], ascending=[True, True]).head(bot_n)[["SKU","Sales"]]

    st.markdown("### Top / Bottom SKUs (YTD)")
    c1, c2 = st.columns(2)
    with c1:
        st.markdown("#### Top 10 by Units")
        tu = top_units.copy()
        tu["Units"] = tu["Units"].map(lambda v: int(round(float(v))) if pd.notna(v) else 0)
        st.dataframe(tu.style.format({"Units": lambda v: fmt_int(v)}), use_container_width=True, height=_table_height(tu), hide_index=True)

        st.markdown("#### Bottom 15 by Units")
        bu = bot_units.copy()
        bu["Units"] = bu["Units"].map(lambda v: int(round(float(v))) if pd.notna(v) else 0)
        st.dataframe(bu.style.format({"Units": lambda v: fmt_int(v)}), use_container_width=True, height=_table_height(bu), hide_index=True)
    with c2:
        st.markdown("#### Top 10 by Sales")
        st.dataframe(top_sales.style.format({"Sales": lambda v: fmt_currency(v)}), use_container_width=True, height=_table_height(top_sales), hide_index=True)

        st.markdown("#### Bottom 15 by Sales")
        st.dataframe(bot_sales.style.format({"Sales": lambda v: fmt_currency(v)}), use_container_width=True, height=_table_height(bot_sales), hide_index=True)

# Vendor Scorecard
with tab_vendor_score:
    st.subheader("Vendor Scorecard")
    vendors = sorted([v for v in vmap["Vendor"].dropna().unique().tolist() if str(v).strip() != ""])
    sel = st.selectbox("Vendor", options=vendors, index=0, key="vs_vendor")
    d = df[df["Vendor"] == sel].copy()

    score_kpis(d)

    months_opt = st.selectbox("Monthly window", options=[3,6,12], index=0, key="vs_months")
    monthly = monthly_totals(d).tail(months_opt)
    if not monthly.empty:
        mon = monthly.copy()
        mon["Units"] = mon["Units"].map(lambda v: int(round(float(v))) if pd.notna(v) else 0)
        st.markdown("### Monthly Totals")
        st.dataframe(mon.style.format({"Units": lambda v: fmt_int(v), "Sales": lambda v: fmt_currency(v)}),
                     use_container_width=True, height=_table_height(mon), hide_index=True)

    agg = d.groupby(["SKU","Retailer"], as_index=False).agg(Units=("Units","sum"), Sales=("Sales","sum"))

    top_n = 10
    bot_n = 15

    top_units = agg.sort_values(["Units","SKU"], ascending=[False, True]).head(top_n)[["SKU","Retailer","Units"]]
    bot_units = agg.sort_values(["Units","SKU"], ascending=[True, True]).head(bot_n)[["SKU","Retailer","Units"]]
    top_sales = agg.sort_values(["Sales","SKU"], ascending=[False, True]).head(top_n)[["SKU","Retailer","Sales"]]
    bot_sales = agg.sort_values(["Sales","SKU"], ascending=[True, True]).head(bot_n)[["SKU","Retailer","Sales"]]

    st.markdown("### Top / Bottom SKUs (YTD)")
    c1, c2 = st.columns(2)
    with c1:
        st.markdown("#### Top 10 by Units")
        tu = top_units.copy()
        tu["Units"] = tu["Units"].map(lambda v: int(round(float(v))) if pd.notna(v) else 0)
        st.dataframe(tu.style.format({"Units": lambda v: fmt_int(v)}), use_container_width=True, height=_table_height(tu), hide_index=True)

        st.markdown("#### Bottom 15 by Units")
        bu = bot_units.copy()
        bu["Units"] = bu["Units"].map(lambda v: int(round(float(v))) if pd.notna(v) else 0)
        st.dataframe(bu.style.format({"Units": lambda v: fmt_int(v)}), use_container_width=True, height=_table_height(bu), hide_index=True)
    with c2:
        st.markdown("#### Top 10 by Sales")
        st.dataframe(top_sales.style.format({"Sales": lambda v: fmt_currency(v)}), use_container_width=True, height=_table_height(top_sales), hide_index=True)

        st.markdown("#### Bottom 15 by Sales")
        st.dataframe(bot_sales.style.format({"Sales": lambda v: fmt_currency(v)}), use_container_width=True, height=_table_height(bot_sales), hide_index=True)

# No Sales SKUs
with tab_no_sales:
    st.subheader("No Sales SKUs")
    weeks = st.selectbox("Timeframe (weeks)", options=[3,6,8,12], index=0, key="ns_weeks")
    retailers = sorted(vmap["Retailer"].dropna().unique().tolist())
    sel_r = st.selectbox("Retailer", options=["All"] + retailers, index=0, key="ns_retailer")

    if df.empty:
        st.info("No sales data yet.")
    else:
        d2 = df.copy()
        d2["StartDate"] = pd.to_datetime(d2["StartDate"], errors="coerce")
        periods = sorted(d2["StartDate"].dropna().dt.date.unique().tolist())
        use = periods[-weeks:] if len(periods) >= weeks else periods

        if not use:
            st.info("No periods found yet.")
        else:
            sold = d2[d2["StartDate"].dt.date.isin(use)].groupby(["Retailer","SKU"], as_index=False).agg(Units=("Units","sum"), Sales=("Sales","sum"))
            ref = vmap[["Retailer","SKU","Vendor","MapOrder"]].copy()
            if sel_r != "All":
                ref = ref[ref["Retailer"] == sel_r].copy()

            merged = ref.merge(sold, on=["Retailer","SKU"], how="left")
            merged["Units"] = merged["Units"].fillna(0.0)
            merged["Sales"] = merged["Sales"].fillna(0.0)

            nos = merged[(merged["Units"] <= 0) & (merged["Sales"] <= 0)].copy()
            nos["Status"] = f"No sales in last {weeks} weeks"
            nos = nos.sort_values(["Retailer","MapOrder","SKU"], ascending=[True, True, True])

            out = nos[["Retailer","Vendor","SKU","Status"]].copy()
            st.dataframe(out, use_container_width=True, height=_table_height(out, max_px=1400), hide_index=True)

# Edit Vendor Map
with tab_edit_map:
    st.subheader("Edit Vendor Map")
    st.caption("Edit Vendor and Price. Click Save to update the default vendor map file used by the app.")
    vmap_disp = vmap[["Retailer","SKU","Vendor","Price","MapOrder"]].copy().sort_values(["Retailer","MapOrder"])
    show = vmap_disp.drop(columns=["MapOrder"]).copy()

    if edit_mode:
        edited = st.data_editor(show, use_container_width=True, hide_index=True, num_rows="dynamic")
        if st.button("Save Vendor Map"):
            updated = edited.copy()
            updated["Retailer"] = updated["Retailer"].map(_normalize_retailer)
            updated["SKU"] = updated["SKU"].map(_normalize_sku)
            updated["Vendor"] = updated["Vendor"].astype(str).str.strip()
            updated["Price"] = pd.to_numeric(updated["Price"], errors="coerce")

            # MapOrder based on current row order per retailer
            updated["MapOrder"] = 0
            for r, grp in updated.groupby("Retailer", sort=False):
                for j, ix in enumerate(grp.index.tolist()):
                    updated.loc[ix, "MapOrder"] = j

            updated.to_excel(DEFAULT_VENDOR_MAP, index=False)
            st.success("Saved vendor map. Reloading…")
            st.rerun()
    else:
        st.info("Turn on Edit Mode in the sidebar to edit.")
        st.dataframe(show, use_container_width=True, height=_table_height(show, max_px=1400), hide_index=True)

# Backup / Restore
with tab_backup:
    st.subheader("Backup / Restore")

    a, b = st.columns(2)
    with a:
        st.markdown("### Download Backup Database")
        if DEFAULT_SALES_STORE.exists():
            st.download_button("Download sales_store.csv", data=DEFAULT_SALES_STORE.read_bytes(), file_name="sales_store.csv", mime="text/csv")
        else:
            st.info("No database yet.")
    with b:
        st.markdown("### Restore Backup Database")
        up = st.file_uploader("Upload sales_store.csv", type=["csv"], key="restore_csv")
        if st.button("Restore now", disabled=up is None):
            DEFAULT_SALES_STORE.write_bytes(up.getbuffer())
            st.success("Restored. Reloading…")
            st.rerun()

    st.divider()
    st.markdown("### Export Enriched Sales")
    if not df.empty:
        ex = df.copy()
        ex["StartDate"] = ex["StartDate"].dt.strftime("%Y-%m-%d")
        ex["EndDate"] = ex["EndDate"].dt.strftime("%Y-%m-%d")
        st.download_button("Download enriched_sales.csv", data=ex.to_csv(index=False).encode("utf-8"),
                           file_name="enriched_sales.csv", mime="text/csv")
    else:
        st.info("No sales yet.")


import streamlit as st
import pandas as pd
import numpy as np
import re
from pathlib import Path
from datetime import date, timedelta

APP_TITLE = "Sales Dashboard (Vendor Map + Weekly Sheets)"
DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

DEFAULT_VENDOR_MAP = DATA_DIR / "vendor_map.xlsx"
DEFAULT_SALES_STORE = DATA_DIR / "sales_store.csv"

# -------------------------
# Helpers
# -------------------------

def fmt_currency(x):
    """Format number as currency like $1,234.56 and negatives as ($1,234.56)."""
    if x is None or (isinstance(x, float) and np.isnan(x)) or (isinstance(x, (pd.Series, pd.DataFrame))):
        # avoid accidental misuse; Series/DataFrame should be formatted via styling
        return "" if x is None else x
    try:
        v = float(x)
    except Exception:
        return ""
    s = f"${abs(v):,.2f}"
    return f"({s})" if v < 0 else s

def fmt_int(x):
    """Format as integer with commas (no decimals)."""
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return ""
    try:
        v = float(x)
    except Exception:
        return ""
    return f"{int(round(v)):,.0f}"

def fmt_2(x):
    """Format as number with 2 decimals and commas."""
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return ""
    try:
        v = float(x)
    except Exception:
        return ""
    return f"{v:,.2f}"

def _normalize_retailer(x: str) -> str:
    if x is None:
        return ""
    x = str(x).strip()
    aliases = {
        "home depot": "Depot",
        "depot": "Depot",
        "lowes": "Lowe's",
        "lowe's": "Lowe's",
        "amazon": "Amazon",
        "tractor supply": "Tractor Supply",
        "tsc": "Tractor Supply",
        "depot so": "Depot SO",
    }
    key = re.sub(r"\s+", " ", x.lower()).strip()
    return aliases.get(key, x)

def _normalize_sku(x: str) -> str:
    if pd.isna(x):
        return ""
    return str(x).strip()

def _safe_to_float(x):
    try:
        if pd.isna(x):
            return np.nan
        return float(x)
    except Exception:
        return np.nan

def parse_filename_dates(filename: str, year: int):
    """
    Supports patterns like:
      'APP 1-1 thru 1-2.xlsx'
      '1-07 to 1-13'
      '2026-01-01 thru 2026-01-07'
    Returns (start_date, end_date) or (None, None) if not found.
    """
    name = Path(filename).stem.lower()
    name = name.replace("_", " ").replace(".", " ")

    full = re.findall(r"(20\d{2})[\/\-\.](\d{1,2})[\/\-\.](\d{1,2})", name)
    if len(full) >= 2:
        s = date(int(full[0][0]), int(full[0][1]), int(full[0][2]))
        e = date(int(full[1][0]), int(full[1][1]), int(full[1][2]))
        return s, e
    if len(full) == 1:
        d = date(int(full[0][0]), int(full[0][1]), int(full[0][2]))
        return d, d

    md = re.findall(r"(\d{1,2})[\/\-\.](\d{1,2})", name)
    if len(md) >= 2 and ("thru" in name or "through" in name or "to" in name):
        s = date(year, int(md[0][0]), int(md[0][1]))
        e = date(year, int(md[1][0]), int(md[1][1]))
        return s, e
    if len(md) == 1:
        d = date(year, int(md[0][0]), int(md[0][1]))
        return d, d

    return None, None

@st.cache_data(show_spinner=False)
def load_vendor_map(path: Path) -> pd.DataFrame:
    df = pd.read_excel(path)
    required = {"Retailer","SKU","Vendor"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Vendor map is missing columns: {sorted(missing)}. Found: {list(df.columns)}")
    if "Price" not in df.columns:
        df["Price"] = np.nan
    df = df.copy()
    df["Retailer"] = df["Retailer"].map(_normalize_retailer)
    df["SKU"] = df["SKU"].map(_normalize_sku)
    df["Vendor"] = df["Vendor"].astype(str).str.strip()
    df["Price"] = df["Price"].map(_safe_to_float)
    return df

def read_weekly_workbook(file, year: int) -> pd.DataFrame:
    """
    Expects each sheet = retailer name, two columns:
      col0: SKU
      col1: Units
    No headers in sheet (header=None).
    """
    xls = pd.ExcelFile(file)
    start_d, end_d = parse_filename_dates(getattr(file, "name", "uploaded.xlsx"), year)
    rows = []
    for sh in xls.sheet_names:
        try:
            df = pd.read_excel(xls, sheet_name=sh, header=None)
        except Exception:
            continue
        if df.shape[1] < 2 or df.empty:
            continue
        df = df.iloc[:, :2]
        df.columns = ["SKU", "Units"]
        df["SKU"] = df["SKU"].map(_normalize_sku)
        df["Units"] = pd.to_numeric(df["Units"], errors="coerce").fillna(0).astype(float)
        df = df[df["SKU"].astype(str).str.strip().ne("")]
        df["Retailer"] = _normalize_retailer(sh)
        df["StartDate"] = pd.to_datetime(start_d)
        df["EndDate"] = pd.to_datetime(end_d)
        df["SourceFile"] = getattr(file, "name", "uploaded.xlsx")
        rows.append(df)
    if not rows:
        return pd.DataFrame(columns=["Retailer","SKU","Units","StartDate","EndDate","SourceFile"])
    out = pd.concat(rows, ignore_index=True)
    return out

def load_sales_store() -> pd.DataFrame:
    if DEFAULT_SALES_STORE.exists():
        try:
            df = pd.read_csv(DEFAULT_SALES_STORE)
            for c in ["StartDate","EndDate"]:
                if c in df.columns:
                    df[c] = pd.to_datetime(df[c], errors="coerce")
            return df
        except Exception:
            return pd.DataFrame()
    return pd.DataFrame()

def save_sales_store(df: pd.DataFrame) -> None:
    if df is None or df.empty:
        return
    df.to_csv(DEFAULT_SALES_STORE, index=False)

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

def enrich_sales(sales: pd.DataFrame, vmap: pd.DataFrame) -> pd.DataFrame:
    if sales is None or sales.empty:
        return pd.DataFrame(columns=["Retailer","Vendor","SKU","Units","Price","Sales","StartDate","EndDate","SourceFile"])
    s = sales.copy()
    s["Retailer"] = s["Retailer"].map(_normalize_retailer)
    s["SKU"] = s["SKU"].map(_normalize_sku)

    m = vmap[["Retailer","SKU","Vendor","Price"]].copy()
    m["Retailer"] = m["Retailer"].map(_normalize_retailer)
    m["SKU"] = m["SKU"].map(_normalize_sku)

    out = s.merge(m, on=["Retailer","SKU"], how="left")
    out["Units"] = pd.to_numeric(out["Units"], errors="coerce").fillna(0).astype(float)
    out["Sales"] = out["Units"] * out["Price"]
    return out

def period_len_days(df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    s = df["StartDate"].dropna()
    e = df["EndDate"].dropna()
    if s.empty or e.empty:
        return 0
    return (max(e) - min(s)).days + 1

def latest_period(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    if df["EndDate"].notna().any():
        latest_end = df["EndDate"].dropna().max()
        return df[df["EndDate"] == latest_end].copy()
    return df

def previous_period(df: pd.DataFrame, days: int) -> pd.DataFrame:
    if df.empty or days <= 0:
        return df.iloc[0:0].copy()
    if not df["EndDate"].notna().any():
        return df.iloc[0:0].copy()
    latest_end = df["EndDate"].dropna().max()
    prev_end = latest_end - timedelta(days=days)
    return df[df["EndDate"] == prev_end].copy()

def add_month(df: pd.DataFrame) -> pd.DataFrame:
    x = df.copy()
    x["Month"] = pd.to_datetime(x["EndDate"], errors="coerce").dt.to_period("M").astype(str)
    return x

def agg_vendor(df: pd.DataFrame) -> pd.DataFrame:
    g = df.groupby("Vendor", dropna=False, as_index=False).agg(Units=("Units","sum"), Sales=("Sales","sum"))
    g["Sales"] = g["Sales"].fillna(0.0)
    return g.sort_values("Sales", ascending=False)

def agg_retailer(df: pd.DataFrame) -> pd.DataFrame:
    g = df.groupby("Retailer", dropna=False, as_index=False).agg(Units=("Units","sum"), Sales=("Sales","sum"))
    g["Sales"] = g["Sales"].fillna(0.0)
    return g.sort_values("Sales", ascending=False)

def agg_sku(df: pd.DataFrame, sku_order_map: dict | None = None) -> pd.DataFrame:
    g = df.groupby(["Retailer","SKU"], dropna=False, as_index=False).agg(
        Units=("Units","sum"),
        Sales=("Sales","sum"),
        Vendor=("Vendor", lambda x: x.dropna().iloc[0] if len(x.dropna()) else np.nan),
        Price=("Price", lambda x: x.dropna().iloc[0] if len(x.dropna()) else np.nan),
    )
    g["Sales"] = g["Sales"].fillna(0.0)

    # Sort Retailer/Vendor names alphabetically, and SKUs in the same order as the vendor map (per retailer)
    if sku_order_map:
        # Build rank map per retailer for fast ordering
        ranks = {}
        for r, skus in sku_order_map.items():
            ranks[r] = {s: i for i, s in enumerate(skus)}
        def _rank(row):
            r = row["Retailer"]
            s = row["SKU"]
            return ranks.get(r, {}).get(s, 10**9)
        g["_sku_rank"] = g.apply(_rank, axis=1)
        g = g.sort_values(["Retailer","_sku_rank","SKU"], ascending=[True, True, True]).drop(columns=["_sku_rank"])
    else:
        g = g.sort_values(["Retailer","SKU"], ascending=[True, True])
    return g

# -------------------------

# UI
# -------------------------
def _sorted_periods(df: pd.DataFrame):
    if df is None or df.empty or "StartDate" not in df.columns:
        return []
    p = pd.to_datetime(df["StartDate"], errors="coerce")
    p = p.dropna().dt.date.unique().tolist()
    return sorted(p)

def _format_period(d: date) -> str:
    if d is None or pd.isna(d):
        return ""
    # show M/D for readability; include year if it changes
    try:
        return pd.to_datetime(d).strftime("%-m/%-d")
    except Exception:
        return str(d)


def _is_number(x):
    try:
        return pd.notna(x) and isinstance(float(x), (float,int))
    except Exception:
        return False

def _diff_color(val):
    try:
        v = float(val)
    except Exception:
        return ""
    if v > 0:
        return "color: green;"
    if v < 0:
        return "color: red;"
    return ""

def _table_height(df, row_height: int = 32, header_height: int = 38, min_height: int = 160, max_height: int = 900) -> int:
    """Return a reasonable Streamlit dataframe height so tables show many rows without internal scrolling."""
    try:
        n = int(len(df)) if df is not None else 0
    except Exception:
        n = 0
    h = header_height + row_height * (n + 1)
    if h < min_height:
        h = min_height
    if h > max_height:
        h = max_height
    return int(h)

def style_currency_table(df: pd.DataFrame, diff_like_cols=None):
    """Currency formatting: $1,234.56 and red/green for diff-like cols."""
    if df is None or df.empty:
        return df
    diff_like_cols = diff_like_cols or []
    num_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
    sty = df.style.format({c: "${:,.2f}" for c in num_cols})
    # color diff-like cols
    cols_to_color = [c for c in df.columns if c in diff_like_cols]
    if cols_to_color:
        sty = sty.applymap(_diff_color, subset=cols_to_color)
    return sty

def style_number_table(df: pd.DataFrame, diff_like_cols=None):
    """Number formatting: 1,234.56 and red/green for diff-like cols."""
    if df is None or df.empty:
        return df
    diff_like_cols = diff_like_cols or []
    num_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
    sty = df.style.format({c: "{:,.2f}" for c in num_cols})
    cols_to_color = [c for c in df.columns if c in diff_like_cols]
    if cols_to_color:
        sty = sty.applymap(_diff_color, subset=cols_to_color)
    return sty




def style_units_wide_table(df: pd.DataFrame, diff_like_cols=None, avg_col_name: str = "Avg"):
    """Units wide tables: show weekly + Diff as integers (no decimals); Avg keeps 2 decimals."""
    if df is None or df.empty:
        return df
    diff_like_cols = diff_like_cols or []
    fmt = {}
    for c in df.columns:
        if pd.api.types.is_numeric_dtype(df[c]):
            if c == avg_col_name:
                fmt[c] = "{:,.2f}"
            else:
                fmt[c] = "{:,.0f}"
    sty = df.style.format(fmt)
    cols_to_color = [c for c in df.columns if c in diff_like_cols]
    if cols_to_color:
        sty = sty.applymap(_diff_color, subset=cols_to_color)
    return sty

def style_units_only_table(df: pd.DataFrame, units_col: str = "Units"):
    """Format a two-column (SKU, Units) table with integer units."""
    if df is None or df.empty:
        return df
    fmt = {}
    if units_col in df.columns and pd.api.types.is_numeric_dtype(df[units_col]):
        fmt[units_col] = "{:,.0f}"
    return df.style.format(fmt)

def style_units_sales_table(df: pd.DataFrame, diff_like_cols=None):
    """Format Units as number and Sales as currency; optionally color diff-like cols."""
    if df is None or df.empty:
        return df
    diff_like_cols = diff_like_cols or []
    fmt = {}
    if "Units" in df.columns and pd.api.types.is_numeric_dtype(df["Units"]):
        fmt["Units"] = "{:,.2f}"
    if "Sales" in df.columns and pd.api.types.is_numeric_dtype(df["Sales"]):
        fmt["Sales"] = "${:,.2f}"
    sty = df.style.format(fmt)
    cols_to_color = [c for c in df.columns if c in diff_like_cols]
    if cols_to_color:
        sty = sty.applymap(_diff_color, subset=cols_to_color)
    return sty



def build_wide_totals(df: pd.DataFrame, index_col: str, value_col: str, periods: list[date], avg_weeks: int):
    """Return a wide table: index_col + period columns + Diff + Avg."""
    if df is None or df.empty:
        return pd.DataFrame()

    tmp = df.copy()
    tmp["StartDate"] = pd.to_datetime(tmp["StartDate"], errors="coerce").dt.date
    wide = tmp.pivot_table(index=index_col, columns="StartDate", values=value_col, aggfunc="sum", fill_value=0.0)

    # Keep requested periods (already sorted)
    cols = [p for p in periods if p in wide.columns]
    wide = wide[cols] if cols else wide.iloc[:, 0:0]

    # Add Diff (last - prev)
    if wide.shape[1] >= 2:
        diff = wide.iloc[:, -1] - wide.iloc[:, -2]
    elif wide.shape[1] == 1:
        diff = wide.iloc[:, -1]
    else:
        diff = 0.0
    wide["Diff"] = diff

    # Add Avg over last avg_weeks columns
    if wide.shape[1] > 0:
        base_cols = [c for c in wide.columns if c != "Diff"]
        use_cols = base_cols[-min(len(base_cols), max(1, avg_weeks)):]
        wide["Avg"] = wide[use_cols].mean(axis=1)
    else:
        wide["Avg"] = 0.0

    # Rename period columns to friendly labels
    rename = {c: _format_period(c) for c in wide.columns if isinstance(c, date)}
    wide = wide.rename(columns=rename)

    wide = wide.reset_index()

    # Sort by latest period value if exists
    period_labels = [ _format_period(p) for p in cols ]
    if period_labels:
        wide = wide.sort_values(period_labels[-1], ascending=False)
    return wide


def add_total_row(wide: pd.DataFrame, name_col: str, total_label: str = "TOTAL") -> pd.DataFrame:
    """Append a TOTAL row summing numeric columns."""
    if wide is None or wide.empty:
        return wide
    out = wide.copy()
    num_cols = [c for c in out.columns if c != name_col]
    # Only sum numeric columns
    sums = {}
    for c in num_cols:
        if pd.api.types.is_numeric_dtype(out[c]):
            sums[c] = out[c].sum()
        else:
            # leave blank for non-numeric
            sums[c] = np.nan
    total_row = {name_col: total_label, **sums}
    out = pd.concat([out, pd.DataFrame([total_row])], ignore_index=True)
    return out


def reorder_skus_for_retailer(wide: pd.DataFrame, retailer: str, sku_order_map: dict) -> pd.DataFrame:
    """Reorder wide SKU tables to match the vendor-map SKU order for the selected retailer."""
    if wide is None or wide.empty:
        return wide
    if not sku_order_map or retailer not in sku_order_map:
        return wide
    order = sku_order_map.get(retailer, [])
    name_col = "SKU" if "SKU" in wide.columns else wide.columns[0]
    # Keep only rows present, in the vendor-map order; append any extras at the end (alphabetical)
    present = wide[name_col].tolist()
    ordered = [s for s in order if s in present]
    extras = sorted([s for s in present if s not in set(ordered)])
    final = ordered + extras
    cat = pd.Categorical(wide[name_col], categories=final, ordered=True)
    out = wide.copy()
    out["_ord"] = cat
    out = out.sort_values("_ord").drop(columns=["_ord"])
    return out

def month_table(df: pd.DataFrame, group_col: str):
    """Month totals for a filtered df: Month, Units, Sales (Month shown as name)."""
    if df is None or df.empty:
        return pd.DataFrame(columns=["Month","Units","Sales"])
    d = df.copy()
    mdt = pd.to_datetime(d["StartDate"], errors="coerce").dt.to_period("M").dt.to_timestamp()
    d["_month_dt"] = mdt
    g = d.groupby("_month_dt", as_index=False).agg(Units=("Units","sum"), Sales=("Sales","sum"))
    g["Sales"] = g["Sales"].fillna(0.0)
    g = g.sort_values("_month_dt")
    g["Month"] = g["_month_dt"].dt.strftime("%B")
    # If multiple years exist, disambiguate
    if g["Month"].duplicated().any():
        g["Month"] = g["_month_dt"].dt.strftime("%B %Y")
    g = g.drop(columns=["_month_dt"])
    # Ensure Month is the first column
    return g[["Month","Units","Sales"]]

def wow_mom_metrics(df: pd.DataFrame):
    """Return dict with YTD units/sales and WoW/MoM deltas."""
    out = {"ytd_units":0.0,"ytd_sales":0.0,"wow_units":None,"wow_sales":None,"mom_units":None,"mom_sales":None}
    if df is None or df.empty:
        return out

    d = df.copy()
    d["StartDate"] = pd.to_datetime(d["StartDate"], errors="coerce")
    out["ytd_units"] = float(d["Units"].sum())
    out["ytd_sales"] = float(d["Sales"].fillna(0).sum())

    # WoW: last two periods
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

    # MoM: last month vs previous month
    d["Month"] = d["StartDate"].dt.to_period("M")
    months = sorted(d["Month"].dropna().unique().tolist())
    if len(months) >= 1:
        cur_m = months[-1]
        cur = d[d["Month"] == cur_m]
        cur_u = cur["Units"].sum()
        cur_s = cur["Sales"].fillna(0).sum()
        if len(months) >= 2:
            prev_m = months[-2]
            prev = d[d["Month"] == prev_m]
            prev_u = prev["Units"].sum()
            prev_s = prev["Sales"].fillna(0).sum()
        else:
            prev_u = 0.0
            prev_s = 0.0
        out["mom_units"] = float(cur_u - prev_u)
        out["mom_sales"] = float(cur_s - prev_s)

    return out

st.set_page_config(page_title=APP_TITLE, layout="wide")
st.title(APP_TITLE)

with st.sidebar:
    st.header("Data Inputs")

    this_year = date.today().year
    year = st.selectbox("Year (for filename date parsing)", options=list(range(this_year-3, this_year+2)), index=3)

    st.subheader("Vendor Map")
    vm_upload = st.file_uploader("Upload Vendor Map (.xlsx)", type=["xlsx"], key="vm_up")
    col_a, col_b = st.columns(2)
    with col_a:
        if st.button("Use uploaded as default", disabled=vm_upload is None):
            DEFAULT_VENDOR_MAP.write_bytes(vm_upload.getbuffer())
            st.success("Saved vendor map as default.")
            st.cache_data.clear()
    with col_b:
        if st.button("Reset cache"):
            st.cache_data.clear()
            st.toast("Cache cleared")

    st.caption(f"Current default vendor map: {DEFAULT_VENDOR_MAP if DEFAULT_VENDOR_MAP.exists() else 'None'}")

    st.subheader("Weekly Sales Workbooks")
    sales_uploads = st.file_uploader(
        "Upload weekly workbook(s) (.xlsx). Each sheet name = retailer; 2 columns = SKU, Units.",
        type=["xlsx"],
        accept_multiple_files=True,
        key="sales_up"
    )

    if st.button("Ingest uploads", disabled=not sales_uploads):
        if not DEFAULT_VENDOR_MAP.exists() and vm_upload is None:
            st.error("Upload a vendor map first (or save one as default).")
        else:
            existing = load_sales_store()
            all_new = []
            for f in sales_uploads:
                all_new.append(read_weekly_workbook(f, year=year))
            new_rows = pd.concat(all_new, ignore_index=True) if all_new else pd.DataFrame()
            combined = upsert_sales(existing, new_rows)
            save_sales_store(combined)
            st.success(f"Ingested {len(new_rows):,} rows. Store now has {len(combined):,} rows.")

    st.divider()
    if st.button("Clear ALL stored sales data"):
        if DEFAULT_SALES_STORE.exists():
            DEFAULT_SALES_STORE.unlink()
        st.warning("Sales store cleared.")

# Load vendor map
if vm_upload is not None:
    tmp_vm_path = DATA_DIR / "_session_vendor_map.xlsx"
    tmp_vm_path.write_bytes(vm_upload.getbuffer())
    vmap = load_vendor_map(tmp_vm_path)
elif DEFAULT_VENDOR_MAP.exists():
    vmap = load_vendor_map(DEFAULT_VENDOR_MAP)
else:
    st.info("Upload a vendor map in the sidebar to begin.")
    st.stop()


# Preserve SKU display order as it appears in the vendor map (per retailer)
sku_order_map = vmap.groupby('Retailer', sort=False)['SKU'].apply(list).to_dict()

sales_store = load_sales_store()
enriched = enrich_sales(sales_store, vmap)


# Global filters removed per request (use dedicated Totals/Scorecard tabs instead)
df = enriched.copy()
unmapped_ct = int((enriched["Vendor"].isna() | enriched["Price"].isna()).sum()) if not enriched.empty else 0
if unmapped_ct:
    st.warning(f"{unmapped_ct:,} row(s) are missing Vendor and/or Price after mapping. Use the 'Unmapped SKUs' tab to review.")

cur_period = latest_period(df)
days = period_len_days(cur_period)
prev_period = previous_period(df, days=days) if days else df.iloc[0:0].copy()

df_m = add_month(df) if not df.empty else df
if not df_m.empty and df_m["Month"].notna().any():
    latest_month = sorted(df_m["Month"].dropna().unique())[-1]
    cur_month_df = df_m[df_m["Month"] == latest_month]
    prev_month = (pd.Period(latest_month) - 1).strftime("%Y-%m")
    prev_month_df = df_m[df_m["Month"] == prev_month]
else:
    cur_month_df = df.iloc[0:0].copy()
    prev_month_df = df.iloc[0:0].copy()

tab_retail_totals, tab_vendor_totals, tab_unit_summary, tab_retail_scorecard, tab_vendor_scorecard, tab_skus, tab_unmapped, tab_no_sales, tab_backup = st.tabs(
    ["Retailer Totals", "Vendor Totals", "Unit Summary", "Retailer Scorecard", "Vendor Scorecard", "SKUs", "Unmapped SKUs", "No Sales SKUs", "Backup / Restore"]
)


with tab_retail_totals:
    st.markdown("### Retailer Totals (by week)")

    tf = st.selectbox("Timeframe (weeks)", options=[2,4,8,12], index=2, key="retail_totals_tf")
    avg_n = st.selectbox("Average over last (weeks)", options=[4,8,12], index=0, key="retail_totals_avg")

    if df.empty or df["Retailer"].dropna().empty:
        st.info("No data available yet. Upload weekly sheets to populate totals.")
    else:
        periods = _sorted_periods(df)
        use_periods = periods[-min(len(periods), tf):]

        st.markdown("#### Sales ($) by Retailer")
        wide_sales = build_wide_totals(df, "Retailer", "Sales", use_periods, avg_n)
        if not wide_sales.empty:
            wide_sales = wide_sales.sort_values("Retailer", ascending=True)
            wide_sales = add_total_row(wide_sales, "Retailer")
        st.dataframe(style_currency_table(wide_sales, diff_like_cols=['Diff']), use_container_width=True, height=_table_height(wide_sales, max_height=4000), hide_index=True)
        st.download_button("Download sales table (CSV)", wide_sales.to_csv(index=False).encode("utf-8"), "retailer_sales_by_week.csv", "text/csv")

        st.markdown("#### Units by Retailer")
        wide_units = build_wide_totals(df, "Retailer", "Units", use_periods, avg_n)
        if not wide_units.empty:
            wide_units = wide_units.sort_values("Retailer", ascending=True)
            wide_units = add_total_row(wide_units, "Retailer")
        st.dataframe(style_units_wide_table(wide_units, diff_like_cols=['Diff']), use_container_width=True, height=_table_height(wide_units, max_height=4000), hide_index=True)
        st.download_button("Download units table (CSV)", wide_units.to_csv(index=False).encode("utf-8"), "retailer_units_by_week.csv", "text/csv")

        st.markdown("---")
        st.markdown("#### Year-to-date totals (all retailers combined)")
        m = wow_mom_metrics(df)
        c1, c2, c3 = st.columns(3)
        with c1:
            st.metric("YTD Units", f"{int(round(m['ytd_units'])):,}", delta=f"{int(round(m['wow_units'])):,} WoW" if m['wow_units'] is not None else None)
        with c2:
            st.metric("YTD Sales", fmt_currency(m['ytd_sales']), delta=f"{fmt_currency(m['wow_sales'])} WoW" if m['wow_sales'] is not None else None)
        with c3:
            # Requested: MoM sales only (as a standalone number)
            st.metric("MoM Sales Change", fmt_currency(m['mom_sales']) if m['mom_sales'] is not None else "$0.00")


with tab_vendor_totals:
    st.markdown("### Vendor Totals (by week)")

    tf = st.selectbox("Timeframe (weeks)", options=[2,4,8,12], index=2, key="vendor_totals_tf")
    avg_n = st.selectbox("Average over last (weeks)", options=[4,8,12], index=0, key="vendor_totals_avg")

    if df.empty or df["Vendor"].dropna().empty:
        st.info("No data available yet. Upload weekly sheets to populate totals.")
    else:
        periods = _sorted_periods(df)
        use_periods = periods[-min(len(periods), tf):]

        st.markdown("#### Sales ($) by Vendor")
        wide_sales = build_wide_totals(df, "Vendor", "Sales", use_periods, avg_n)
        if not wide_sales.empty:
            wide_sales = wide_sales.sort_values("Vendor", ascending=True)
            wide_sales = add_total_row(wide_sales, "Vendor")
        st.dataframe(style_currency_table(wide_sales, diff_like_cols=['Diff']), use_container_width=True, height=_table_height(wide_sales, max_height=4000), hide_index=True)
        st.download_button("Download sales table (CSV)", wide_sales.to_csv(index=False).encode("utf-8"), "vendor_sales_by_week.csv", "text/csv")

        st.markdown("#### Units by Vendor")
        wide_units = build_wide_totals(df, "Vendor", "Units", use_periods, avg_n)
        if not wide_units.empty:
            wide_units = wide_units.sort_values("Vendor", ascending=True)
            wide_units = add_total_row(wide_units, "Vendor")
        st.dataframe(style_units_wide_table(wide_units, diff_like_cols=['Diff']), use_container_width=True, height=_table_height(wide_units, max_height=4000), hide_index=True)
        st.download_button("Download units table (CSV)", wide_units.to_csv(index=False).encode("utf-8"), "vendor_units_by_week.csv", "text/csv")

with tab_unit_summary:
    st.markdown("### Unit Summary (SKU-level by Retailer)")

    if df.empty:
        st.info("Upload weekly sheets to see SKU-level summaries.")
    else:
        retailers = sorted([r for r in df["Retailer"].dropna().unique().tolist() if str(r).strip() != ""])
        sel_r = st.selectbox("Retailer", options=retailers, index=0 if retailers else None, key="unit_sum_retailer")
        tf = st.selectbox("Timeframe (weeks)", options=[2,4,8,12], index=2, key="unit_sum_tf")
        avg_n = st.selectbox("Average over last (weeks)", options=[4,8,12], index=0, key="unit_sum_avg")

        if not sel_r:
            st.info("Select a retailer.")
        else:
            rdf = df[df["Retailer"] == sel_r].copy()
            periods = _sorted_periods(rdf)
            use_periods = periods[-min(len(periods), tf):]

            st.markdown("#### Units by SKU (weekly)")
            wide_u = build_wide_totals(rdf, "SKU", "Units", use_periods, avg_n)
            wide_u = reorder_skus_for_retailer(wide_u, sel_r, sku_order_map)

            # Hide SKUs that have never sold (Units=0 and Sales=0 across all time for this retailer)
            never_sold = rdf.groupby("SKU", as_index=False).agg(_u=("Units","sum"), _s=("Sales","sum"))
            never_sold = set(never_sold[(never_sold["_u"]<=0) & (never_sold["_s"]<=0)]["SKU"].tolist())
            if never_sold:
                wide_u = wide_u[~wide_u["SKU"].isin(never_sold)].copy()

            wide_u = add_total_row(wide_u, "SKU")
            st.dataframe(style_units_wide_table(wide_u, diff_like_cols=['Diff']), use_container_width=True, height=_table_height(wide_u), hide_index=True)
            st.download_button("Download units (CSV)", wide_u.to_csv(index=False).encode("utf-8"), f"{sel_r}_sku_units.csv", "text/csv")

            st.markdown("#### Sales ($) by SKU (weekly)")
            wide_s = build_wide_totals(rdf, "SKU", "Sales", use_periods, avg_n)
            wide_s = reorder_skus_for_retailer(wide_s, sel_r, sku_order_map)

            # Hide SKUs that have never sold (Units=0 and Sales=0 across all time for this retailer)
            never_sold = rdf.groupby("SKU", as_index=False).agg(_u=("Units","sum"), _s=("Sales","sum"))
            never_sold = set(never_sold[(never_sold["_u"]<=0) & (never_sold["_s"]<=0)]["SKU"].tolist())
            if never_sold:
                wide_s = wide_s[~wide_s["SKU"].isin(never_sold)].copy()

            wide_s = add_total_row(wide_s, "SKU")
            st.dataframe(style_currency_table(wide_s, diff_like_cols=['Diff']), use_container_width=True, height=_table_height(wide_s), hide_index=True)
            st.download_button("Download sales (CSV)", wide_s.to_csv(index=False).encode("utf-8"), f"{sel_r}_sku_sales.csv", "text/csv")


with tab_retail_scorecard:
    st.markdown("### Retailer Scorecard")

    if df.empty:
        st.info("Upload weekly sheets to see scorecards.")
    else:
        retailers = sorted([r for r in df["Retailer"].dropna().unique().tolist() if str(r).strip() != ""])
        sel_retailer = st.selectbox("Retailer", options=retailers, index=0 if retailers else None, key="retail_score_retailer")

        if not sel_retailer:
            st.info("Select a retailer.")
        else:
            rdf = df[df["Retailer"] == sel_retailer].copy()

            m = wow_mom_metrics(rdf)

            k1, k2 = st.columns(2)
            k1.metric("YTD Units", f"{m['ytd_units']:,.0f}")
            k2.metric("YTD Sales", f"${m['ytd_sales']:,.2f}")

            s1, s2, s3, s4 = st.columns(4)
            s1.metric("WoW Units", value="", delta="" if m["wow_units"] is None else f"{m['wow_units']:+,.0f}", delta_color="normal")
            s2.metric("WoW Sales", value="", delta="" if m["wow_sales"] is None else f"${m['wow_sales']:+,.2f}", delta_color="normal")
            s3.metric("MoM Units", value="", delta="" if m["mom_units"] is None else f"{m['mom_units']:+,.0f}", delta_color="normal")
            s4.metric("MoM Sales", value="", delta="" if m["mom_sales"] is None else f"${m['mom_sales']:+,.2f}", delta_color="normal")

            st.markdown("#### Monthly totals")
            months_n = st.selectbox("Months to show", options=[3,6,12], index=1, key="retail_score_months")
            mt = month_table(rdf, "Retailer")
            if mt.empty:
                st.info("No month data yet.")
            else:
                mt_show = mt.tail(min(len(mt), months_n)).copy()
                st.dataframe(style_units_sales_table(mt_show), use_container_width=True, height=_table_height(mt_show), hide_index=True)

            sku_agg = rdf.groupby(["SKU"], as_index=False).agg(Units=("Units","sum"), Sales=("Sales","sum"))
            sku_agg["Sales"] = sku_agg["Sales"].fillna(0.0)

            sku_to_vendor = {}
            if vmap is not None and not vmap.empty:
                vm_sub = vmap[vmap["Retailer"] == sel_retailer].copy()
                for _, rr in vm_sub.iterrows():
                    s = rr.get("SKU")
                    v = rr.get("Vendor")
                    if pd.notna(s) and s not in sku_to_vendor:
                        sku_to_vendor[s] = v

            top_units = sku_agg.sort_values("Units", ascending=False).head(10)[["SKU","Units"]].copy()
            top_sales = sku_agg.sort_values("Sales", ascending=False).head(10)[["SKU","Sales"]].copy()
            bot_units = sku_agg.sort_values("Units", ascending=True).head(15)[["SKU","Units"]].copy()
            bot_sales = sku_agg.sort_values("Sales", ascending=True).head(15)[["SKU","Sales"]].copy()

            for tdf in [top_units, top_sales, bot_units, bot_sales]:
                tdf["Vendor"] = tdf["SKU"].map(lambda s: sku_to_vendor.get(s, ""))

            top_units = top_units[["SKU","Vendor","Units"]]
            bot_units = bot_units[["SKU","Vendor","Units"]]
            top_sales = top_sales[["SKU","Vendor","Sales"]]
            bot_sales = bot_sales[["SKU","Vendor","Sales"]]

            st.markdown("#### Top / Bottom SKUs")
            t1, t2 = st.columns(2)
            with t1:
                st.markdown("**Top 10 by Units**")
                st.dataframe(style_units_only_table(top_units), use_container_width=True, height=340, hide_index=True)
                st.markdown("**Bottom 15 by Units**")
                st.dataframe(style_units_only_table(bot_units), use_container_width=True, height=340, hide_index=True)
            with t2:
                st.markdown("**Top 10 by Sales ($)**")
                st.dataframe(style_currency_table(top_sales), use_container_width=True, height=340, hide_index=True)
                st.markdown("**Bottom 15 by Sales ($)**")
                st.dataframe(style_currency_table(bot_sales), use_container_width=True, height=340, hide_index=True)

with tab_vendor_scorecard:
    st.markdown("### Vendor Scorecard")

    if df.empty:
        st.info("Upload weekly sheets to see scorecards.")
    else:
        vendors = sorted([v for v in df["Vendor"].dropna().unique().tolist() if str(v).strip() != ""])
        sel_vendor = st.selectbox("Vendor", options=vendors, index=0 if vendors else None, key="vendor_score_vendor")

        if not sel_vendor:
            st.info("Select a vendor.")
        else:
            vdf = df[df["Vendor"] == sel_vendor].copy()

            m = wow_mom_metrics(vdf)

            # KPIs: show YTD totals, and show WoW + MoM as their own small numbers underneath
            k1, k2 = st.columns(2)
            k1.metric("YTD Units", f"{m['ytd_units']:,.0f}")
            k2.metric("YTD Sales", f"${m['ytd_sales']:,.2f}")

            s1, s2, s3, s4 = st.columns(4)
            s1.metric("WoW Units", value="", delta="" if m["wow_units"] is None else f"{m['wow_units']:+,.0f}", delta_color="normal")
            s2.metric("WoW Sales", value="", delta="" if m["wow_sales"] is None else f"${m['wow_sales']:+,.2f}", delta_color="normal")
            s3.metric("MoM Units", value="", delta="" if m["mom_units"] is None else f"{m['mom_units']:+,.0f}", delta_color="normal")
            s4.metric("MoM Sales", value="", delta="" if m["mom_sales"] is None else f"${m['mom_sales']:+,.2f}", delta_color="normal")

            st.markdown("#### Monthly totals")
            months_n = st.selectbox("Months to show", options=[3,6,12], index=1, key="vendor_score_months")
            mt = month_table(vdf, "Vendor")
            if mt.empty:
                st.info("No month data yet.")
            else:
                mt_show = mt.tail(min(len(mt), months_n)).copy()
                st.dataframe(style_units_sales_table(mt_show), use_container_width=True, height=_table_height(mt_show), hide_index=True)

            sku_agg = vdf.groupby("SKU", as_index=False).agg(Units=("Units","sum"), Sales=("Sales","sum"))
            sku_agg["Sales"] = sku_agg["Sales"].fillna(0.0)

            # Vendor-map SKU order for this vendor (preserve original map order; de-dupe SKUs)
            vm_skus = vmap[vmap["Vendor"] == sel_vendor]["SKU"].tolist()
            seen = set()
            vm_skus = [s for s in vm_skus if not (s in seen or seen.add(s))]
            vm_rank = {s: i for i, s in enumerate(vm_skus)}

            def _vm_sort(df_in: pd.DataFrame) -> pd.DataFrame:
                if df_in is None or df_in.empty:
                    return df_in
                out = df_in.copy()
                out["_rank"] = out["SKU"].map(lambda s: vm_rank.get(s, 10**9))
                out = out.sort_values(["_rank","SKU"], ascending=[True, True]).drop(columns=["_rank"])
                return out

            # Pick top/bottom sets by metric (sorted)
            # Pick top/bottom sets by metric (sorted)
            sku_to_retailer = {}
            if vmap is not None and not vmap.empty:
                vm_sub = vmap[vmap["Vendor"] == sel_vendor].copy()
                # preserve vendor-map order: first retailer encountered for a SKU
                for _, rr in vm_sub.iterrows():
                    s = rr.get("SKU")
                    r = rr.get("Retailer")
                    if pd.notna(s) and s not in sku_to_retailer:
                        sku_to_retailer[s] = r

            top_units = sku_agg.sort_values("Units", ascending=False).head(10)[["SKU","Units"]].copy()
            top_sales = sku_agg.sort_values("Sales", ascending=False).head(10)[["SKU","Sales"]].copy()
            bot_units = sku_agg.sort_values("Units", ascending=True).head(15)[["SKU","Units"]].copy()
            bot_sales = sku_agg.sort_values("Sales", ascending=True).head(15)[["SKU","Sales"]].copy()

            # Add Retailer column and reorder as requested
            for tdf in [top_units, top_sales, bot_units, bot_sales]:
                tdf["Retailer"] = tdf["SKU"].map(lambda s: sku_to_retailer.get(s, ""))
            top_units = top_units[["SKU","Retailer","Units"]]
            bot_units = bot_units[["SKU","Retailer","Units"]]
            top_sales = top_sales[["SKU","Retailer","Sales"]]
            bot_sales = bot_sales[["SKU","Retailer","Sales"]]


            st.markdown("#### Top / Bottom SKUs")
            t1, t2 = st.columns(2)
            with t1:
                st.markdown("**Top 10 by Units**")
                st.dataframe(style_units_only_table(top_units), use_container_width=True, height=340, hide_index=True)
                st.markdown("**Bottom 15 by Units**")
                st.dataframe(style_units_only_table(bot_units), use_container_width=True, height=340, hide_index=True)
            with t2:
                st.markdown("**Top 10 by Sales ($)**")
                st.dataframe(style_currency_table(top_sales), use_container_width=True, height=340, hide_index=True)
                st.markdown("**Bottom 15 by Sales ($)**")
                st.dataframe(style_currency_table(bot_sales), use_container_width=True, height=340, hide_index=True)

with tab_skus:
    st.markdown("### SKU Table (filtered)")
    sku_tbl = agg_sku(df, sku_order_map=sku_order_map) if not df.empty else pd.DataFrame()
    st.dataframe(sku_tbl, use_container_width=True, height=600, hide_index=True)
    st.download_button("Download CSV", sku_tbl.to_csv(index=False).encode("utf-8"), "sku_table.csv", "text/csv")

with tab_unmapped:
    st.markdown("### Unmapped / Missing Price")
    if enriched.empty:
        st.info("No data yet.")
    else:
        um = enriched[enriched["Vendor"].isna() | enriched["Price"].isna()].copy()
        um["_sku_rank"] = um.apply(lambda r: {s:i for i,s in enumerate(sku_order_map.get(r["Retailer"], []))}.get(r["SKU"], 10**9), axis=1)
        um = um.sort_values(["Retailer","_sku_rank","SKU"]).drop(columns=["_sku_rank"])
        st.dataframe(um, use_container_width=True, height=600, hide_index=True)
        st.download_button("Download CSV", um.to_csv(index=False).encode("utf-8"), "unmapped_rows.csv", "text/csv")


with tab_no_sales:
    st.markdown("### No Sales SKUs")
    if df.empty:
        st.info("Upload weekly sheets to identify SKUs with no sales in a selected timeframe.")
    else:
        lookback = st.selectbox("Show SKUs with no sales for the last...", options=[3,6,8,12], index=1, key="no_sales_lookback")
        periods = _sorted_periods(df)
        if not periods:
            st.info("No dated weekly periods found yet.")
        else:
            use_periods = periods[-min(len(periods), lookback):]
            recent = df[df["StartDate"].dt.date.isin(use_periods)].copy()

            # Start from ALL mapped retailer/vendor/SKU combos in the vendor map
            mapped = vmap[["Retailer","Vendor","SKU"]].drop_duplicates().copy()

            sold_recent = recent.groupby(["Retailer","Vendor","SKU"], as_index=False).agg(
                Units=("Units","sum"),
                Sales=("Sales","sum"),
            )

            merged = mapped.merge(sold_recent, on=["Retailer","Vendor","SKU"], how="left")
            merged["Units"] = merged["Units"].fillna(0)
            merged["Sales"] = merged["Sales"].fillna(0)

            no_sold = merged[(merged["Units"] <= 0) & (merged["Sales"] <= 0)].copy()
            no_sold["Status"] = f"No units or sales in last {lookback} weeks"

            # Sort: Retailer/Vendor alpha, SKUs in vendor-map order per retailer
            def _sku_rank(row):
                r = row["Retailer"]
                sku = row["SKU"]
                order = sku_order_map.get(r, [])
                try:
                    return order.index(sku)
                except ValueError:
                    return 10**9

            if not no_sold.empty:
                no_sold["__sku_rank"] = no_sold.apply(_sku_rank, axis=1)
                no_sold = no_sold.sort_values(["Retailer","Vendor","__sku_rank","SKU"]).drop(columns=["__sku_rank"])

            no_sold = no_sold[["Retailer","Vendor","SKU","Status"]]

            if no_sold.empty:
                st.success(f"All mapped SKUs have sales/units in the last {lookback} weeks.")
            else:
                st.dataframe(no_sold, use_container_width=True, height=_table_height(no_sold, max_height=4000), hide_index=True)
                st.download_button("Download (CSV)", no_sold.to_csv(index=False).encode("utf-8"), "no_sales_skus.csv", "text/csv")

with tab_backup:
    st.markdown("### Backup / Restore")
    st.write("These exports include **all stored rows**, not just filtered rows.")
    st.write(f"Stored rows: **{len(enriched):,}**")

    c1, c2 = st.columns(2)
    with c1:
        st.download_button(
            "Download raw store (CSV)",
            DEFAULT_SALES_STORE.read_bytes() if DEFAULT_SALES_STORE.exists() else b"",
            file_name="sales_store.csv",
            mime="text/csv",
            disabled=not DEFAULT_SALES_STORE.exists()
        )
    with c2:
        st.download_button(
            "Download enriched CSV",
            enriched.to_csv(index=False).encode("utf-8") if not enriched.empty else b"",
            file_name="sales_enriched.csv",
            mime="text/csv",
            disabled=enriched.empty
        )

    st.divider()
    st.subheader("Backup / Restore database")

    st.caption("Backup = your stored sales database (sales_store.csv). You can restore by uploading a prior backup.")

    backup_up = st.file_uploader("Upload backup sales_store.csv", type=["csv"], key="restore_sales_store")
    if backup_up is not None:
        if st.button("Restore uploaded backup (overwrite current database)"):
            try:
                DEFAULT_SALES_STORE.write_bytes(backup_up.getbuffer())
                st.success("Backup restored. Please refresh the page (or rerun) to see the restored data.")
            except Exception as e:
                st.error(f"Restore failed: {e}")
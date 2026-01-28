
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
        df["StartDate"] = start_d
        df["EndDate"] = end_d
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
                    df[c] = pd.to_datetime(df[c], errors="coerce").dt.date
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
            existing[c] = pd.to_datetime(existing[c], errors="coerce").dt.date
        if c in new_rows.columns:
            new_rows[c] = pd.to_datetime(new_rows[c], errors="coerce").dt.date

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

def agg_sku(df: pd.DataFrame) -> pd.DataFrame:
    g = df.groupby(["Retailer","SKU"], dropna=False, as_index=False).agg(
        Units=("Units","sum"),
        Sales=("Sales","sum"),
        Vendor=("Vendor", lambda x: x.dropna().iloc[0] if len(x.dropna()) else np.nan),
        Price=("Price", lambda x: x.dropna().iloc[0] if len(x.dropna()) else np.nan),
    )
    g["Sales"] = g["Sales"].fillna(0.0)
    return g.sort_values("Units", ascending=False)

# -------------------------
# UI
# -------------------------
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

sales_store = load_sales_store()
enriched = enrich_sales(sales_store, vmap)

# Global filters
st.subheader("Filters")
f1, f2, f3, f4 = st.columns([1,1,1,2])
with f1:
    retailer_filter = st.multiselect("Retailer", sorted(enriched["Retailer"].dropna().unique()) if not enriched.empty else [])
with f2:
    vendor_filter = st.multiselect("Vendor", sorted(enriched["Vendor"].dropna().unique()) if not enriched.empty else [])
with f3:
    show_unmapped = st.checkbox("Show only unmapped SKUs (missing Vendor/Price)", value=False)
with f4:
    st.caption("Tip: Name weekly files like 'APP 1-1 thru 1-7.xlsx' so WoW/MoM works automatically.")

df = enriched.copy()
if retailer_filter:
    df = df[df["Retailer"].isin(retailer_filter)]
if vendor_filter:
    df = df[df["Vendor"].isin(vendor_filter)]
if show_unmapped:
    df = df[df["Vendor"].isna() | df["Price"].isna()]

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

tab_summary, tab_vendor_totals, tab_vendor_scorecard, tab_retail_totals, tab_retail_scorecard, tab_skus, tab_unmapped, tab_backup = st.tabs(
    ["Summary", "Vendor Totals", "Vendor Scorecard", "Retailer Totals", "Retailer Scorecard", "SKUs", "Unmapped SKUs", "Backup / Exports"]
)

with tab_summary:
    c1, c2, c3, c4 = st.columns(4)
    total_units = float(df["Units"].sum()) if not df.empty else 0.0
    total_sales = float(df["Sales"].fillna(0).sum()) if not df.empty else 0.0
    c1.metric("Total Units (filtered)", f"{total_units:,.0f}")
    c2.metric("Total Sales (filtered)", f"${total_sales:,.2f}")
    c3.metric("Rows", f"{len(df):,}")
    c4.metric("Distinct SKUs", f"{df['SKU'].nunique() if not df.empty else 0:,}")

    st.markdown("### Latest Period (WoW)")
    if cur_period.empty or not cur_period["EndDate"].notna().any():
        st.info("No dated uploads detected yet.")
    else:
        cur_u = cur_period["Units"].sum()
        cur_s = cur_period["Sales"].fillna(0).sum()
        prev_u = prev_period["Units"].sum() if not prev_period.empty else 0
        prev_s = prev_period["Sales"].fillna(0).sum() if not prev_period.empty else 0
        st.write(f"Period end date: **{cur_period['EndDate'].dropna().max()}** (length: {days} day(s))")
        m1, m2 = st.columns(2)
        m1.metric("Units (latest period)", f"{cur_u:,.0f}", f"{(cur_u-prev_u):,.0f}")
        m2.metric("Sales (latest period)", f"${cur_s:,.2f}", f"${(cur_s-prev_s):,.2f}")

    st.markdown("### Latest Month (MoM)")
    if cur_month_df.empty:
        st.info("No monthly rollups yet (requires EndDate in uploads).")
    else:
        cm_u = cur_month_df["Units"].sum()
        cm_s = cur_month_df["Sales"].fillna(0).sum()
        pm_u = prev_month_df["Units"].sum() if not prev_month_df.empty else 0
        pm_s = prev_month_df["Sales"].fillna(0).sum() if not prev_month_df.empty else 0
        st.write(f"Month: **{sorted(df_m['Month'].dropna().unique())[-1]}**")
        m1, m2 = st.columns(2)
        m1.metric("Units (latest month)", f"{cm_u:,.0f}", f"{(cm_u-pm_u):,.0f}")
        m2.metric("Sales (latest month)", f"${cm_s:,.2f}", f"${(cm_s-pm_s):,.2f}")

with tab_vendor_totals:
    st.markdown("### Vendor Totals (filtered)")
    vtot = agg_vendor(df) if not df.empty else pd.DataFrame(columns=["Vendor","Units","Sales"])
    st.dataframe(vtot, use_container_width=True, height=520)
    st.download_button("Download CSV", vtot.to_csv(index=False).encode("utf-8"), "vendor_totals.csv", "text/csv")

with tab_vendor_scorecard:
    st.markdown("### Vendor Scorecard")
    vendors = sorted(df["Vendor"].dropna().unique()) if not df.empty else []
    sel_vendor = st.selectbox("Select Vendor", options=vendors, index=0 if vendors else None)
    if not sel_vendor:
        st.info("Select a vendor.")
    else:
        vdf = df[df["Vendor"] == sel_vendor].copy()
        st.markdown("#### KPIs")
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("YTD Units", f"{vdf['Units'].sum():,.0f}")
        k2.metric("YTD Sales", f"${vdf['Sales'].fillna(0).sum():,.2f}")

        v_cur = latest_period(vdf)
        v_days = period_len_days(v_cur)
        v_prev = previous_period(vdf, v_days) if v_days else vdf.iloc[0:0].copy()
        if not v_cur.empty and v_cur["EndDate"].notna().any():
            vu = v_cur["Units"].sum()
            vs = v_cur["Sales"].fillna(0).sum()
            pu = v_prev["Units"].sum() if not v_prev.empty else 0
            ps = v_prev["Sales"].fillna(0).sum() if not v_prev.empty else 0
            k3.metric("WoW Units", f"{vu:,.0f}", f"{(vu-pu):,.0f}")
            k4.metric("WoW Sales", f"${vs:,.2f}", f"${(vs-ps):,.2f}")
        else:
            k3.metric("WoW Units", "—")
            k4.metric("WoW Sales", "—")

        st.markdown("#### Monthly (MoM)")
        vdf_m = add_month(vdf)
        if vdf_m.empty or not vdf_m["Month"].notna().any():
            st.info("No monthly rollups yet (requires EndDate).")
        else:
            months = sorted(vdf_m["Month"].dropna().unique())
            cur_m = months[-1]
            prev_m = (pd.Period(cur_m) - 1).strftime("%Y-%m")
            curm = vdf_m[vdf_m["Month"] == cur_m]
            prevm = vdf_m[vdf_m["Month"] == prev_m]
            mu = curm["Units"].sum()
            ms = curm["Sales"].fillna(0).sum()
            pu = prevm["Units"].sum() if not prevm.empty else 0
            ps = prevm["Sales"].fillna(0).sum() if not prevm.empty else 0
            mm1, mm2 = st.columns(2)
            mm1.metric(f"Units ({cur_m})", f"{mu:,.0f}", f"{(mu-pu):,.0f}")
            mm2.metric(f"Sales ({cur_m})", f"${ms:,.2f}", f"${(ms-ps):,.2f}")

        st.markdown("#### Top SKUs (by Units)")
        sku = agg_sku(vdf)
        st.dataframe(sku.head(50), use_container_width=True, height=520)

with tab_retail_totals:
    st.markdown("### Retailer Totals (filtered)")
    rtot = agg_retailer(df) if not df.empty else pd.DataFrame(columns=["Retailer","Units","Sales"])
    st.dataframe(rtot, use_container_width=True, height=520)
    st.download_button("Download CSV", rtot.to_csv(index=False).encode("utf-8"), "retailer_totals.csv", "text/csv")

with tab_retail_scorecard:
    st.markdown("### Retailer Scorecard")
    retailers = sorted(df["Retailer"].dropna().unique()) if not df.empty else []
    sel_r = st.selectbox("Select Retailer", options=retailers, index=0 if retailers else None, key="sel_retail")
    if not sel_r:
        st.info("Select a retailer.")
    else:
        rdf = df[df["Retailer"] == sel_r].copy()
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("YTD Units", f"{rdf['Units'].sum():,.0f}")
        k2.metric("YTD Sales", f"${rdf['Sales'].fillna(0).sum():,.2f}")

        r_cur = latest_period(rdf)
        r_days = period_len_days(r_cur)
        r_prev = previous_period(rdf, r_days) if r_days else rdf.iloc[0:0].copy()
        if not r_cur.empty and r_cur["EndDate"].notna().any():
            ru = r_cur["Units"].sum()
            rs = r_cur["Sales"].fillna(0).sum()
            pu = r_prev["Units"].sum() if not r_prev.empty else 0
            ps = r_prev["Sales"].fillna(0).sum() if not r_prev.empty else 0
            k3.metric("WoW Units", f"{ru:,.0f}", f"{(ru-pu):,.0f}")
            k4.metric("WoW Sales", f"${rs:,.2f}", f"${(rs-ps):,.2f}")
        else:
            k3.metric("WoW Units", "—")
            k4.metric("WoW Sales", "—")

        st.markdown("#### Top Vendors (by Sales)")
        topv = rdf.groupby("Vendor", as_index=False).agg(
            Units=("Units","sum"),
            Sales=("Sales", lambda x: x.fillna(0).sum())
        ).sort_values("Sales", ascending=False)
        st.dataframe(topv.head(50), use_container_width=True, height=520)

with tab_skus:
    st.markdown("### SKU Table (filtered)")
    sku_tbl = agg_sku(df) if not df.empty else pd.DataFrame()
    st.dataframe(sku_tbl, use_container_width=True, height=600)
    st.download_button("Download CSV", sku_tbl.to_csv(index=False).encode("utf-8"), "sku_table.csv", "text/csv")

with tab_unmapped:
    st.markdown("### Unmapped / Missing Price")
    if enriched.empty:
        st.info("No data yet.")
    else:
        um = enriched[enriched["Vendor"].isna() | enriched["Price"].isna()].copy()
        um = um.sort_values(["Retailer","SKU"])
        st.dataframe(um, use_container_width=True, height=600)
        st.download_button("Download CSV", um.to_csv(index=False).encode("utf-8"), "unmapped_rows.csv", "text/csv")

with tab_backup:
    st.markdown("### Backup / Exports")
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
    st.caption("Restore by placing a backed up sales_store.csv into ./data/ and reboot the app.")

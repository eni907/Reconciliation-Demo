# apppublic.py — Public Demo

import io, traceback, datetime
import streamlit as st
import pandas as pd
from Reconciliationpublic_20250914 import run_reconciliation

st.set_page_config(page_title="Automated Reconciliation Tool — Demo",
                   page_icon="🧮", layout="wide")
st.title("Automated Reconciliation Tool — Demo")
st.caption("Seamless. Accurate. Effortless.")

# ---------- Inputs ----------
src = st.file_uploader("Upload Source File", type=["xlsx", "csv"], key="src")
tgt = st.file_uploader("Upload Target File", type=["xlsx", "csv"], key="tgt")
tol = st.number_input("Tolerance", value=0.0, step=0.01,
                      help="Allowed absolute difference for amounts.")

def _safe_read(uploaded) -> pd.DataFrame:
    name = uploaded.name.lower()
    if name.endswith(".csv"):
        return pd.read_csv(uploaded)
    return pd.read_excel(uploaded)

def _preview(uploaded, label):
    try:
        df = _safe_read(uploaded)
        st.success(f"{label} loaded: {df.shape[0]:,} rows")
        st.dataframe(df.head(5), use_container_width=True, height=180)
    except Exception:
        st.error(f"{label} file cannot be read. Please check format and try again.")

if src: _preview(src, "Source")
if tgt: _preview(tgt, "Target")

def _log_error(e: Exception):
    with open("app_errors.log", "a", encoding="utf-8") as f:
        f.write("\n" + "="*80 + "\n")
        f.write(f"[{datetime.datetime.now().isoformat()}] ERROR\n")
        f.write("".join(traceback.format_exc()))

# ---------- Run ----------
if src and tgt:
    try:
        with st.spinner("Running reconciliation…"):
            # 不依赖磁盘文件，output_path 可不传
            result = run_reconciliation(src, tgt, tolerance=tol)

        # 兼容 6 元组（忽略多余项）或 5 元组
        if isinstance(result, tuple) and len(result) >= 5:
            summary, df_match, df_mis, df_missing, df_extra = result[:5]
        else:
            st.error("Unexpected engine result."); st.stop()

        st.subheader("Summary")
        st.json(summary)

        st.subheader("Matches")
        st.dataframe(df_match, use_container_width=True)

        st.subheader("Mismatches")
        st.dataframe(df_mis, use_container_width=True)

        st.subheader("Missing in Target")
        st.dataframe(df_missing, use_container_width=True)

        st.subheader("Extra in Target")
        st.dataframe(df_extra, use_container_width=True)

        # ---------- In-memory Excel export ----------
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            df_match.to_excel(writer, sheet_name="Matches", index=False)
            df_mis.to_excel(writer, sheet_name="Mismatches", index=False)
            df_missing.to_excel(writer, sheet_name="Missing_in_Target", index=False)
            df_extra.to_excel(writer, sheet_name="Extra_in_Target", index=False)
        output.seek(0)

        st.download_button(
            label="Download Report (Excel)",
            data=output,  # 亦可用 output.getvalue()
            file_name="reconciliation_report.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    except Exception as e:
        _log_error(e)
        st.error("Something went wrong. Please verify the files and try again.")

# Reconciliation_20250914.py
import io
from pathlib import Path
from typing import Tuple, List, Any

import pandas as pd

# ---- Required column names (please keep your file headers consistent) ----
KEY_COL = "invoice_no"
AMOUNT_COL = "amount"


# ------------------------------------------------------------------------- #
# Internal: robust loader that accepts path string / Path / Streamlit UploadedFile
# - Handles CSV & Excel (.xlsx with openpyxl)
# - Safely resets file pointer if it's a file-like object (e.g., Streamlit upload)
# - Normalizes/validates required columns
# - Ensures invoice_no is str and amount is numeric
# ------------------------------------------------------------------------- #
def _load_df(file_like: Any, label: str, debug_log: List[str] = None) -> pd.DataFrame:
    if debug_log is None:
        debug_log = []

    def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
        # 允许列名大小写差异/首尾空格，统一成小写
        df = df.copy()
        df.columns = [str(c).strip() for c in df.columns]
        return df

    # A) Path-like (str/Path) -> read directly from disk
    if isinstance(file_like, (str, Path)):
        path = str(file_like)
        name_lower = path.lower()
        if debug_log is not None:
            debug_log.append(f"[{label}] reading from path: {path}")

        if name_lower.endswith(".csv"):
            df = pd.read_csv(path, dtype={KEY_COL: "string"})
        else:
            # openpyxl needed for .xlsx
            df = pd.read_excel(path, engine="openpyxl")

    # B) File-like (e.g., Streamlit UploadedFile)
    else:
        # 重要：预防之前读过，先把“指针”归零
        try:
            file_like.seek(0)
        except Exception:
            pass

        # 取原始二进制，再用 BytesIO 给 pandas 读
        try:
            raw = file_like.getvalue()  # Streamlit UploadedFile
        except AttributeError:
            raw = file_like.read()      # 其它类文件对象

        name_lower = getattr(file_like, "name", "").lower()
        if debug_log is not None:
            debug_log.append(f"[{label}] reading from file-like: name={name_lower or '(unknown)'} size={len(raw)}")

        bio = io.BytesIO(raw)
        if name_lower.endswith(".csv"):
            df = pd.read_csv(bio, dtype={KEY_COL: "string"})
        else:
            df = pd.read_excel(bio, engine="openpyxl")

    # ---- normalize / validate ----
    df = _normalize_columns(df)

    need = {KEY_COL, AMOUNT_COL}
    missing = need - set(map(str.lower, df.columns))
    # 为了容错大小写，我们在这里按小写匹配，再重命名回标准名
    col_map_lower = {c.lower(): c for c in df.columns}
    if missing:
        # 再检查是否只是大小写不同，如果是，则重命名回标准名
        fixed = {}
        for want in need:
            if want in col_map_lower:  # 说明只是大小写不同
                fixed[col_map_lower[want]] = want
        if fixed:
            df = df.rename(columns=fixed)
            missing = need - set(df.columns)

    if missing:
        raise ValueError(
            f"{label} is missing required columns: {list(missing)}. "
            f"Actual columns: {list(df.columns)}. "
            f"Expected at least: {KEY_COL}, {AMOUNT_COL}"
        )

    # 主键保留为字符串（避免前导零丢失）
    df[KEY_COL] = df[KEY_COL].astype("string").str.strip()

    # 金额转数值（无效值转 NaN）
    df[AMOUNT_COL] = pd.to_numeric(df[AMOUNT_COL], errors="coerce")

    # 去掉 key 为空的行
    df = df.dropna(subset=[KEY_COL])

    return df


# ------------------------------------------------------------------------- #
# Public API
# ------------------------------------------------------------------------- #
def run_reconciliation(
    file1: Any,
    file2: Any,
    tolerance: float = 0.0,
    output_path: str = "reconciliation_report.xlsx",
    debug: bool = False,
) -> Tuple[dict, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame] | Tuple[
    dict, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, List[str]
]:
    """
    Core reconciliation routine.
    Parameters
    ----------
    file1, file2 : path-like or file-like (e.g., Streamlit UploadedFile)
    tolerance : float
        Allowed absolute difference between amounts to be considered a match.
    output_path : str
        Excel export path; failures will be swallowed (won't block UI).
    debug : bool
        If True, returns a 6th element: debug_log (list of strings).

    Returns
    -------
    summary : dict
    matches : DataFrame
    mismatches : DataFrame
    missing_in_target : DataFrame
    extra_in_target : DataFrame
    [debug_log : list[str]]  # only when debug=True
    """
    debug_log: List[str] = []
    if debug:
        debug_log.append(f"[engine] start with tolerance={tolerance}")

    # 读取 & 预处理
    df1 = _load_df(file1, "Source", debug_log)
    df2 = _load_df(file2, "Target", debug_log)

    if debug:
        debug_log.append(f"[Source] shape={df1.shape} keys={df1[KEY_COL].nunique()}")
        debug_log.append(f"[Target] shape={df2.shape} keys={df2[KEY_COL].nunique()}")

    # 只保留所需列
    s1 = df1[[KEY_COL, AMOUNT_COL]].copy()
    s2 = df2[[KEY_COL, AMOUNT_COL]].copy()

    # 合并
    merged = pd.merge(
        s1, s2,
        on=KEY_COL,
        how="outer",
        suffixes=("_src", "_tgt"),
        indicator=True,
    )

    both = merged["_merge"] == "both"
    left_only = merged["_merge"] == "left_only"
    right_only = merged["_merge"] == "right_only"

    # 计算绝对差（对任一端为空保留 NaN）
    diff = (merged[f"{AMOUNT_COL}_src"] - merged[f"{AMOUNT_COL}_tgt"]).abs()

    matches = merged[both & (diff <= tolerance)]
    mismatches = merged[both & (diff > tolerance)]

    missing = merged[left_only][[KEY_COL, f"{AMOUNT_COL}_src"]].rename(
        columns={f"{AMOUNT_COL}_src": AMOUNT_COL}
    )
    extra = merged[right_only][[KEY_COL, f"{AMOUNT_COL}_tgt"]].rename(
        columns={f"{AMOUNT_COL}_tgt": AMOUNT_COL}
    )

    summary = {
        "key_column": KEY_COL,
        "amount_column": AMOUNT_COL,
        "tolerance": float(tolerance),
        "matches": int(len(matches)),
        "mismatches": int(len(mismatches)),
        "missing_in_target": int(len(missing)),
        "extra_in_target": int(len(extra)),
        "total_keys_source": int(df1[KEY_COL].nunique()),
        "total_keys_target": int(df2[KEY_COL].nunique()),
    }

    if debug:
        debug_log.append(f"[engine] matches={summary['matches']} mismatches={summary['mismatches']}")
        debug_log.append(f"[engine] missing_in_target={summary['missing_in_target']} extra_in_target={summary['extra_in_target']}")

    # 可选：导出 Excel（失败不影响主流程）
    try:
        with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
            pd.DataFrame([summary]).to_excel(writer, sheet_name="Summary", index=False)
            matches.to_excel(writer, sheet_name="Matches", index=False)
            mismatches.to_excel(writer, sheet_name="Mismatches", index=False)
            missing.to_excel(writer, sheet_name="MissingInTarget", index=False)
            extra.to_excel(writer, sheet_name="ExtraInTarget", index=False)
    except Exception as e:
        if debug:
            debug_log.append(f"[excel] export skipped: {e!r}")

    if debug:
        return summary, matches, mismatches, missing, extra, debug_log
    return summary, matches, mismatches, missing, extra

from __future__ import annotations

from pathlib import Path
import re
import pandas as pd

# ---- Export configuration ----------------------------------------------------

# This is the *desired* order (as shown in your UI).
# We'll map these to actual df.columns using normalization so "System Qty" matches "SystemQty", etc.
EXPORT_COLUMN_ORDER = [
    "Tag",
    "Item",
    "Description",
    "Default Location",
    "Whs",
    "Location",
    "System Qty",
    "Count Qty",
    "Set Location Qty To",
    "Total $ Variance",
    "Recommendation Type",
    "Group Headline",
    "Reason",
    "Assigned To",
    "Site",
    "Stock Area",
    "Count 1 Entry On-hand Qty",
    "Missing Location Master",
]

# Columns that should NEVER be exported
EXPORT_HIDE_COLUMNS = {
    "InternalID",
    "RowHash",
    "DebugNotes",
    "IsSecuredLocation",
    "TempCalculation",
}


def _norm(s: str) -> str:
    """Normalize column names to compare them reliably."""
    return re.sub(r"[^a-z0-9]+", "", str(s).strip().lower())


def _resolve_order(desired_order: list[str], actual_columns: list[str]) -> list[str]:
    """
    Convert desired column labels into the actual df column names using normalized matching.
    Any desired columns not found are ignored (no crash).
    """
    actual_by_norm = {_norm(c): c for c in actual_columns}

    resolved = []
    for wanted in desired_order:
        key = _norm(wanted)
        if key in actual_by_norm:
            resolved.append(actual_by_norm[key])

    return resolved


def prepare_export_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply:
      1) hide list
      2) desired order (normalized match)
      3) append remaining cols not in desired order
    """
    if df is None or df.empty:
        return df

    # 1) drop hidden cols
    visible_cols = [c for c in df.columns if c not in EXPORT_HIDE_COLUMNS]
    out = df.loc[:, visible_cols].copy()

    # 2) enforce desired order (robust to spaces/case/$)
    ordered_cols = _resolve_order(EXPORT_COLUMN_ORDER, list(out.columns))

    # 3) append anything not explicitly ordered
    remaining_cols = [c for c in out.columns if c not in ordered_cols]
    final_cols = ordered_cols + remaining_cols

    return out.loc[:, final_cols]


def export_workbook(
    out_path: Path,
    review_df: pd.DataFrame,
    group_df: pd.DataFrame,
    transfers_df: pd.DataFrame,
) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Adjustment suggestions are just groups with RemainingAdjustmentQty != 0
    adj_df = group_df.copy()
    if "RemainingAdjustmentQty" in adj_df.columns:
        adj_df = adj_df[adj_df["RemainingAdjustmentQty"] != 0].copy()
    else:
        adj_df = adj_df.iloc[0:0].copy()

    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        # Review sheet: apply column order + hide list
        prepare_export_df(review_df).to_excel(writer, sheet_name="Review_Lines", index=False)

        # Leave these as-is (or we can add ordering later if you want)
        group_df.to_excel(writer, sheet_name="Group_Summary", index=False)

        if transfers_df is None or transfers_df.empty:
            pd.DataFrame(
                columns=["Whs", "Item", "Batch/lot", "Qty", "FromLocation", "ToLocation", "Reason", "Confidence", "Severity"]
            ).to_excel(writer, sheet_name="Transfer_Suggestions", index=False)
        else:
            transfers_df.to_excel(writer, sheet_name="Transfer_Suggestions", index=False)

        if adj_df is None or adj_df.empty:
            pd.DataFrame(
                columns=["Whs", "Item", "Batch/lot", "RemainingAdjustmentQty", "RecommendationHeadline", "Flags", "Confidence", "Severity"]
            ).to_excel(writer, sheet_name="Adjustment_Suggestions", index=False)
        else:
            keep = [c for c in ["Whs","Item","Batch/lot","RemainingAdjustmentQty","RecommendationHeadline","Flags","Confidence","Severity"] if c in adj_df.columns]
            adj_df[keep].to_excel(writer, sheet_name="Adjustment_Suggestions", index=False)

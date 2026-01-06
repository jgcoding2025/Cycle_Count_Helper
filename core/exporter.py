from __future__ import annotations

from pathlib import Path
import pandas as pd


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
        review_df.to_excel(writer, sheet_name="Review_Lines", index=False)
        group_df.to_excel(writer, sheet_name="Group_Summary", index=False)

        if transfers_df is None or transfers_df.empty:
            pd.DataFrame(columns=["Whs","Item","Batch/lot","Qty","FromLocation","ToLocation","Reason","Confidence","Severity"]).to_excel(
                writer, sheet_name="Transfer_Suggestions", index=False
            )
        else:
            transfers_df.to_excel(writer, sheet_name="Transfer_Suggestions", index=False)

        if adj_df is None or adj_df.empty:
            pd.DataFrame(columns=["Whs","Item","Batch/lot","RemainingAdjustmentQty","RecommendationHeadline","Flags","Confidence","Severity"]).to_excel(
                writer, sheet_name="Adjustment_Suggestions", index=False
            )
        else:
            # Keep only the most useful columns
            keep = [c for c in ["Whs","Item","Batch/lot","RemainingAdjustmentQty","RecommendationHeadline","Flags","Confidence","Severity"] if c in adj_df.columns]
            adj_df[keep].to_excel(writer, sheet_name="Adjustment_Suggestions", index=False)

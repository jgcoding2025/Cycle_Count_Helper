from __future__ import annotations

import pandas as pd


def build_review_lines(
    session_id: str,
    recount_df: pd.DataFrame,
    locations_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Enrich recount lines with location metadata (Location Type, Allocation Category, etc.)
    using (Whs, Location) left join.

    Returns a dataframe ready to show in the UI grid.
    """
    merged = recount_df.merge(
        locations_df,
        on=["Whs", "Location"],
        how="left",
        suffixes=("", "_loc"),
        indicator=True,
    )

    merged["Missing Location Master"] = merged["_merge"].map(
        {"both": "N", "left_only": "Y", "right_only": "?"}
    )
    merged = merged.drop(columns=["_merge"])

    merged.insert(0, "SessionId", session_id)

    merged["SystemQty"] = merged["Count 1 cutoff on-hand qty"]
    merged["CountQty"] = merged["Count 1 qty"]
    merged["VarianceQty"] = merged["Count 1 variance qty"]
    merged["DefaultLocation"] = merged["Item Rev Default Location"]

    preferred = [
        "SessionId",
        "Whs",
        "Item",
        "Batch/lot",
        "DefaultLocation",
        "Location",
        "SystemQty",
        "CountQty",
        "VarianceQty",
        "Location Type",
        "Allocation Category",
        "Missing Location Master",
    ]

    final_cols = preferred + [c for c in merged.columns if c not in preferred]
    out = merged[final_cols].copy()

    out = out.sort_values(
        ["Whs", "Item", "Batch/lot", "Location"],
        kind="stable",
    ).reset_index(drop=True)

    return out

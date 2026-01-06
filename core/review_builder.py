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

    merged = merged.rename(
        columns={
            "Count 1 cutoff on-hand qty": "SystemQty",
            "Count 1 qty": "CountQty",
            "Count 1 variance qty": "VarianceQty",
            "Item Rev Default Location": "DefaultLocation",
        }
    )

    merged.insert(0, "SessionId", session_id)

    cols = [
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

    for c in ["Tag", "Assigned to", "Description", "Allocated", "Cur cost", "Count 1 entry on-hand qty", "Count Status", "Notes"]:
        if c in merged.columns and c not in cols:
            cols.append(c)


    for c in ["Allocation Priority", "Stock Area", "Supervisor"]:
        if c in merged.columns and c not in cols:
            cols.append(c)

    cols = [c for c in cols if c in merged.columns]
    out = merged[cols].copy()

    out = out.sort_values(
        ["Whs", "Item", "Batch/lot", "Location"],
        kind="stable",
    ).reset_index(drop=True)

    return out

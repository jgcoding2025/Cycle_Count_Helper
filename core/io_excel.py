from __future__ import annotations

from pathlib import Path
import pandas as pd


def _norm_col(c: str) -> str:
    return str(c).strip()


def _to_str_upper(s: pd.Series) -> pd.Series:
    # keep NaN as NaN, otherwise strip and upper
    return s.astype("string").str.strip().str.upper()


def load_warehouse_locations(path: Path) -> pd.DataFrame:
    """
    Loads Warehouse Locations.xlsx -> sheet 'All Locations'
    Expected columns (at minimum): Whs, Location, Location Type, Allocation Category
    """
    df = pd.read_excel(path, sheet_name="All Locations", dtype="object")
    df.columns = [_norm_col(c) for c in df.columns]

    required = ["Whs", "Location", "Location Type", "Allocation Category"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(
            f"Warehouse Locations is missing required columns: {missing}. "
            f"Found: {list(df.columns)}"
        )

    # Normalize join keys
    df["Whs"] = df["Whs"].astype("string").str.strip()
    df["Location"] = _to_str_upper(df["Location"])

    # Keep only columns we care about for now (we can add more later)
    keep = [
        "Whs",
        "Location",
        "Location Type",
        "Allocation Category",
    ]
    # Include optional columns if present (nice-to-have)
    optional = ["Allocation Priority", "Stock Area", "Supervisor"]
    for c in optional:
        if c in df.columns:
            keep.append(c)

    return df[keep].copy()


def load_recount_workbook(path: Path) -> pd.DataFrame:
    """
    Loads recount workbook.
    MVP: Prefer sheet 'Sheet2' if present; otherwise use first sheet.
    Expected columns (at minimum):
      Whs, Item, Location, Batch/lot, Item Rev Default Location,
      Count 1 cutoff on-hand qty, Count 1 qty, Count 1 variance qty
    """
    xls = pd.ExcelFile(path)
    sheet = "Sheet2" if "Sheet2" in xls.sheet_names else xls.sheet_names[0]

    df = pd.read_excel(xls, sheet_name=sheet, dtype="object")
    df.columns = [_norm_col(c) for c in df.columns]

    required = [
        "Whs",
        "Item",
        "Location",
        "Batch/lot",
        "Item Rev Default Location",
        "Count 1 cutoff on-hand qty",
        "Count 1 qty",
        "Count 1 variance qty",
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(
            f"Recount sheet '{sheet}' is missing required columns: {missing}. "
            f"Found: {list(df.columns)}"
        )

    # Normalize join keys
    df["Whs"] = df["Whs"].astype("string").str.strip()
    df["Location"] = _to_str_upper(df["Location"])

    # Normalize some important text columns
    df["Item"] = df["Item"].astype("string").str.strip()
    df["Batch/lot"] = df["Batch/lot"].astype("string").fillna("").str.strip()
    df["Item Rev Default Location"] = _to_str_upper(df["Item Rev Default Location"].astype("string"))

    # Coerce numeric columns safely
    for c in ["Count 1 cutoff on-hand qty", "Count 1 qty", "Count 1 variance qty"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    # Keep extras if present (useful for later)
    optional = [
    "Tag",
    "Assigned to",
    "Description",
    "Allocated",
    "Cur cost",
    "Count 1 entry on-hand qty",
    "Count Status",
    "Notes",
    ]

    keep = required + [c for c in optional if c in df.columns]

    return df[keep].copy()

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import pandas as pd


@dataclass(frozen=True)
class TransferSuggestion:
    whs: str
    item: str
    batch_lot: str
    qty: float
    from_location: str
    to_location: str
    reason: str
    confidence: str
    severity: int


def _is_default_eligible(loc_type: str | None, alloc_cat: str | None) -> bool:
    if loc_type is None or alloc_cat is None:
        return False
    return str(loc_type).strip().lower() == "unsecured" and str(alloc_cat).strip().lower() == "available"


def _confidence_cap(conf: str, cap: str) -> str:
    order = {"Low": 0, "Med": 1, "High": 2}
    inv = {v: k for k, v in order.items()}
    return inv[min(order.get(conf, 1), order.get(cap, 1))]


def _group_headline(flags: dict, remaining_adj: float, has_transfers: bool) -> str:
    if flags.get("missing_master"):
        return "Investigate: location not in master"
    if flags.get("move_recount"):
        return "Move + recount required (include default)"
    if flags.get("secured_variance"):
        return "Investigate: secured location variance"
    if flags.get("default_empty"):
        return "Action needed: default counted zero (verify on-hand)"

    if remaining_adj != 0:
        direction = "up" if remaining_adj > 0 else "down"
        qty = abs(remaining_adj)

        if has_transfers:
            return f"Adjust {direction} {qty:g} after transfers"
        else:
            return f"Adjust {direction} {qty:g}"

    if has_transfers:
        return "Transfers only (resolve via default)"

    return "No variance"

def apply_recommendations(
    review_lines: pd.DataFrame,
    transfer_mode: str = "ADJUST",
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Input: Review_Lines dataframe from Step 2 (already enriched with Location Type / Allocation Category)
    Output:
      - updated review_lines with recommendation columns added
      - transfer_suggestions dataframe
      - group_summary dataframe
    """

    # Ensure expected columns exist
    required = [
        "Whs", "Item", "Batch/lot", "DefaultLocation", "Location",
        "SystemQty", "CountQty", "VarianceQty",
        "Location Type", "Allocation Category", "Missing Location Master",
    ]
    missing = [c for c in required if c not in review_lines.columns]
    if missing:
        raise ValueError(f"Review_Lines missing required columns for Step 3: {missing}")

    df = review_lines.copy()

    # Normalize key fields for grouping
    df["Whs"] = df["Whs"].astype("string").str.strip()
    df["Item"] = df["Item"].astype("string").str.strip()
    df["Batch/lot"] = df["Batch/lot"].astype("string").fillna("").str.strip()
    df["Location"] = df["Location"].astype("string").str.strip().str.upper()
    df["DefaultLocation"] = df["DefaultLocation"].astype("string").fillna("").str.strip().str.upper()

    # Add placeholders for output columns
    df["IsDefault"] = "N"
    df["IsSecondary"] = "N"
    df["RecommendationType"] = ""
    df["RecommendedQty"] = 0.0
    df["FromLocation"] = ""
    df["ToLocation"] = ""
    df["RemainingAdjustmentQty"] = 0.0
    df["SetLocationQtyTo"] = pd.NA
    df["Reason"] = ""
    df["Confidence"] = "Med"
    df["Severity"] = 0
    df["GroupHeadline"] = ""

    transfer_mode = str(transfer_mode).strip().upper() or "TRANSFER"
    if transfer_mode not in {"TRANSFER", "ADJUST"}:
        raise ValueError(f"Unsupported transfer_mode '{transfer_mode}'. Use TRANSFER or ADJUST.")
    use_transfers = transfer_mode == "TRANSFER"

    transfers: list[TransferSuggestion] = []
    group_rows = []

    # Group by Whs/Item/Lot
    gcols = ["Whs", "Item", "Batch/lot"]
    for (whs, item, lot), g in df.groupby(gcols, sort=False):
        g = g.copy()

        # ---- Warehouse guardrail ----
        if str(whs).strip() != "50":
            idx = g.index
            df.loc[idx, "RecommendationType"] = "NO_ACTION"
            df.loc[idx, "Reason"] = "Warehouse is not 50; this tool does not auto-recommend transfers/default updates outside WHS 50."
            df.loc[idx, "Confidence"] = "High"
            df.loc[idx, "Severity"] = 0
            df.loc[idx, "GroupHeadline"] = "No action (non-WHS 50)"

            group_rows.append({
                "Whs": whs,
                "Item": item,
                "Batch/lot": lot,
                "DefaultLocation": str(g["DefaultLocation"].dropna().astype(str).head(1).values[0]) if (g["DefaultLocation"] != "").any() else "",
                "SystemTotal": float(g["SystemQty"].sum()),
                "CountTotal": float(g["CountQty"].sum()),
                "NetVariance": float(g["VarianceQty"].sum()),
                "SysST01": float(g.loc[g["Location"] == "ST01", "SystemQty"].sum()),
                "DefaultSystemAfter": None,
                "DefaultCount": None,
                "Flags": "NonWHS50",
                "RecommendationHeadline": "No action (non-WHS 50)",
                "RemainingAdjustmentQty": 0.0,
                "Confidence": "High",
                "Severity": 0,
            })
            continue

        # Flags
        flags = {
            "missing_master": (g["Missing Location Master"] == "Y").any(),
            "secured_variance": False,
            "default_empty": False,
        }

        # Determine default location (authoritative)
        default_loc = ""
        nonblank_defaults = g["DefaultLocation"][g["DefaultLocation"] != ""].unique()
        if len(nonblank_defaults) >= 1:
            # Should be consistent; take first
            default_loc = str(nonblank_defaults[0])
        else:
            # No default location provided => investigate
            # Mark all rows in group as investigate
            idx = g.index
            df.loc[idx, "RecommendationType"] = "INVESTIGATE"
            df.loc[idx, "Reason"] = "DefaultLocation is blank; cannot reconcile group automatically."
            df.loc[idx, "Confidence"] = "Low"
            df.loc[idx, "Severity"] = 100 if flags["missing_master"] else 85
            headline = "Investigate: default location missing"
            df.loc[idx, "GroupHeadline"] = headline

            group_rows.append({
                "Whs": whs, "Item": item, "Batch/lot": lot,
                "DefaultLocation": "",
                "SystemTotal": float(g["SystemQty"].sum()),
                "CountTotal": float(g["CountQty"].sum()),
                "NetVariance": float(g["VarianceQty"].sum()),
                "SysST01": float(g.loc[g["Location"] == "ST01", "SystemQty"].sum()),
                "DefaultSystemAfter": None,
                "DefaultCount": None,
                "Flags": "DefaultMissing",
                "RecommendationHeadline": headline,
                "RemainingAdjustmentQty": None,
                "Confidence": "Low",
                "Severity": int(df.loc[idx, "Severity"].max()),
            })
            continue

        # Mark default vs secondary
        is_default_mask = g["Location"] == default_loc
        df.loc[g.index[is_default_mask], "IsDefault"] = "Y"
        df.loc[g.index[~is_default_mask], "IsSecondary"] = "Y"

        # Secured variance flag
        secured_var = ((g["Location Type"].astype("string").str.strip().str.lower() == "secured") &
                       (g["VarianceQty"].astype(float) != 0)).any()
        flags["secured_variance"] = bool(secured_var)

        # Compute ST01 system qty
        sys_st01 = float(g.loc[g["Location"] == "ST01", "SystemQty"].sum())

        # Identify default row
        default_rows = g[g["Location"] == default_loc]
        if default_rows.empty:
            # Default row missing from recount lines
            idx = g.index
            df.loc[idx, "RecommendationType"] = "INVESTIGATE"
            df.loc[idx, "Reason"] = f"DefaultLocation '{default_loc}' not present in recount lines."
            df.loc[idx, "Confidence"] = "Low"
            df.loc[idx, "Severity"] = 85
            headline = "Investigate: default row missing"
            df.loc[idx, "GroupHeadline"] = headline

            group_rows.append({
                "Whs": whs, "Item": item, "Batch/lot": lot,
                "DefaultLocation": default_loc,
                "SystemTotal": float(g["SystemQty"].sum()),
                "CountTotal": float(g["CountQty"].sum()),
                "NetVariance": float(g["VarianceQty"].sum()),
                "SysST01": sys_st01,
                "DefaultSystemAfter": None,
                "DefaultCount": None,
                "Flags": "DefaultRowMissing",
                "RecommendationHeadline": headline,
                "RemainingAdjustmentQty": None,
                "Confidence": "Low",
                "Severity": 85,
            })
            continue

        default_system = float(default_rows["SystemQty"].sum())
        default_count = float(default_rows["CountQty"].sum())

        # Default eligibility (from master metadata on that row)
        default_loc_type = default_rows["Location Type"].iloc[0] if "Location Type" in default_rows.columns else None
        default_alloc_cat = default_rows["Allocation Category"].iloc[0] if "Allocation Category" in default_rows.columns else None
        default_eligible = _is_default_eligible(default_loc_type, default_alloc_cat)

        # Build transfer plan: secondary strict reconciliation
        transfers_in_default = 0.0
        transfers_out_default = 0.0

        secondary = g[(g["Location"] != default_loc) & (g["Location"] != "ST01")].copy()

        for ridx, row in secondary.iterrows():
            loc = str(row["Location"])
            system_qty = float(row["SystemQty"])
            count_qty = float(row["CountQty"])
            delta = count_qty - system_qty

            if delta > 0:
                # Secondary has more physical than system (overage at secondary).
                # Rule:
                # - If Allocation Category is AVAILABLE or AVAILABLE UPON REQUEST:
                #     Physically move excess to default, then RECOUNT including default.
                # - Otherwise:
                #     RECOUNT including default (cannot move from this location type/category).

                qty = float(delta)

                sec_alloc_cat = row["Allocation Category"] if "Allocation Category" in row else None
                sec_alloc_norm = str(sec_alloc_cat).strip().lower() if sec_alloc_cat is not None else ""

                sec_can_move = sec_alloc_norm in {
                    "available",
                    "available upon request",
                    "available_on_request",  # defensive normalization
                }

                # In BOTH cases, we want a recount including default (movable locations get moved first).
                flags["move_recount"] = True

                if sec_can_move:
                    # Tracks intended move INTO default for group context/math
                    transfers_in_default += qty

                    df.loc[ridx, "RecommendationType"] = "INVESTIGATE"
                    df.loc[ridx, "RecommendedQty"] = qty
                    df.loc[ridx, "Reason"] = (
                        "MOVE + RECOUNT: secondary > system and location is movable (AVAILABLE / AVAILABLE UPON REQUEST). "
                        f"Physically move excess to default '{default_loc}', then recount including default."
                    )
                    df.loc[ridx, "Confidence"] = "Med" if flags["secured_variance"] else "High"
                    df.loc[ridx, "Severity"] = 80

                    # Optional: only emit a transfer suggestion when transfer mode is enabled.
                    # Note: this is an alternative execution method; primary outcome is still recount.
                    if use_transfers:
                        transfers.append(TransferSuggestion(
                            whs=str(whs), item=str(item), batch_lot=str(lot),
                            qty=qty, from_location=loc, to_location=default_loc,
                            reason=(
                                "Transfer enabled (alternative to adjustment). Planned physical move to default before recount."
                            ),
                            confidence="Med" if flags["secured_variance"] else "High",
                            severity=80,
                        ))
                else:
                    df.loc[ridx, "RecommendationType"] = "INVESTIGATE"
                    df.loc[ridx, "RecommendedQty"] = qty
                    df.loc[ridx, "Reason"] = (
                        "RECOUNT REQUIRED: secondary > system but location is NOT movable. "
                        f"Recount this location and default '{default_loc}' to resolve."
                    )
                    df.loc[ridx, "Confidence"] = "Med" if flags["secured_variance"] else "High"
                    df.loc[ridx, "Severity"] = 85


            elif delta < 0:
                # Secondary has less physical than system (shortage at secondary).
                # New rule:
                # - If secondary is movable (AVAILABLE / AVAILABLE UPON REQUEST) AND default is not verified (count==0),
                #   assume techs may have replenished default from overstock -> MOVE + RECOUNT including default.
                # - Otherwise, standard: recommend ADJUST down at secondary (or TRANSFER secondary->default if enabled).

                qty = float(abs(delta))

                sec_alloc_cat = row["Allocation Category"] if "Allocation Category" in row else None
                sec_alloc_norm = str(sec_alloc_cat).strip().lower() if sec_alloc_cat is not None else ""

                sec_can_move = sec_alloc_norm in {
                    "available",
                    "available upon request",
                    "available_on_request",
                }

                default_not_verified = (default_count == 0)

                if sec_can_move and default_not_verified:
                    # Treat as "could have replenished default from overstock"
                    flags["move_recount"] = True
                    transfers_in_default += qty  # reflects likely movement into default for group context

                    df.loc[ridx, "RecommendationType"] = "INVESTIGATE"
                    df.loc[ridx, "RecommendedQty"] = qty
                    df.loc[ridx, "Reason"] = (
                        "MOVE + RECOUNT: secondary < system and default is not verified (count 0). "
                        f"Possible replenishment from overstock into default '{default_loc}'. "
                        "Recount including default before any adjustment."
                    )
                    df.loc[ridx, "Confidence"] = "Med" if flags["secured_variance"] else "High"
                    df.loc[ridx, "Severity"] = 80

                    # Optional: only emit a transfer suggestion when transfer mode is enabled
                    if use_transfers:
                        transfers.append(TransferSuggestion(
                            whs=str(whs), item=str(item), batch_lot=str(lot),
                            qty=qty, from_location=loc, to_location=default_loc,
                            reason="Transfer enabled (alternative). Possible overstock replenished default; recount required.",
                            confidence="Med" if flags["secured_variance"] else "High",
                            severity=80,
                        ))

                else:
                    # Standard handling: secondary must be accurate
                    if use_transfers:
                        transfers_in_default += qty
                        transfers.append(TransferSuggestion(
                            whs=str(whs), item=str(item), batch_lot=str(lot),
                            qty=qty, from_location=loc, to_location=default_loc,
                            reason="Secondary < system; move system qty out so secondary is accurate.",
                            confidence="Med" if flags["secured_variance"] else "High",
                            severity=90,
                        ))

                        df.loc[ridx, "RecommendationType"] = "TRANSFER"
                        df.loc[ridx, "RecommendedQty"] = qty
                        df.loc[ridx, "FromLocation"] = loc
                        df.loc[ridx, "ToLocation"] = default_loc
                        df.loc[ridx, "Reason"] = "Secondary < system; transfer to default to reconcile (adjustment alternative)."
                    else:
                        df.loc[ridx, "RecommendationType"] = "ADJUST"
                        df.loc[ridx, "SetLocationQtyTo"] = float(df.loc[ridx, "CountQty"])
                        df.loc[ridx, "RecommendedQty"] = qty
                        df.loc[ridx, "Reason"] = "Secondary < system; adjust down at this location."

                    df.loc[ridx, "Confidence"] = "Med" if flags["secured_variance"] else "High"
                    df.loc[ridx, "Severity"] = 90


            else:
                # no action for this secondary line
                df.loc[ridx, "RecommendationType"] = "NO_ACTION"
                df.loc[ridx, "Reason"] = "Secondary matches system."
                df.loc[ridx, "Confidence"] = "High"
                df.loc[ridx, "Severity"] = 0

        # Compute default after transfers
        default_after = default_system + transfers_in_default - transfers_out_default

        # If group requires MOVE + RECOUNT, do not auto-adjust default.
        # The recount result must drive the final adjustment decision.
        if flags.get("move_recount"):
            remaining_adj = 0.0
            for didx in default_rows.index:
                df.loc[didx, "RecommendationType"] = "INVESTIGATE"
                df.loc[didx, "Reason"] = (
                    f"MOVE + RECOUNT required due to secondary variance; recount including default '{default_loc}' "
                    "before making any adjustment."
                )
                df.loc[didx, "Confidence"] = "Med"
                df.loc[didx, "Severity"] = max(df.loc[didx, "Severity"], 80)


        # Determine remaining adjustment AFTER transfers
        remaining_adj = 0.0
        default_reason_lines = []
        group_conf = "High"
        group_sev = 0
        group_flags_str = []

        if flags["missing_master"]:
            group_flags_str.append("MissingMaster")
            group_sev = max(group_sev, 100)
            group_conf = _confidence_cap(group_conf, "Med")

        if flags["secured_variance"]:
            group_flags_str.append("SecuredVariance")
            group_sev = max(group_sev, 95)
            # allow investigate to be High, but cap auto-fix confidence
            group_conf = _confidence_cap(group_conf, "Med")

        if default_count == 0:
            # NEW RULE:
            # For unsecured+available defaults, if any qty exists in ST01, assume the default location may be physically full,
            # so default showing empty is not an issue.
            if default_eligible and sys_st01 > 0:
                default_reason_lines.append(
                    "Default location shows 0, but ST01 shows inventory. Assume default may be physically full; no default-empty issue."
                )
                remaining_adj = 0.0

                # Mark default row(s) as no action (transfers may still reconcile secondaries)
                for didx in default_rows.index:
                    df.loc[didx, "RecommendationType"] = "NO_ACTION"
                    df.loc[didx, "Reason"] = " ".join(default_reason_lines)
                    df.loc[didx, "Confidence"] = group_conf
                    df.loc[didx, "Severity"] = max(df.loc[didx, "Severity"], 25)

            else:
                flags["default_empty"] = True
                group_flags_str.append("DefaultEmpty")
                group_sev = max(group_sev, 85)
                group_conf = _confidence_cap(group_conf, "Med")

                default_reason_lines.append(
                    f"Default counted 0 with system {default_system:g}; verify on-hand and adjust down if empty."
                )
                # No min/max enforcement when default is empty; do not propose adjustment here.
                remaining_adj = 0.0

                # Mark default row(s)
                for didx in default_rows.index:
                    df.loc[didx, "RecommendationType"] = "INVESTIGATE"
                    df.loc[didx, "Reason"] = "Default counted 0 while system shows inventory; verify on-hand and adjust if empty."
                    df.loc[didx, "Confidence"] = group_conf
                    df.loc[didx, "Severity"] = max(df.loc[didx, "Severity"], 85)

        else:
            if default_eligible:
                # Apply min/max with ST01, but only on default (and only when default_count > 0 which is true here)
                min_expected = default_after
                max_expected = default_after + sys_st01

                default_reason_lines.append("Applied ST01 min/max rule on default (unsecured+available).")
                default_reason_lines.append(f"MIN={min_expected:g}, MAX={max_expected:g}, DefaultCount={default_count:g}.")

                if default_count < min_expected:
                    remaining_adj = -(min_expected - default_count)
                    default_reason_lines.append("Default below MIN after transfers; adjust down remaining shortage.")
                elif default_count > max_expected:
                    remaining_adj = +(default_count - max_expected)
                    default_reason_lines.append("Default above MAX after transfers; adjust up remaining overage.")
                else:
                    remaining_adj = 0.0
                    default_reason_lines.append("Default within expected range; no adjustment required.")

            else:
                # Not eligible => compare directly
                default_reason_lines.append("Default not eligible for ST01 min/max; compared directly after transfers.")
                remaining_adj = default_count - default_after
                if remaining_adj != 0:
                    if remaining_adj > 0:
                        default_reason_lines.append("Default count above system-after-transfers; adjust up.")
                    else:
                        default_reason_lines.append("Default count below system-after-transfers; adjust down.")
                else:
                    default_reason_lines.append("Default matches system-after-transfers; no adjustment required.")

            if remaining_adj != 0:
                group_sev = max(group_sev, 80)

        # Mark default row(s)
            for didx in default_rows.index:
                if remaining_adj != 0:
                    df.loc[didx, "RecommendationType"] = "ADJUST"
                    df.loc[didx, "SetLocationQtyTo"] = float(df.loc[didx, "CountQty"])
                    df.loc[didx, "RemainingAdjustmentQty"] = remaining_adj
                    df.loc[didx, "RecommendedQty"] = abs(remaining_adj)
                    df.loc[didx, "Reason"] = " ".join(default_reason_lines)
                    df.loc[didx, "Confidence"] = group_conf
                    df.loc[didx, "Severity"] = max(df.loc[didx, "Severity"], group_sev)
                else:
                    df.loc[didx, "RecommendationType"] = "NO_ACTION"
                    df.loc[didx, "Reason"] = " ".join(default_reason_lines)
                    df.loc[didx, "Confidence"] = group_conf
                    df.loc[didx, "Severity"] = max(df.loc[didx, "Severity"], 60 if (transfers_in_default or transfers_out_default) else 0)

        has_transfers = any(t.whs == str(whs) and t.item == str(item) and t.batch_lot == str(lot) for t in transfers)
        has_moves = has_transfers or bool(transfers_in_default or transfers_out_default)
        headline = _group_headline(flags, remaining_adj, has_moves)
        group_sev = max(group_sev, 60 if has_moves else 0)

        # Write group headline + remaining adjustment into all rows in group for easy filtering
        df.loc[g.index, "GroupHeadline"] = headline
        df.loc[g.index, "RemainingAdjustmentQty"] = remaining_adj
        df.loc[g.index, "Confidence"] = df.loc[g.index, "Confidence"].replace("", group_conf)
        df.loc[g.index, "Severity"] = df.loc[g.index, "Severity"].clip(lower=group_sev)

        group_rows.append({
            "Whs": whs,
            "Item": item,
            "Batch/lot": lot,
            "DefaultLocation": default_loc,
            "SystemTotal": float(g["SystemQty"].sum()),
            "CountTotal": float(g["CountQty"].sum()),
            "NetVariance": float(g["VarianceQty"].sum()),
            "SysST01": sys_st01,
            "DefaultSystemAfter": float(default_after),
            "DefaultCount": float(default_count),
            "Flags": ",".join(group_flags_str) if group_flags_str else "",
            "RecommendationHeadline": headline,
            "RemainingAdjustmentQty": float(remaining_adj),
            "Confidence": group_conf,
            "Severity": int(group_sev),
        })

    transfers_df = pd.DataFrame([t.__dict__ for t in transfers]) if use_transfers else pd.DataFrame()
    if not transfers_df.empty:
        transfers_df = transfers_df.rename(
            columns={
                "batch_lot": "Batch/lot",
                "from_location": "FromLocation",
                "to_location": "ToLocation",
                "qty": "Qty",
            }
        )
        transfers_df = transfers_df[[
            "whs", "item", "Batch/lot", "Qty", "FromLocation", "ToLocation",
            "reason", "confidence", "severity"
        ]].rename(columns={
            "whs": "Whs",
            "item": "Item",
            "reason": "Reason",
            "confidence": "Confidence",
            "severity": "Severity",
        })

    group_summary_df = pd.DataFrame(group_rows)

    return df, transfers_df, group_summary_df

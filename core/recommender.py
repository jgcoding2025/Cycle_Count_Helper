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


def _group_headline(flags: dict, remaining_adj: float, has_mirrored: bool) -> str:
    if flags.get("missing_master"):
        return "Investigate: location not in master"
    if flags.get("move_recount"):
        return "Investigate: move + recount recommended"
    if flags.get("secured_variance"):
        return "Investigate: secured location variance"
    if flags.get("default_empty"):
        return "Action needed: default counted zero (verify on-hand)"

    if remaining_adj != 0:
        direction = "up" if remaining_adj > 0 else "down"
        qty = abs(remaining_adj)

        if has_mirrored:
            return f"Adjust {direction} {qty:g} after transfers"
        else:
            return f"Adjust {direction} {qty:g}"

    if has_mirrored:
        return "Moves or transfers only (resolve via default)"

    return "No variance"

def apply_recommendations(
    review_lines: pd.DataFrame,
    transfer_mode: str = "TRANSFER",
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
    df["SuggestedEntryQty"] = pd.NA
    df["FromLocation"] = ""
    df["ToLocation"] = ""
    df["RemainingAdjustmentQty"] = 0.0
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
            default_loc = (
                str(g["DefaultLocation"].dropna().astype(str).head(1).values[0])
                if (g["DefaultLocation"] != "").any()
                else ""
            )
            sys_st01 = float(g.loc[g["Location"] == "ST01", "SystemQty"].sum())
            net_variance = float(g.loc[g["Location"] != "ST01", "CountQty"].sum() - g.loc[g["Location"] != "ST01", "SystemQty"].sum())

            for ridx, row in g.iterrows():
                loc = str(row["Location"])
                if loc == "ST01":
                    df.loc[ridx, "RecommendationType"] = "NO_ACTION"
                    df.loc[ridx, "Reason"] = "ST01 is system-only; do not count or adjust."
                    df.loc[ridx, "Confidence"] = "High"
                    df.loc[ridx, "Severity"] = 0
                    continue

                df.loc[ridx, "IsSecondary"] = "Y"
                system_qty = float(row["SystemQty"])
                count_qty = float(row["CountQty"])
                delta = count_qty - system_qty

                if delta == 0:
                    df.loc[ridx, "RecommendationType"] = "NO_ACTION"
                    df.loc[ridx, "Reason"] = "Non-WHS 50: location matches system."
                    df.loc[ridx, "Confidence"] = "High"
                    df.loc[ridx, "Severity"] = 0
                else:
                    df.loc[ridx, "RecommendationType"] = "ADJUST"
                    df.loc[ridx, "RecommendedQty"] = abs(delta)
                    df.loc[ridx, "RemainingAdjustmentQty"] = delta
                    df.loc[ridx, "Reason"] = "Non-WHS 50: adjust secondary location to physical count."
                    df.loc[ridx, "Confidence"] = "High"
                    df.loc[ridx, "Severity"] = 80

            headline = "No variance" if net_variance == 0 else f"Adjust {'up' if net_variance > 0 else 'down'} {abs(net_variance):g}"
            df.loc[g.index, "GroupHeadline"] = headline

            group_rows.append({
                "Whs": whs,
                "Item": item,
                "Batch/lot": lot,
                "DefaultLocation": default_loc,
                "SystemTotal": float(g["SystemQty"].sum()),
                "CountTotal": float(g["CountQty"].sum()),
                "NetVariance": float(g["VarianceQty"].sum()),
                "SysST01": sys_st01,
                "DefaultSystemAfter": None,
                "DefaultCount": None,
                "Flags": "NonWHS50",
                "RecommendationHeadline": headline,
                "RemainingAdjustmentQty": float(net_variance),
                "Confidence": "High",
                "Severity": 80 if net_variance != 0 else 0,
            })
            continue

        # Flags
        flags = {
            "missing_master": (g["Missing Location Master"] == "Y").any(),
            "secured_variance": False,
            "default_empty": False,
            "move_recount": False,
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

        # Mark default vs secondary (ST01 is neither)
        is_default_mask = g["Location"] == default_loc
        is_st01_mask = g["Location"] == "ST01"
        df.loc[g.index[is_default_mask], "IsDefault"] = "Y"
        df.loc[g.index[~is_default_mask & ~is_st01_mask], "IsSecondary"] = "Y"

        # ST01 is system-only; do not expect physical count
        for st_idx in g.index[is_st01_mask]:
            df.loc[st_idx, "RecommendationType"] = "NO_ACTION"
            df.loc[st_idx, "Reason"] = "ST01 is system-only; do not count or adjust."
            df.loc[st_idx, "Confidence"] = "High"
            df.loc[st_idx, "Severity"] = 0

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
        default_counted = bool(default_rows["CountQty"].notna().any())

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
            loc_type = str(row.get("Location Type", "")).strip().lower()
            is_secured = loc_type == "secured"

            if is_secured and delta != 0:
                df.loc[ridx, "RecommendationType"] = "ADJUST"
                df.loc[ridx, "RecommendedQty"] = abs(delta)
                df.loc[ridx, "Reason"] = "Secured variance; adjust to physical count (no move or ST01 tolerance)."
                df.loc[ridx, "Confidence"] = "Med"
                df.loc[ridx, "Severity"] = 95
                continue

            if delta > 0:
                qty = float(delta)
                default_not_verified = (not default_counted) or default_count == 0
                move_recount = default_not_verified or sys_st01 > 0

                # Scenario A: secondary +5, default not counted, SysST01 > 0 => move + recount, no net adjust.
                # Scenario B: secondary +3, default counted 0, SysST01 > 0 => move + recount, avoid net adjust.
                if move_recount:
                    flags["move_recount"] = True
                    transfers_in_default += qty
                    df.loc[ridx, "RecommendationType"] = "INVESTIGATE"
                    df.loc[ridx, "RecommendedQty"] = qty
                    df.loc[ridx, "Reason"] = (
                        "MOVE + RECOUNT: secondary over system; move excess to default and recount default."
                    )
                    df.loc[ridx, "Confidence"] = "Med"
                    df.loc[ridx, "Severity"] = 70
                else:
                    transfers_out_default += qty
                    if use_transfers:
                        transfers.append(TransferSuggestion(
                            whs=str(whs), item=str(item), batch_lot=str(lot),
                            qty=qty, from_location=default_loc, to_location=loc,
                            reason="Secondary location must be accurate; system short vs count.",
                            confidence="Med" if flags["secured_variance"] else "High",
                            severity=90,
                        ))

                        # Mark line recommendation
                        df.loc[ridx, "RecommendationType"] = "TRANSFER"
                        df.loc[ridx, "RecommendedQty"] = qty
                        df.loc[ridx, "FromLocation"] = default_loc
                        df.loc[ridx, "ToLocation"] = loc
                        df.loc[ridx, "Reason"] = "Secondary > system; transfer from default to reconcile."
                    else:
                        df.loc[ridx, "RecommendationType"] = "ADJUST"
                        df.loc[ridx, "RecommendedQty"] = qty
                        df.loc[ridx, "Reason"] = (
                            "Secondary > system; adjust up here (transfer math preserved)."
                        )
                    df.loc[ridx, "Confidence"] = "Med" if flags["secured_variance"] else "High"
                    df.loc[ridx, "Severity"] = 90

            elif delta < 0:
                qty = float(abs(delta))

                # Scenario C: secondary -4, default counted and verified => transfer/move to default or adjust.
                transfers_in_default += qty

                if use_transfers:
                    transfers.append(TransferSuggestion(
                        whs=str(whs), item=str(item), batch_lot=str(lot),
                        qty=qty, from_location=loc, to_location=default_loc,
                        reason="Secondary location must be accurate; excess system qty moved out.",
                        confidence="Med" if flags["secured_variance"] else "High",
                        severity=90,
                    ))

                    df.loc[ridx, "RecommendationType"] = "TRANSFER"
                    df.loc[ridx, "RecommendedQty"] = qty
                    df.loc[ridx, "FromLocation"] = loc
                    df.loc[ridx, "ToLocation"] = default_loc
                    df.loc[ridx, "Reason"] = "Secondary < system; transfer to default to reconcile."
                else:
                    df.loc[ridx, "RecommendationType"] = "ADJUST"
                    df.loc[ridx, "RecommendedQty"] = qty
                    df.loc[ridx, "Reason"] = (
                        "Secondary < system; adjust down here (transfer math preserved)."
                    )
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

        if flags["move_recount"]:
            group_flags_str.append("MoveRecount")
            group_sev = max(group_sev, 70)
            group_conf = _confidence_cap(group_conf, "Med")

        avoid_net_adjust = flags["move_recount"]
        target_default_qty = None
        default_action = "NO_ACTION"

        if default_eligible and default_count == 0 and sys_st01 > 0:
            default_reason_lines.append(
                "ST01 present; default may be physically full (no default-empty issue)."
            )
            target_default_qty = default_after
        elif default_count == 0:
            flags["default_empty"] = True
            group_flags_str.append("DefaultEmpty")
            group_sev = max(group_sev, 85)
            group_conf = _confidence_cap(group_conf, "Med")
            default_action = "INVESTIGATE"
            default_reason_lines.append(
                f"Default counted 0 with system {default_system:g}; verify on-hand and adjust down if empty."
            )
        else:
            if default_eligible:
                # Apply min/max with ST01 on default (unsecured+available).
                min_expected = default_after
                max_expected = default_after + sys_st01
                target_default_qty = min_expected

                default_reason_lines.append("Applied ST01 min/max rule on default (unsecured+available).")
                default_reason_lines.append(f"MIN={min_expected:g}, MAX={max_expected:g}, DefaultCount={default_count:g}.")

                if default_count < min_expected:
                    target_default_qty = min_expected
                    default_reason_lines.append("Default below MIN; adjust to MIN.")
                elif default_count > max_expected:
                    target_default_qty = max_expected
                    default_reason_lines.append("Default above MAX; adjust to MAX.")
                else:
                    default_reason_lines.append("Default within [MIN, MAX]; snap target entry to MIN.")
            else:
                # Not eligible => compare directly
                default_reason_lines.append("Default not eligible for ST01 min/max; compared directly after transfers.")
                target_default_qty = default_after
                if default_count > default_after:
                    default_reason_lines.append("Default count above system-after-transfers; adjust down.")
                elif default_count < default_after:
                    default_reason_lines.append("Default count below system-after-transfers; adjust up.")
                else:
                    default_reason_lines.append("Default matches system-after-transfers; no adjustment required.")

            remaining_adj = default_count - target_default_qty if target_default_qty is not None else 0.0
            if remaining_adj != 0:
                default_action = "ADJUST"
                group_sev = max(group_sev, 80)

        if avoid_net_adjust and default_action == "ADJUST":
            default_action = "INVESTIGATE"
            remaining_adj = 0.0
            default_reason_lines.append(
                "MOVE + RECOUNT recommended; avoid net adjustments while variance may be staging/ST01."
            )
        elif avoid_net_adjust and default_action == "NO_ACTION":
            default_reason_lines.append(
                "MOVE + RECOUNT recommended; verify default after move before adjustments."
            )

        # Mark default row(s)
        for didx in default_rows.index:
            if target_default_qty is not None:
                df.loc[didx, "SuggestedEntryQty"] = target_default_qty
            if default_action == "ADJUST":
                df.loc[didx, "RecommendationType"] = "ADJUST"
                df.loc[didx, "RemainingAdjustmentQty"] = remaining_adj
                df.loc[didx, "RecommendedQty"] = abs(remaining_adj)
                df.loc[didx, "Reason"] = " ".join(default_reason_lines)
                df.loc[didx, "Confidence"] = group_conf
                df.loc[didx, "Severity"] = max(df.loc[didx, "Severity"], group_sev)
            elif default_action == "INVESTIGATE":
                df.loc[didx, "RecommendationType"] = "INVESTIGATE"
                df.loc[didx, "Reason"] = " ".join(default_reason_lines)
                df.loc[didx, "Confidence"] = group_conf
                df.loc[didx, "Severity"] = max(df.loc[didx, "Severity"], 85)
            else:
                df.loc[didx, "RecommendationType"] = "NO_ACTION"
                df.loc[didx, "Reason"] = " ".join(default_reason_lines)
                df.loc[didx, "Confidence"] = group_conf
                df.loc[didx, "Severity"] = max(
                    df.loc[didx, "Severity"],
                    60 if (transfers_in_default or transfers_out_default) else 0,
                )

        if not use_transfers and not default_rows.empty and not flags["move_recount"]:
            transfer_adjust_default = default_after - default_system
            total_default_adjust = transfer_adjust_default + remaining_adj
            if total_default_adjust != 0 and (df.loc[default_rows.index, "RecommendationType"] != "INVESTIGATE").all():
                direction = "up" if total_default_adjust > 0 else "down"
                for didx in default_rows.index:
                    existing_reason = str(df.loc[didx, "Reason"])
                    transfer_reason = (
                        f"Transfer alternative: adjust default {direction} {abs(total_default_adjust):g} "
                        f"to offset secondary adjustments."
                    )
                    combined_reason = " ".join([existing_reason, transfer_reason]).strip()
                    df.loc[didx, "RecommendationType"] = "ADJUST"
                    df.loc[didx, "RecommendedQty"] = abs(total_default_adjust)
                    df.loc[didx, "Reason"] = combined_reason
                    df.loc[didx, "Confidence"] = group_conf
                    df.loc[didx, "Severity"] = max(df.loc[didx, "Severity"], group_sev)

        has_mirrored = bool(transfers_in_default or transfers_out_default)
        headline = _group_headline(flags, remaining_adj, has_mirrored)
        group_sev = max(group_sev, 60 if has_mirrored else 0)

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

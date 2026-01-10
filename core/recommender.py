from __future__ import annotations

import pandas as pd


def _is_default_eligible(loc_type: str | None, alloc_cat: str | None) -> bool:
    if loc_type is None or alloc_cat is None:
        return False
    return str(loc_type).strip().lower() == "unsecured" and str(alloc_cat).strip().lower() == "available"


def _confidence_cap(conf: str, cap: str) -> str:
    order = {"Low": 0, "Med": 1, "High": 2}
    inv = {v: k for k, v in order.items()}
    return inv[min(order.get(conf, 1), order.get(cap, 1))]


def _group_headline(flags: dict, net_group_adj: float, has_adjustments: bool) -> str:
    if flags.get("missing_master"):
        return "Investigate: location not in master"
    if flags.get("secured_variance"):
        return "Investigate: secured location variance"

    if abs(net_group_adj) > 0:
        direction = "up" if net_group_adj > 0 else "down"
        return f"Adjust {direction} {abs(net_group_adj):g}"

    if has_adjustments:
        return "Adjust (Net 0)"

    return "No variance"


def apply_recommendations(
    review_lines: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Input: Review_Lines dataframe from Step 2 (already enriched with Location Type / Allocation Category)
    Output:
      - updated review_lines with recommendation columns added
      - empty transfer_suggestions dataframe (transfers are not recommended)
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
    df["RemainingAdjustmentQty"] = 0.0
    # Default: set-to = counted qty (trusted physical)
    df["SetLocationQtyTo"] = pd.to_numeric(df["CountQty"], errors="coerce")

    # ST01 is never adjusted → leave blank
    df.loc[df["Location"] == "ST01", "SetLocationQtyTo"] = pd.NA
    df["Reason"] = ""
    df["Confidence"] = "Med"
    df["Severity"] = 0
    df["GroupHeadline"] = ""

    group_rows = []

    # Group by Item
    gcols = ["Item"]
    for (item), g in df.groupby(gcols, sort=False):
        g = g.copy()

        # Group is by Item (warehouse ignored). Keep summary values for reporting/UI.
        whs_summary = "MULTI" if g["Whs"].nunique() > 1 else str(g["Whs"].iloc[0])

        if "Batch/lot" in g.columns:
            lot_summary = str(g["Batch/lot"].iloc[0])
        else:
            lot_summary = ""


        # Flags
        flags = {
            "missing_master": (g["Missing Location Master"] == "Y").any(),
            "secured_variance": False,
            "default_empty": False,
        }

        # Hard stop: if any row is missing location master, do not auto-recommend adjustments
        if flags["missing_master"]:
            idx = g.index
            df.loc[idx, "RecommendationType"] = "INVESTIGATE"
            df.loc[idx, "Reason"] = "Missing Location Master = Y for at least one row in this group; verify master data before any adjustments."
            df.loc[idx, "Confidence"] = "Low"
            df.loc[idx, "Severity"] = 100
            df.loc[idx, "GroupHeadline"] = "Investigate: location not in master"

            group_rows.append({
                "Whs": whs_summary,
                "Item": item,
                "Batch/lot": lot_summary,
                "DefaultLocation": str(g["DefaultLocation"][g["DefaultLocation"] != ""].head(1).values[0]) if (g["DefaultLocation"] != "").any() else "",
                "SystemTotal": float(g["SystemQty"].sum()),
                "CountTotal": float(g["CountQty"].sum()),
                "NetVariance": float(g["VarianceQty"].sum()),
                "SysST01": float(g.loc[g["Location"] == "ST01", "SystemQty"].sum()),
                "DefaultSystemAfter": None,
                "DefaultCount": None,
                "Flags": "MissingMaster",
                "RecommendationHeadline": "Investigate: location not in master",
                "RemainingAdjustmentQty": 0.0,
                "Confidence": "Low",
                "Severity": 100,
            })
            continue


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
                "Whs": whs_summary,
                "Item": item, "Batch/lot": lot_summary,
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
                "Whs": whs_summary,
                "Item": item, "Batch/lot": lot_summary,
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

        secondary = g[(g["Location"] != default_loc) & (g["Location"] != "ST01")].copy()

        for ridx, row in secondary.iterrows():
            loc = str(row["Location"])
            system_qty = float(row["SystemQty"])
            count_qty = float(row["CountQty"])
            delta = count_qty - system_qty

            if delta > 0:
                # Secondary has more physical than system (overage at secondary).
                qty = float(delta)

                df.loc[ridx, "RecommendationType"] = "ADJUST"
                df.loc[ridx, "SetLocationQtyTo"] = float(df.loc[ridx, "CountQty"])
                df.loc[ridx, "RecommendedQty"] = qty
                df.loc[ridx, "Reason"] = "Secondary > system; adjust up at this location."
                df.loc[ridx, "Confidence"] = "Med" if flags["secured_variance"] else "High"
                df.loc[ridx, "Severity"] = 80
            elif delta < 0:
                # Secondary has less physical than system (shortage at secondary).
                qty = float(abs(delta))

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

        # Net delta from all non-default, non-ST01 locations (what we're correcting via ADJUSTs)
        net_secondary_delta = float((secondary["CountQty"] - secondary["SystemQty"]).sum())

        # Balancing adjustment at default so net adjustment = 0
        # If secondaries total +10, default should be -10
        balancing_adj = -net_secondary_delta

        remaining_adj = 0.0

        # Determine remaining adjustment
        remaining_adj = 0.0
        default_reason_lines = []
        group_conf = "High"
        group_sev = 0
        group_flags_str = []

        if flags["missing_master"]:
            group_flags_str.append("MissingMaster")
            group_sev = max(group_sev, 100)
            group_conf = _confidence_cap(group_conf, "Med")

        # If secured variance exists, do NOT auto-adjust (policy decision).
        if flags.get("secured_variance"):
            group_flags_str.append("SecuredVariance")
            group_sev = max(group_sev, 95)
            group_conf = _confidence_cap(group_conf, "Med")

            for i in g.index:
                # If you only want secured locations investigated, scope this to those rows.
                df.loc[i, "RecommendationType"] = "INVESTIGATE"
                df.loc[i, "Reason"] = "Secured location variance present; investigate before adjusting."
                df.loc[i, "Confidence"] = group_conf
                df.loc[i, "Severity"] = max(df.loc[i, "Severity"], 95)

            remaining_adj = 0.0

        else:
            # Priority 1: balancing when secondary variance exists (net zero rule)
            if net_secondary_delta != 0:
                # Desired default adjustment to offset secondaries
                desired_default_adj = -net_secondary_delta

                # Cap: default cannot be reduced below 0
                # Min adjustment is -default_system (brings system down to zero)
                min_default_adj = -float(default_system)
                remaining_adj = max(desired_default_adj, min_default_adj)

                net_group_adj = net_secondary_delta + remaining_adj

                default_reason_lines.append(
                    f"Balancing default to offset secondary changes. "
                    f"Secondary net changes={net_secondary_delta:g}; desired default adjust={desired_default_adj:g}. "
                    f"Capped default adjust at {remaining_adj:g} to avoid negative location qty."
                )

                if abs(net_group_adj) > 0 and sys_st01 > 0:
                    default_reason_lines.append(
                        f"Net adjustment is {net_group_adj:g}. Alt: Check ST01. "
                        f"Possible ST01 offline/relief activity from default could explain the difference."
                    )

            # Priority 2: only if NO secondary variance, consider ST01/direct compare
            else:
                if default_eligible:
                    min_expected = default_system
                    max_expected = default_system + sys_st01

                    if default_count < min_expected:
                        remaining_adj = -(min_expected - default_count)
                        default_reason_lines.append(
                            f"Applied ST01 min/max rule on default (unsecured+available). "
                            f"MIN={min_expected:g}, MAX={max_expected:g}, DefaultCount={default_count:g}. "
                            f"Default below MIN; adjust down remaining shortage."
                        )
                    elif default_count > max_expected:
                        remaining_adj = +(default_count - max_expected)
                        default_reason_lines.append(
                            f"Applied ST01 min/max rule on default (unsecured+available). "
                            f"MIN={min_expected:g}, MAX={max_expected:g}, DefaultCount={default_count:g}. "
                            f"Default above MAX; adjust up remaining excess."
                        )
                    else:
                        remaining_adj = 0.0
                        default_reason_lines.append(
                            f"Applied ST01 min/max rule on default (unsecured+available). "
                            f"MIN={min_expected:g}, MAX={max_expected:g}, DefaultCount={default_count:g}. "
                            f"Default within range; no default adjustment."
                        )
                else:
                    # direct compare
                    if default_count != default_system:
                        remaining_adj = default_count - default_system
                        direction = "up" if remaining_adj > 0 else "down"
                        default_reason_lines.append(
                            f"Default {direction} needed: system {default_system:g}, count {default_count:g}."
                        )
                    else:
                        remaining_adj = 0.0
                        default_reason_lines.append("Default matches system; no adjustment.")


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
                    df.loc[didx, "Severity"] = max(df.loc[didx, "Severity"], 0)

        has_adjustments = (df.loc[g.index, "RecommendationType"] == "ADJUST").any()

        net_group_adj = net_secondary_delta + remaining_adj
        headline = _group_headline(flags, net_group_adj, has_adjustments)

        # Write group headline + remaining adjustment into all rows in group for easy filtering
        df.loc[g.index, "GroupHeadline"] = headline
        df.loc[g.index, "RemainingAdjustmentQty"] = remaining_adj
        df.loc[g.index, "Confidence"] = df.loc[g.index, "Confidence"].replace("", group_conf)
        df.loc[g.index, "Severity"] = df.loc[g.index, "Severity"].clip(lower=group_sev)

        default_after = default_system + remaining_adj

        group_rows.append({
            "Whs": whs_summary,
            "Item": item,
            "Batch/lot": lot_summary,
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

    transfers_df = pd.DataFrame()

    group_summary_df = pd.DataFrame(group_rows)

    return df, transfers_df, group_summary_df

from __future__ import annotations

import pandas as pd


# -----------------------------
# Helpers
# -----------------------------
def _norm_str(x) -> str:
    if x is None or pd.isna(x):
        return ""
    return str(x).strip()


def _norm_upper(x) -> str:
    return _norm_str(x).upper()


def _norm_lower(x) -> str:
    return _norm_str(x).lower()


def _to_num(x) -> float:
    # Safe numeric conversion (NaN -> 0)
    try:
        v = pd.to_numeric(x, errors="coerce")
        if pd.isna(v):
            return 0.0
        return float(v)
    except Exception:
        return 0.0


def _alloc_is_available_or_upr(alloc_cat: str | None) -> bool:
    """
    Business rule uses: Allocation Category is Available OR Available upon request
    (case-insensitive)
    """
    a = _norm_lower(alloc_cat)
    return a in {"available", "available upon request"}


def _default_is_unsecured(loc_type: str | None) -> bool:
    return _norm_lower(loc_type) == "unsecured"


def _eligible_for_st01_logic(
    default_loc_type: str | None,
    default_alloc_cat: str | None,
    st01_qty: float,
) -> bool:
    # Step C.1
    return _default_is_unsecured(default_loc_type) and _alloc_is_available_or_upr(default_alloc_cat) and (st01_qty != 0)


def _scenario_label(eligible_for_st01: bool, unbalanced_secondary: bool) -> str:
    # Step E
    if (not eligible_for_st01) and (not unbalanced_secondary):
        return "Scenario 1"
    if eligible_for_st01 and (not unbalanced_secondary):
        return "Scenario 2"
    if (not eligible_for_st01) and unbalanced_secondary:
        return "Scenario 3"
    return "Scenario 4"


def _group_headline(investigate: bool, has_adjustments: bool, net_group_variance: float) -> str:
    """
    Simple headline that stays compatible with UI filtering.
    We prefer:
      - Investigate (when rules say so)
      - Adjust (if any location set-to differs from system)
      - No variance otherwise
    """
    if investigate:
        return "Investigate"
    if has_adjustments:
        # Optional directional hint
        if abs(net_group_variance) > 0:
            direction = "up" if net_group_variance > 0 else "down"
            return f"Adjust {direction} {abs(net_group_variance):g}"
        return "Adjust"
    return "No variance"

def apply_recommendations(review_lines: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Implements Business Rules (Steps A–H) from business_logic.txt.

    Input: Review_Lines dataframe from Step 2 (already enriched with Location Type / Allocation Category)
    Output:
      - updated review_lines with recommendation columns added
      - empty transfer_suggestions dataframe (transfers are not system-generated adjustments; only guidance text)
      - group_summary dataframe
    """

    required = [
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
    missing = [c for c in required if c not in review_lines.columns]
    if missing:
        raise ValueError(f"Review_Lines missing required columns for Step 3: {missing}")

    df = review_lines.copy()

    # Normalize grouping keys
    df["Whs"] = df["Whs"].astype("string").map(_norm_str)
    df["Item"] = df["Item"].astype("string").map(_norm_str)
    df["Batch/lot"] = df["Batch/lot"].astype("string").fillna("").map(_norm_str)
    df["Location"] = df["Location"].astype("string").map(_norm_upper)
    df["DefaultLocation"] = df["DefaultLocation"].astype("string").fillna("").map(_norm_upper)

    # Ensure numeric columns
    df["SystemQty"] = pd.to_numeric(df["SystemQty"], errors="coerce").fillna(0.0)
    df["CountQty"] = pd.to_numeric(df["CountQty"], errors="coerce").fillna(0.0)
    df["VarianceQty"] = pd.to_numeric(df["VarianceQty"], errors="coerce").fillna(0.0)

    # Output columns expected by UI
    df["IsDefault"] = "N"
    df["IsSecondary"] = "N"

    df["RecommendationType"] = ""
    df["SetLocationQtyTo"] = None  # will be set per rules
    df["Reason"] = ""

    df["RecommendedQty"] = 0.0
    df["RemainingAdjustmentQty"] = 0.0  # we will use this to surface residual_unbalanced at group level

    df["Confidence"] = "High"
    df["Severity"] = 0
    df["GroupHeadline"] = ""

    # Extra traceability columns (won't break UI; helps auditing)
    df["_eligible_for_ST01_logic"] = ""
    df["_unbalanced_secondary"] = ""
    df["_scenario"] = ""
    df["_residual_unbalanced"] = 0.0
    df["_net_secondary_adjustments"] = 0.0
    df["_default_adjusted_noconstraint"] = 0.0
    df["_default_adjusted_with_constraint"] = 0.0
    df["_default_outside_tolerance_qty"] = 0.0
    df["_investigate"] = ""

    group_rows: list[dict] = []

    # -----------------------------
    # Step A — Group by Item (ignore warehouse differences)
    # -----------------------------
    for item, g in df.groupby(["Item"], sort=False):
        g = g.copy()
        idx_all = g.index

        whs_summary = "MULTI" if g["Whs"].nunique() > 1 else str(g["Whs"].iloc[0])
        lot_summary = str(g["Batch/lot"].iloc[0]) if "Batch/lot" in g.columns else ""

        # Hard stop: Missing location master -> Investigate, no auto
        if (g["Missing Location Master"] == "Y").any():
            df.loc[idx_all, "RecommendationType"] = "INVESTIGATE"
            df.loc[idx_all, "GroupHeadline"] = "Investigate"
            df.loc[idx_all, "Reason"] = (
                "Missing Location Master = Y for at least one row in this item group; verify master data before adjustments."
            )
            df.loc[idx_all, "Confidence"] = "Low"
            df.loc[idx_all, "Severity"] = 100
            df.loc[idx_all, "_investigate"] = "Yes"
            group_rows.append(
                {
                    "Whs": whs_summary,
                    "Item": item,
                    "Batch/lot": lot_summary,
                    "DefaultLocation": "",
                    "SystemTotal": float(g["SystemQty"].sum()),
                    "CountTotal": float(g["CountQty"].sum()),
                    "NetVariance": float(g["VarianceQty"].sum()),
                    "SysST01": float(g.loc[g["Location"] == "ST01", "SystemQty"].sum()),
                    "Scenario": "",
                    "EligibleForST01Logic": "",
                    "UnbalancedSecondary": "",
                    "ResidualUnbalanced": 0.0,
                    "RecommendationHeadline": "Investigate",
                    "Reason": "Missing Location Master in group",
                }
            )
            continue

        # Determine default location (authoritative)
        defaults = [d for d in g["DefaultLocation"].unique().tolist() if _norm_upper(d) != ""]
        default_loc = defaults[0] if defaults else ""

        if default_loc == "":
            df.loc[idx_all, "RecommendationType"] = "INVESTIGATE"
            df.loc[idx_all, "GroupHeadline"] = "Investigate"
            df.loc[idx_all, "Reason"] = "DefaultLocation is blank; cannot compute default vs secondary for this item group."
            df.loc[idx_all, "Confidence"] = "Low"
            df.loc[idx_all, "Severity"] = 90
            df.loc[idx_all, "_investigate"] = "Yes"
            group_rows.append(
                {
                    "Whs": whs_summary,
                    "Item": item,
                    "Batch/lot": lot_summary,
                    "DefaultLocation": "",
                    "SystemTotal": float(g["SystemQty"].sum()),
                    "CountTotal": float(g["CountQty"].sum()),
                    "NetVariance": float(g["VarianceQty"].sum()),
                    "SysST01": float(g.loc[g["Location"] == "ST01", "SystemQty"].sum()),
                    "Scenario": "",
                    "EligibleForST01Logic": "",
                    "UnbalancedSecondary": "",
                    "ResidualUnbalanced": 0.0,
                    "RecommendationHeadline": "Investigate",
                    "Reason": "DefaultLocation missing",
                }
            )
            continue

        # Identify rows by class: Default / Secondary / ST01
        is_st01 = g["Location"] == "ST01"
        is_default = g["Location"] == default_loc
        is_secondary = (~is_st01) & (~is_default)

        df.loc[g.index[is_default], "IsDefault"] = "Y"
        df.loc[g.index[is_secondary], "IsSecondary"] = "Y"

        # Compute ST01 system qty (Step A note: ST01 excluded from secondary variance math)
        st01_qty = float(g.loc[is_st01, "SystemQty"].sum())

        # Pull default row values
        default_rows = g.loc[is_default]
        if default_rows.empty:
            df.loc[idx_all, "RecommendationType"] = "INVESTIGATE"
            df.loc[idx_all, "GroupHeadline"] = "Investigate"
            df.loc[idx_all, "Reason"] = f"DefaultLocation '{default_loc}' not present as a row in recount lines for this item."
            df.loc[idx_all, "Confidence"] = "Low"
            df.loc[idx_all, "Severity"] = 90
            df.loc[idx_all, "_investigate"] = "Yes"
            group_rows.append(
                {
                    "Whs": whs_summary,
                    "Item": item,
                    "Batch/lot": lot_summary,
                    "DefaultLocation": default_loc,
                    "SystemTotal": float(g["SystemQty"].sum()),
                    "CountTotal": float(g["CountQty"].sum()),
                    "NetVariance": float(g["VarianceQty"].sum()),
                    "SysST01": st01_qty,
                    "Scenario": "",
                    "EligibleForST01Logic": "",
                    "UnbalancedSecondary": "",
                    "ResidualUnbalanced": 0.0,
                    "RecommendationHeadline": "Investigate",
                    "Reason": "Default row missing in recount lines",
                }
            )
            continue

        default_system = float(default_rows["SystemQty"].sum())
        default_counted = float(default_rows["CountQty"].sum())
        default_loc_type = default_rows["Location Type"].iloc[0] if "Location Type" in default_rows.columns else None
        default_alloc_cat = default_rows["Allocation Category"].iloc[0] if "Allocation Category" in default_rows.columns else None

        # -----------------------------
        # Step A — Secondary adjustments
        # 1) For every secondary row: set_location_qty_to = count
        # 2) net_secondary_adjustments = Σ(count − system) over secondaries
        # -----------------------------
        secondary_rows = g.loc[is_secondary].copy()
        if not secondary_rows.empty:
            df.loc[secondary_rows.index, "SetLocationQtyTo"] = secondary_rows["CountQty"].astype(float)
            # Recommend ADJUST if it changes anything, else NO_ACTION
            sec_delta = secondary_rows["CountQty"].astype(float) - secondary_rows["SystemQty"].astype(float)
            df.loc[secondary_rows.index, "RecommendationType"] = sec_delta.apply(lambda d: "ADJUST" if d != 0 else "NO_ACTION")
            df.loc[secondary_rows.index, "RecommendedQty"] = sec_delta.abs().astype(float)
            df.loc[secondary_rows.index, "Reason"] = sec_delta.apply(
                lambda d: "Secondary set to count." if d != 0 else "Secondary already matches system."
            )
        net_secondary_adjustments = float((secondary_rows["CountQty"] - secondary_rows["SystemQty"]).sum()) if not secondary_rows.empty else 0.0

        # ST01 row: blank / excluded (Step F.2)
        if is_st01.any():
            df.loc[g.index[is_st01], "SetLocationQtyTo"] = None
            df.loc[g.index[is_st01], "RecommendationType"] = ""
            df.loc[g.index[is_st01], "RecommendedQty"] = 0.0
            df.loc[g.index[is_st01], "Reason"] = "ST01 is never adjusted; excluded from adjustment outputs."

        # -----------------------------
        # Step B — Default balancing math (always)
        # -----------------------------
        default_adjusted_noconstraint = default_system - net_secondary_adjustments
        default_adjusted_with_constraint = max(default_adjusted_noconstraint, 0.0)
        residual_unbalanced = default_adjusted_noconstraint - default_adjusted_with_constraint  # negative or 0

        # -----------------------------
        # Step C — Tags
        # -----------------------------
        eligible_for_st01_logic = _eligible_for_st01_logic(default_loc_type, default_alloc_cat, st01_qty)
        unbalanced_secondary = residual_unbalanced < 0

        # -----------------------------
        # Step D — ST01 tolerance qty (only meaningful when eligible)
        # -----------------------------
        default_outside_tolerance_qty = 0.0
        if eligible_for_st01_logic:
            expected_physical_min = default_system
            expected_physical_max = default_system + st01_qty
            if default_counted < expected_physical_min:
                default_outside_tolerance_qty = default_counted - expected_physical_min
            elif default_counted > expected_physical_max:
                default_outside_tolerance_qty = default_counted - expected_physical_max
            else:
                default_outside_tolerance_qty = 0.0

        # -----------------------------
        # Step E — Scenario 2x2
        # -----------------------------
        scenario = _scenario_label(eligible_for_st01_logic, unbalanced_secondary)

        # -----------------------------
        # Step F — Final set_location_qty_to outputs (default)
        # Implement: default_set_to = MAX( scenario_base - net_secondary_adjustments, 0 )
        # -----------------------------
        if scenario == "Scenario 1":
            # Not ST01-eligible, balanced: baseline is the observed default count
            scenario_base = default_counted

        elif scenario == "Scenario 2":
            # ST01-eligible, balanced: baseline is system plus any outside-tolerance qty
            # (outside-tolerance qty is already 0 when count within MIN/MAX)
            scenario_base = default_system + default_outside_tolerance_qty

        elif scenario in {"Scenario 3", "Scenario 4"}:
            # Unbalanced (would go < 0): baseline should be system; balancing will floor at 0
            scenario_base = default_system

        else:
            scenario_base = default_counted  # safe fallback

        default_set_to = max(float(scenario_base) - float(net_secondary_adjustments), 0.0)


        # Write default row outputs
        df.loc[default_rows.index, "SetLocationQtyTo"] = float(default_set_to)

        # RecommendationType for default based on whether it changes system qty
        default_delta = float(default_set_to) - float(default_system)
        df.loc[default_rows.index, "RecommendationType"] = "ADJUST" if default_delta != 0 else "NO_ACTION"
        df.loc[default_rows.index, "RecommendedQty"] = abs(default_delta)

        # -----------------------------
        # Step G — Investigate triggers + recommended paths forward
        # -----------------------------
        investigate = False
        reasons: list[str] = []

        # Scenario 4 rule
        if scenario == "Scenario 4":
            investigate = True
            reasons.append("Scenario 4: ST01-eligible AND unbalanced secondary (default would need < 0 to balance; clamped to 0).")

        # Residual/ST01 rule (independent)
        if (residual_unbalanced != 0) and (st01_qty > 0):
            investigate = True
            reasons.append("Residual/ST01 rule: residual_unbalanced ≠ 0 AND ST01 > 0.")

        # Exists an “Available / Available upon request” secondary with excess stock (definition)
        sec_has_excess_available_or_upr = False
        if not secondary_rows.empty:
            sec_has_excess_available_or_upr = bool(
                (
                    secondary_rows["Allocation Category"].astype("string").map(_norm_lower).isin(["available", "available upon request"])
                    & ((secondary_rows["CountQty"] - secondary_rows["SystemQty"]) > 0)
                ).any()
            )

        # Guidance text (only mention ST01 if ST01 != 0 per rules)
        guidance: list[str] = []
        if investigate:
            if sec_has_excess_available_or_upr:
                guidance.append(
                    f"Recommend: Physically transfer {abs(residual_unbalanced):g} to the Default, then update location count."
                )
            else:
                if st01_qty != 0:
                    guidance.append(f"Recommend: Check ST01 — may be inflated by approx. {abs(residual_unbalanced):g}.")
                else:
                    guidance.append(
                        f"Recommend: Investigate residual imbalance of approx. {abs(residual_unbalanced):g} (ST01 is 0, so do not use ST01 in reasoning)."
                    )

        # -----------------------------
        # Concise Reason text
        # - Always show ST01 MIN/MAX when ST01 != 0
        # -----------------------------
        reason_parts: list[str] = []

        # 1) Scenario + basic decision
        reason_parts.append(
            f"MAX of {scenario_base:g} - secondaries Δ of {net_secondary_adjustments:g} = {default_set_to:g}."
        )

        # 2) Secondary summary
        # (net_secondary_adjustments is Σ(count-system) over secondaries)
        if not secondary_rows.empty:
            reason_parts.append(f"Secondaries net Δ = {net_secondary_adjustments:g}.")
        else:
            reason_parts.append("No secondary locations.")

        # 3) ST01 MIN/MAX always when ST01 has qty
        if st01_qty != 0:
            expected_min = default_system
            expected_max = default_system + st01_qty
            # Add a short note if count is outside the plausible band
            if default_counted < expected_min:
                tol_note = f"Below MIN by {expected_min - default_counted:g}."
            elif default_counted > expected_max:
                tol_note = f"Above MAX by {default_counted - expected_max:g}."
            else:
                tol_note = "Count is within range."
            reason_parts.append(f"ST01 range: MIN {expected_min:g} / MAX {expected_max:g} (ST01 {st01_qty:g}). {tol_note}")

        # 4) Balancing math (short)
        # Only show the constraint line if it matters (Scenario 3/4 or residual exists)
        if scenario in {"Scenario 3", "Scenario 4"} or residual_unbalanced != 0:
            reason_parts.append(
                f"Balancing: Default target = {default_adjusted_noconstraint:g} → constrained to {default_adjusted_with_constraint:g} (residual {residual_unbalanced:g})."
            )

        # 5) Eligibility note (short, only if ST01 exists)
        if st01_qty != 0:
            if eligible_for_st01_logic:
                reason_parts.append("ST01 logic applied ✅")
            else:
                reason_parts.append("ST01 logic NOT eligible (shown for plausibility only).")

        # 6) Investigate + guidance
        if investigate and reasons:
            reason_parts.append("INVESTIGATE: " + " ".join(reasons))
        if guidance:
            reason_parts.append("NEXT: " + " ".join(guidance))

        df.loc[default_rows.index, "Reason"] = " | ".join(reason_parts)

        # Also stamp scenario/tags onto all rows in group for auditing/filtering
        df.loc[idx_all, "_eligible_for_ST01_logic"] = "Yes" if eligible_for_st01_logic else "No"
        df.loc[idx_all, "_unbalanced_secondary"] = "Yes" if unbalanced_secondary else "No"
        df.loc[idx_all, "_scenario"] = scenario
        df.loc[idx_all, "_residual_unbalanced"] = float(residual_unbalanced)
        df.loc[idx_all, "_net_secondary_adjustments"] = float(net_secondary_adjustments)
        df.loc[idx_all, "_default_adjusted_noconstraint"] = float(default_adjusted_noconstraint)
        df.loc[idx_all, "_default_adjusted_with_constraint"] = float(default_adjusted_with_constraint)
        df.loc[idx_all, "_default_outside_tolerance_qty"] = float(default_outside_tolerance_qty)
        df.loc[idx_all, "_investigate"] = "Yes" if investigate else "No"

        # RemainingAdjustmentQty: surface residual_unbalanced to the UI (0 when balanced)
        df.loc[idx_all, "RemainingAdjustmentQty"] = float(residual_unbalanced)

        # Confidence/Severity (simple, policy-aligned)
        if investigate:
            df.loc[idx_all, "Confidence"] = "Med"
            df.loc[idx_all, "Severity"] = df.loc[idx_all, "Severity"].clip(lower=85)
        else:
            df.loc[idx_all, "Confidence"] = "High"
            # If there were adjustments, modest severity; else 0
            set_to_num = pd.to_numeric(df.loc[idx_all, "SetLocationQtyTo"], errors="coerce")
            sys_num = pd.to_numeric(df.loc[idx_all, "SystemQty"], errors="coerce").fillna(0.0)

            any_adjust = bool(((~set_to_num.isna()) & (set_to_num != sys_num)).any())

            df.loc[idx_all, "Severity"] = df.loc[idx_all, "Severity"].clip(lower=(50 if any_adjust else 0))

        # Group headline
        has_adjustments = bool(
            ((df.loc[idx_all, "SetLocationQtyTo"].notna()) & (df.loc[idx_all, "SetLocationQtyTo"].astype(float) != df.loc[idx_all, "SystemQty"].astype(float))).any()
        )

        # Net group variance is informational: Σ(set-to - system) excluding ST01 (ST01 is NA)
        non_st01 = df.loc[idx_all].copy()
        non_st01 = non_st01[non_st01["Location"] != "ST01"]
        net_group_variance = float(
            (pd.to_numeric(non_st01["SetLocationQtyTo"], errors="coerce").fillna(0.0) - non_st01["SystemQty"]).sum()
        )

        df.loc[idx_all, "GroupHeadline"] = _group_headline(investigate, has_adjustments, net_group_variance)

        # Group summary row
        group_rows.append(
            {
                "Whs": whs_summary,
                "Item": item,
                "Batch/lot": lot_summary,
                "DefaultLocation": default_loc,
                "SystemTotal": float(g["SystemQty"].sum()),
                "CountTotal": float(g["CountQty"].sum()),
                "NetVariance": float(g["VarianceQty"].sum()),
                "SysST01": float(st01_qty),
                "Scenario": scenario,
                "EligibleForST01Logic": "Yes" if eligible_for_st01_logic else "No",
                "UnbalancedSecondary": "Yes" if unbalanced_secondary else "No",
                "ResidualUnbalanced": float(residual_unbalanced),
                "NetSecondaryAdjustments": float(net_secondary_adjustments),
                "DefaultSetTo": float(default_set_to),
                "Investigate": "Yes" if investigate else "No",
                "RecommendationHeadline": df.loc[idx_all, "GroupHeadline"].iloc[0],
                "Guidance": " ".join(guidance) if guidance else "",
            }
        )

    transfers_df = pd.DataFrame()  # system does not emit transfer rows; only guidance text
    group_summary_df = pd.DataFrame(group_rows)

    return df, transfers_df, group_summary_df

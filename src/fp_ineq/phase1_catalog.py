from __future__ import annotations

from dataclasses import dataclass

__all__ = [
    "PHASE1_FAMILIES",
    "PHASE1_SCENARIOS",
    "Phase1FamilySpec",
    "Phase1ScenarioSpec",
    "phase1_family_by_id",
    "phase1_family_specs",
    "phase1_distribution_specs",
    "phase1_public_bundle_specs",
    "phase1_scenario_by_variant",
    "phase1_transfer_core_specs",
]


@dataclass(frozen=True)
class Phase1FamilySpec:
    family_id: str
    label: str
    summary: str
    maturity: str
    include_in_public_bundle: bool = True


@dataclass(frozen=True)
class Phase1ScenarioSpec:
    variant_id: str
    family_id: str
    ui_factor: float
    trgh_delta_q: float
    trsh_factor: float
    transfer_description: str
    distribution_description: str
    group: str
    label: str
    summary: str
    trfin_fed_share: float = 0.0
    trfin_sl_share: float = 0.0
    include_in_transfer_core: bool = True
    include_in_distribution_block: bool = True
    include_in_public_bundle: bool = True


PHASE1_FAMILIES = (
    Phase1FamilySpec(
        family_id="baseline",
        label="Baseline",
        summary="Observed integrated baseline path used as the public comparison anchor.",
        maturity="public",
    ),
    Phase1FamilySpec(
        family_id="ui",
        label="UI",
        summary="Unemployment-insurance shock probes and matched ladder runs on the stock UB channel.",
        maturity="public",
        include_in_public_bundle=False,
    ),
    Phase1FamilySpec(
        family_id="federal-transfers",
        label="Federal Transfers",
        summary="Broad federal household-transfer probes on the stock TRGH channel.",
        maturity="public",
        include_in_public_bundle=False,
    ),
    Phase1FamilySpec(
        family_id="state-local-transfers",
        label="State/Local Transfers",
        summary="State/local household-transfer probes on the stock TRSH channel.",
        maturity="public",
        include_in_public_bundle=False,
    ),
    Phase1FamilySpec(
        family_id="transfer-package",
        label="Transfer Package",
        summary="Combined relief and shock runs across the installed transfer channels.",
        maturity="public",
        include_in_public_bundle=False,
    ),
    Phase1FamilySpec(
        family_id="transfer-composite",
        label="Transfer Composite",
        summary="Matched-bin composite ladder runs across UI, federal, and state/local transfer channels.",
        maturity="public",
    ),
    Phase1FamilySpec(
        family_id="credit-effective-rates",
        label="Credit Effective Rates",
        summary="Private credit-family runs on a shared neutral baseline using the experimental effective-rate wedge patch.",
        maturity="private-experimental",
        include_in_public_bundle=False,
    ),
    Phase1FamilySpec(
        family_id="wealth-shadow",
        label="Wealth Shadow",
        summary="Private shadow wealth block built on endogenous FAIR bridges pending a public-family decision.",
        maturity="private-shadow",
        include_in_public_bundle=False,
    ),
)


PHASE1_SCENARIOS = (
    Phase1ScenarioSpec(
        variant_id="baseline-observed",
        family_id="baseline",
        ui_factor=1.00,
        trgh_delta_q=0.0,
        trsh_factor=1.00,
        transfer_description="Baseline integrated transfer-core prototype.",
        distribution_description="Baseline transfer-core scenario with integrated distribution identities.",
        group="Baseline",
        label="Baseline Observed",
        summary="Solved baseline path with integrated distribution outputs.",
    ),
    Phase1ScenarioSpec(
        variant_id="ui-relief",
        family_id="ui",
        ui_factor=1.02,
        trgh_delta_q=0.0,
        trsh_factor=1.00,
        transfer_description="Higher UI generosity through the stock UB channel.",
        distribution_description="Higher UI generosity with integrated distribution identities.",
        group="UI",
        label="UI Medium",
        summary="Solved matched UI ladder medium rung with integrated distribution outputs.",
    ),
    Phase1ScenarioSpec(
        variant_id="ui-shock",
        family_id="ui",
        ui_factor=0.98,
        trgh_delta_q=0.0,
        trsh_factor=1.00,
        transfer_description="Lower UI generosity through the stock UB channel.",
        distribution_description="Lower UI generosity with integrated distribution identities.",
        group="UI",
        label="UI Shock",
        summary="Solved UI transfer-channel probe with integrated distribution outputs.",
    ),
    Phase1ScenarioSpec(
        variant_id="ui-small",
        family_id="ui",
        ui_factor=1.0141888330491307,
        trgh_delta_q=0.0,
        trsh_factor=1.00,
        transfer_description="Smaller matched UI ladder rung through the stock UB channel.",
        distribution_description="Smaller matched UI ladder rung with integrated distribution identities.",
        group="UI",
        label="UI Small",
        summary="Solved matched UI ladder rung with integrated distribution outputs.",
        include_in_transfer_core=False,
    ),
    Phase1ScenarioSpec(
        variant_id="ui-medium",
        family_id="ui",
        ui_factor=1.02,
        trgh_delta_q=0.0,
        trsh_factor=1.00,
        transfer_description="Medium matched UI ladder rung through the stock UB channel.",
        distribution_description="Medium matched UI ladder rung with integrated distribution identities.",
        group="UI",
        label="UI Medium",
        summary="Solved matched UI ladder medium rung with integrated distribution outputs.",
        include_in_transfer_core=False,
        include_in_distribution_block=False,
        include_in_public_bundle=False,
    ),
    Phase1ScenarioSpec(
        variant_id="ui-large",
        family_id="ui",
        ui_factor=1.023625843049206,
        trgh_delta_q=0.0,
        trsh_factor=1.00,
        transfer_description="Larger matched UI ladder rung through the stock UB channel.",
        distribution_description="Larger matched UI ladder rung with integrated distribution identities.",
        group="UI",
        label="UI Large",
        summary="Solved matched UI ladder rung with integrated distribution outputs.",
        include_in_transfer_core=False,
    ),
    Phase1ScenarioSpec(
        variant_id="federal-transfer-relief",
        family_id="federal-transfers",
        ui_factor=1.00,
        trgh_delta_q=2.0,
        trsh_factor=1.00,
        transfer_description="Higher federal household transfers through the stock TRGH channel.",
        distribution_description="Higher federal household transfers with integrated distribution identities.",
        group="Federal Transfers",
        label="Federal Transfer Relief",
        summary="Solved broad federal household-transfer probe with integrated distribution outputs.",
    ),
    Phase1ScenarioSpec(
        variant_id="federal-transfer-shock",
        family_id="federal-transfers",
        ui_factor=1.00,
        trgh_delta_q=-2.0,
        trsh_factor=1.00,
        transfer_description="Lower federal household transfers through the stock TRGH channel.",
        distribution_description="Lower federal household transfers with integrated distribution identities.",
        group="Federal Transfers",
        label="Federal Transfer Shock",
        summary="Solved broad federal household-transfer probe with integrated distribution outputs.",
    ),
    Phase1ScenarioSpec(
        variant_id="state-local-transfer-relief",
        family_id="state-local-transfers",
        ui_factor=1.00,
        trgh_delta_q=0.0,
        trsh_factor=1.02,
        transfer_description="Higher state/local household transfers through the stock TRSH channel.",
        distribution_description="Higher state/local household transfers with integrated distribution identities.",
        group="State/Local Transfers",
        label="State Local Transfer Relief",
        summary="Solved state/local household-transfer probe with integrated distribution outputs.",
    ),
    Phase1ScenarioSpec(
        variant_id="state-local-transfer-shock",
        family_id="state-local-transfers",
        ui_factor=1.00,
        trgh_delta_q=0.0,
        trsh_factor=0.99,
        transfer_description="Lower state/local household transfers through the stock TRSH channel.",
        distribution_description="Lower state/local household transfers with integrated distribution identities.",
        group="State/Local Transfers",
        label="State Local Transfer Shock",
        summary="Solved state/local household-transfer probe with integrated distribution outputs.",
    ),
    Phase1ScenarioSpec(
        variant_id="transfer-package-relief",
        family_id="transfer-package",
        ui_factor=1.02,
        trgh_delta_q=2.0,
        trsh_factor=1.02,
        trfin_fed_share=0.0,
        trfin_sl_share=0.0,
        transfer_description="Combined transfer relief through the stock UB, TRGH, and TRSH channels.",
        distribution_description="Combined transfer relief with offline-estimated in-model distribution identities.",
        group="Transfer Package",
        label="Transfer Package Relief",
        summary="Solved combined transfer-channel probe with integrated distribution outputs.",
    ),
    Phase1ScenarioSpec(
        variant_id="transfer-package-shock",
        family_id="transfer-package",
        ui_factor=0.98,
        trgh_delta_q=-2.0,
        trsh_factor=0.99,
        transfer_description="Combined transfer shock through the stock UB, TRGH, and TRSH channels.",
        distribution_description="Combined transfer shock with offline-estimated in-model distribution identities.",
        group="Transfer Package",
        label="Transfer Package Shock",
        summary="Solved combined transfer-channel probe with integrated distribution outputs.",
    ),
    Phase1ScenarioSpec(
        variant_id="transfer-composite-small",
        family_id="transfer-composite",
        ui_factor=1.01,
        trgh_delta_q=1.0,
        trsh_factor=1.01,
        trfin_fed_share=1.0,
        trfin_sl_share=1.0,
        transfer_description="Small catalog transfer-composite package through UB, TRGH, and TRSH with full financing shares.",
        distribution_description="Small catalog transfer-composite package with integrated distribution identities.",
        group="Transfer Composite",
        label="Transfer Composite Small",
        summary="Catalog transfer-composite small package with deterministic levers and financing shares.",
        include_in_transfer_core=False,
    ),
    Phase1ScenarioSpec(
        variant_id="transfer-composite-medium",
        family_id="transfer-composite",
        ui_factor=1.02,
        trgh_delta_q=2.0,
        trsh_factor=1.02,
        trfin_fed_share=1.0,
        trfin_sl_share=1.0,
        transfer_description="Medium catalog transfer-composite package through UB, TRGH, and TRSH with full financing shares.",
        distribution_description="Medium catalog transfer-composite package with integrated distribution identities.",
        group="Transfer Composite",
        label="Transfer Composite Medium",
        summary="Catalog transfer-composite medium package with deterministic levers and financing shares.",
        include_in_transfer_core=False,
    ),
    Phase1ScenarioSpec(
        variant_id="transfer-composite-large",
        family_id="transfer-composite",
        ui_factor=1.03,
        trgh_delta_q=3.0,
        trsh_factor=1.03,
        trfin_fed_share=1.0,
        trfin_sl_share=1.0,
        transfer_description="Large catalog transfer-composite package through UB, TRGH, and TRSH with full financing shares.",
        distribution_description="Large catalog transfer-composite package with integrated distribution identities.",
        group="Transfer Composite",
        label="Transfer Composite Large",
        summary="Catalog transfer-composite large package with deterministic levers and financing shares.",
        include_in_transfer_core=False,
    ),
)


def phase1_family_specs() -> list[Phase1FamilySpec]:
    return list(PHASE1_FAMILIES)


def phase1_family_by_id() -> dict[str, Phase1FamilySpec]:
    return {spec.family_id: spec for spec in PHASE1_FAMILIES}


def phase1_transfer_core_specs() -> list[Phase1ScenarioSpec]:
    return [spec for spec in PHASE1_SCENARIOS if spec.include_in_transfer_core]


def phase1_distribution_specs() -> list[Phase1ScenarioSpec]:
    return [spec for spec in PHASE1_SCENARIOS if spec.include_in_distribution_block]


def phase1_public_bundle_specs(
    *,
    family_maturities: tuple[str, ...] = ("public",),
    family_ids: tuple[str, ...] | None = None,
) -> list[Phase1ScenarioSpec]:
    allowed_maturities = {str(item) for item in family_maturities}
    allowed_families = {str(item) for item in family_ids} if family_ids is not None else None
    families = phase1_family_by_id()
    public_specs: list[Phase1ScenarioSpec] = []
    for spec in PHASE1_SCENARIOS:
        if not spec.include_in_public_bundle:
            continue
        family = families[spec.family_id]
        if not family.include_in_public_bundle:
            continue
        if family.maturity not in allowed_maturities:
            continue
        if allowed_families is not None and spec.family_id not in allowed_families:
            continue
        public_specs.append(spec)
    return public_specs


def phase1_scenario_by_variant() -> dict[str, Phase1ScenarioSpec]:
    return {spec.variant_id: spec for spec in PHASE1_SCENARIOS}

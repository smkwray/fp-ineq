from __future__ import annotations

__all__ = [
    "PUBLIC_TO_RUNTIME",
    "RUNTIME_TO_PUBLIC",
    "to_public_name",
    "to_runtime_name",
]


PUBLIC_TO_RUNTIME = {
    "IPOVALL": "IPOVALL",
    "IPOVCH": "IPOVCH",
    "IGINIHH": "IGINIHH",
    "IMEDRINC": "IMEDRINC",
    "IWGAP1050": "IWG1050",
    "IWGAP150": "IWGAP150",
    "ITRCOMP": "ITRCOMP",
    "IUIBEN": "IUIBEN",
    "ISSBEN": "ISSBEN",
    "ISNAP": "ISNAP",
    "ICRDCMP": "ICRDCMP",
    "IHHNW": "IHHNW",
    "IHOMEQ": "IHOMEQ",
    "IFFUNDS": "IFFUNDS",
    "UR": "UR",
    "GDPR": "GDPR",
    "PCY": "PCY",
    "PIEF": "PIEF",
}

RUNTIME_TO_PUBLIC = {runtime: public for public, runtime in PUBLIC_TO_RUNTIME.items()}


def to_runtime_name(name: str) -> str:
    return PUBLIC_TO_RUNTIME.get(name, name)


def to_public_name(name: str) -> str:
    return RUNTIME_TO_PUBLIC.get(name, name)

from __future__ import annotations

import tomllib
from dataclasses import dataclass, field, fields
from pathlib import Path


@dataclass
class CsvConfig:
    tab_name:               str = "products"
    sku_column:             str = "sku"
    name_column:            str = "name"
    supplier_column:        str = "supplier"
    parent_product_column:  str = "parent product"


@dataclass
class DriveConfig:
    # "supplier" → root/supplier/sku  |  "flat" → root/sku
    structure: str = "supplier"


@dataclass
class DiagnoseConfig:
    report_tab: str = "Diagnose Report"


@dataclass
class LifestyleConfig:
    report_tab: str = "Lifestyle Rename"


@dataclass
class ModelsConfig:
    report_tab: str = "Models Report"


@dataclass
class InputPaths:
    product_photos:        str = "product_photos"
    lifestyle_photos:      str = "lifestyle_photos"
    thumbnails_website:    str = "thumbnails/website_thumbnail"
    thumbnails_system:     str = "thumbnails/system_thumbnail"
    videos:                str = "videos"
    diagram:               str = "diagram"
    models_dwg:            str = "models/dwg"
    models_obj:            str = "models/obj"
    models_gltf:           str = "models/gltf"
    models_skp:            str = "models/skp"
    assembly_instructions: str = "assembly_instructions"
    carton_layout:         str = "carton_layout"
    barcode:               str = "barcode"

    _DISPLAY: dict[str, str] = field(default_factory=lambda: {
        "product_photos":        "Product Photos",
        "lifestyle_photos":      "Lifestyle Photos",
        "thumbnails_website":    "Thumbnails / Website",
        "thumbnails_system":     "Thumbnails / System",
        "videos":                "Videos",
        "diagram":               "Diagram",
        "models_dwg":            "Models / DWG",
        "models_obj":            "Models / OBJ",
        "models_gltf":           "Models / GLTF",
        "models_skp":            "Models / SKP",
        "assembly_instructions": "Assembly Instructions",
        "carton_layout":         "Carton Layout",
        "barcode":               "Barcode",
    }, init=False, repr=False, compare=False)

    def entries(self) -> list[tuple[str, str, str]]:
        """Return (key, display_name, relative_path) for every tracked directory."""
        return [
            (f.name, self._DISPLAY[f.name], getattr(self, f.name))
            for f in fields(self)
            if not f.name.startswith("_")
        ]


@dataclass
class PipelineConfig:
    csv:       CsvConfig       = field(default_factory=CsvConfig)
    drive:     DriveConfig     = field(default_factory=DriveConfig)
    diagnose:  DiagnoseConfig  = field(default_factory=DiagnoseConfig)
    lifestyle: LifestyleConfig = field(default_factory=LifestyleConfig)
    models:    ModelsConfig    = field(default_factory=ModelsConfig)
    paths:     InputPaths      = field(default_factory=InputPaths)

    @classmethod
    def load(cls, path: Path) -> "PipelineConfig":
        with open(path, "rb") as fh:
            raw = tomllib.load(fh)

        def _build(dc, section: dict):
            known = {f.name for f in fields(dc)}
            return dc(**{k: v for k, v in section.items() if k in known})

        return cls(
            csv=_build(CsvConfig, raw.get("csv", {})),
            drive=_build(DriveConfig, raw.get("drive", {})),
            diagnose=_build(DiagnoseConfig, raw.get("diagnose", {})),
            lifestyle=_build(LifestyleConfig, raw.get("lifestyle", {})),
            models=_build(ModelsConfig, raw.get("models", {})),
            paths=_build(InputPaths, raw.get("paths", {}).get("input", {})),
        )

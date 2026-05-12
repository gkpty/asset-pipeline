from __future__ import annotations

import tomllib
from dataclasses import dataclass, field, fields
from pathlib import Path
from typing import Optional


@dataclass
class CsvConfig:
    tab_name:               str = "products"
    sku_column:             str = "sku"
    name_column:            str = "name"
    supplier_column:        str = "supplier"
    parent_product_column:  str = "parent product"
    supplier_ref_column:    str = "supplier ref"
    barcode_column:         str = "barcode"
    # For modular products, two SKUs can share a parent_product but be physically
    # different parts (e.g. left armrest vs corner piece) — siblings only count
    # for asset reuse when this column also matches.
    part_column:            str = "part"
    # Photos can be duplicated verbatim across SKUs that share all materials but
    # differ ONLY in size (queen→king, small pot→large pot). The COPY action in
    # `asset generate --type photos` requires sibling.size != target.size.
    size_column:            str = "size"


@dataclass
class DriveConfig:
    # "supplier" → root/supplier/sku  |  "flat" → root/sku
    structure: str = "supplier"


@dataclass
class CategoryConfig:
    # Per-category override. Currently just `structure`; future: paths/diagnostics overrides.
    structure: Optional[str] = None


@dataclass
class DiagnoseConfig:
    report_tab: str = "Diagnose Report"


@dataclass
class LifestyleConfig:
    report_tab: str = "Lifestyle Rename"


@dataclass
class ModelsConfig:
    report_tab: str = "Models Report"


_DEFAULT_MATERIAL_COLUMNS: dict[str, str] = {
    "material":   "materials",
    "color":      "materials",
    "top":        "materials",
    "panel":      "materials",
    "seat":       "materials",
    "legs":       "materials",
    "trim":       "materials",
    "weaving":    "materials",
    "upholstery": "upholstery",
}


_QUALITY_COST_USD = {"low": 0.011, "medium": 0.063, "high": 0.167}

# Per-model USD-per-image defaults. Used by the budget pre-flight to give
# accurate cost estimates when the model cycle mixes providers with different
# pricing. Override per-deployment in TOML under [generate.photos.model_costs].
_DEFAULT_MODEL_COSTS: dict[str, float] = {
    # Google Gemini Flash Image (≈$0.039/img per Gemini's published rate).
    "gemini-3.1-flash-image-preview":  0.039,
    "gemini-2.5-flash-image-preview":  0.039,
    "gemini-2.5-flash-image":          0.039,
    # OpenAI gpt-image-1 — quality-tiered. The dispatch falls back to the
    # quality-derived cost_per_image_usd for OpenAI models not listed here,
    # so we don't need a separate entry per quality tier.
    "gpt-image-1":                     0.167,  # high quality estimate
    "gpt-image-2-2026-04-21":          0.167,  # estimate; update when verified
}


# Public read-only handles for the CLI's --models help text. These stay in
# sync with the cost maps automatically — add a model + cost above and it
# shows up in --help.
KNOWN_PHOTO_MODELS: tuple[str, ...] = tuple(_DEFAULT_MODEL_COSTS.keys())


@dataclass
class PhotosGenerateConfig:
    # Models tried in order, cycling on retry. attempt 1 → models[0]; retry 1 →
    # models[1]; retry 2 → models[0] (wraps). Provider is detected from the
    # model name prefix (gpt-* / dall-* → OpenAI, gemini-* → Google).
    models:             list[str] = field(default_factory=lambda: [
        "gpt-image-1",
        "gemini-3.1-flash-image-preview",
    ])
    quality:            str   = "high"        # low | medium | high (OpenAI only; Gemini ignores)
    size:               str   = "auto"        # "auto" → match source aspect; or 1024x1024 / 1536x1024 / 1024x1536
    # cost_per_image_usd is the fallback per-image cost for any model not in
    # `model_costs`. Auto-derived from `quality` if left at 0 (OpenAI tier
    # pricing). model_costs overrides this on a per-model basis — set
    # gemini-* entries there, leave gpt-image-1 to the quality-derived fallback.
    cost_per_image_usd: float = 0.0
    model_costs:        dict[str, float] = field(default_factory=lambda: dict(_DEFAULT_MODEL_COSTS))
    # Verifier loop: after each generate call, ask Claude to QA the output.
    # If verification fails, retry up to max_retries with the next model in the
    # cycle (and the verifier's retry_instructions appended to the prompt).
    # Disable per-run with --no-verify.
    verify_enabled:     bool  = True
    verify_model:       str   = "claude-sonnet-4-6"
    max_retries:        int   = 2
    # Map of master-sheet material column -> drive category subfolder under the parent root.
    material_columns:   dict[str, str] = field(default_factory=lambda: dict(_DEFAULT_MATERIAL_COLUMNS))

    def __post_init__(self) -> None:
        if not self.cost_per_image_usd:
            self.cost_per_image_usd = _QUALITY_COST_USD.get(self.quality.strip().lower(), 0.063)

    def cost_for(self, model: str) -> float:
        """USD per image for a given model. Falls back to cost_per_image_usd
        (auto-derived from quality) for any model not in model_costs."""
        m = model.strip()
        if m in self.model_costs and self.model_costs[m]:
            return float(self.model_costs[m])
        # Case-insensitive match as a courtesy for sloppy TOML.
        ml = m.lower()
        for k, v in self.model_costs.items():
            if k.lower() == ml and v:
                return float(v)
        return float(self.cost_per_image_usd)

    def worst_case_cost_per_image(self, models: list[str], max_retries: int) -> float:
        """Sum of per-image costs across all attempts (1 + max_retries),
        cycling through `models`. Used for the budget pre-flight ceiling."""
        if not models:
            return 0.0
        attempts = 1 + max(0, max_retries)
        return sum(self.cost_for(models[i % len(models)]) for i in range(attempts))


# Background-neutral motion + geometry block. The scene (white studio vs.
# --add-background scene) is handled separately by build_prompt's Section 1,
# so this block must NOT mention "white background" / "studio" / similar.
_DEFAULT_VIDEO_PROMPT = (
    "The camera makes a slow, smooth push-in (subtle dolly forward) with "
    "a very gentle frame shift, ending slightly tighter on the subject. "
    "The camera is mounted on a fully stabilized professional tripod or "
    "motorized gimbal — every frame is rock-steady, mechanically smooth, "
    "with absolutely no handheld feel, no micro-jitter, no wobble, no "
    "shake, no float, no breathing of the frame. The motion is deliberate "
    "and locked, as if shot on a robotic motion-control rig. The product "
    "itself is completely static: it does not rotate, tilt, lift, deform, "
    "or change pose in any way; only the camera moves. Every part of the "
    "product — its geometry, topology, part count, silhouette, materials, "
    "and surface details — is identical in every single frame. Nothing is "
    "added, duplicated, split, removed, or hallucinated as the camera "
    "moves; legs, panels, rails, and any other components stay exactly "
    "the same in count and shape. Photorealistic, soft natural lighting. "
    "No text, watermarks, logos, captions, or overlays."
)


_DEFAULT_VIDEO_MODEL_COSTS: dict[str, float] = {
    # Replicate pricing snapshots — update when their list changes.
    "bytedance/seedance-2.0":         0.50,    # estimate; verify after first run
    "bytedance/seedance-2.0-fast":    0.25,    # estimate; verify after first run
    "bytedance/seedance-1-pro":       0.30,
    "bytedance/seedance-1-pro-fast":  0.15,
    "bytedance/seedance-1-lite":      0.15,
    "kwaivgi/kling-v2.5-turbo-pro":   0.50,
    "kwaivgi/kling-v2.1":             0.50,
    "google/veo-3":                   3.00,
    "google/veo-3.1":                 3.00,
    "runwayml/gen4-turbo":            1.25,
}


KNOWN_VIDEO_MODELS: tuple[str, ...] = tuple(_DEFAULT_VIDEO_MODEL_COSTS.keys())


@dataclass
class VideoGenerateConfig:
    # Replicate model slugs tried in order, cycling on retry. Provider is
    # always Replicate for now (gpt-* and gemini-* don't ship image-to-video
    # yet in our setup).
    models:              list[str] = field(default_factory=lambda: ["bytedance/seedance-2.0"])
    duration_seconds:    int       = 6           # plus title+logo bookends → 8s typical final
    resolution:          str       = "720p"     # 480p | 720p | 1080p (model-dependent)
    # 9:16 by default — vertical, ready for IG Reels / YouTube Shorts.
    # 16:9 / 4:3 / 1:1 / 3:4 / 9:16 / 21:9 / 9:21 are valid Seedance values.
    # Note: Seedance 1.x ignores this field when an image is supplied, so
    # the stage pre-letterboxes each keyframe to this ratio as a safety net
    # (harmless on 2.0, which honors aspect_ratio with image inputs).
    aspect_ratio:        str       = "9:16"
    # Lock the camera so Seedance only animates the subject (no dolly /
    # pan / tilt / zoom). Honored by Seedance 1.x only — 2.0 dropped this
    # field, so we lean on the default prompt to enforce a static camera.
    camera_fixed:        bool      = True
    # Use the first AND last white-bg photo as start + end keyframes (via
    # Seedance's `last_frame_image`). Disable for single-frame mode where
    # the model interpolates motion from one keyframe + the prompt — this
    # is the default because it gives the prompt full control over motion.
    # Two keyframes at different angles FORCE the model into a rotation
    # arc regardless of prompt, which historically caused "extra leg" type
    # geometry hallucinations.
    use_last_frame:      bool      = False
    # When --add-background is set, the stage switches to reference-images
    # mode (Seedance 2.0+) and uses up to this many white-bg photos as
    # identity / style references. Seedance caps at 9.
    max_reference_images: int      = 9
    # Used by `--add-background` (no value) and `--add-audio` (no value).
    # `--background "..."` / `--audio "..."` override these per-run.
    default_background:  str       = "a nice minimalist villa"
    default_audio:       str       = "smooth jazz"
    default_prompt:      str       = _DEFAULT_VIDEO_PROMPT

    # --- Title + logo bookend overlays. -----------------------------------
    # Enabled per-run via `--add-title-card` / `--add-logo-card`. The final
    # video = title_intro (title_card_seconds) + seedance (duration_seconds)
    # + logo_outro (logo_card_seconds). The intro is the seedance's FIRST
    # frame frozen with the title text overlaid (fades out). The outro is
    # the seedance's LAST frame frozen with the logo overlaid (fades in).
    title_card_seconds:  float     = 1.0
    logo_card_seconds:   float     = 1.0
    # Empty path → auto-detect from a small list of system font candidates
    # (Helvetica on macOS, DejaVu on Linux, Arial on Windows). Point this at
    # a custom .ttf / .otf for branded typography.
    title_font_path:     str       = ""
    # Text color (hex). A subtle white outline is added automatically for
    # legibility against varied backgrounds.
    card_text_color:     str       = "#111111"
    # Google Drive file ID of the company logo. Used by --add-logo-card.
    # Override per-run with --logo-id. Empty → cards disabled.
    logo_drive_id:       str       = ""
    # Replicate API token is read from REPLICATE_API_TOKEN env var.
    max_retries:         int       = 1          # video gen is slow + expensive; retry sparingly
    cost_per_video_usd:  float     = 0.0        # 0 → auto-fill from model_costs[models[0]]
    model_costs:         dict[str, float] = field(default_factory=lambda: dict(_DEFAULT_VIDEO_MODEL_COSTS))

    def __post_init__(self) -> None:
        if not self.cost_per_video_usd and self.models:
            self.cost_per_video_usd = self.model_costs.get(self.models[0], 0.30)

    def cost_for(self, model: str) -> float:
        m = model.strip()
        if m in self.model_costs and self.model_costs[m]:
            return float(self.model_costs[m])
        return float(self.cost_per_video_usd)

    def worst_case_cost_per_video(self, models: list[str], max_retries: int) -> float:
        if not models:
            return 0.0
        attempts = 1 + max(0, max_retries)
        return sum(self.cost_for(models[i % len(models)]) for i in range(attempts))


@dataclass
class GenerateConfig:
    report_tab: str = "Generate Report"
    photos:     PhotosGenerateConfig = field(default_factory=PhotosGenerateConfig)
    video:      VideoGenerateConfig  = field(default_factory=VideoGenerateConfig)


@dataclass
class ScaffoldConfig:
    report_tab:         str = "Scaffold Report"
    # Fallback folder name used by `scaffold --clean --move` when GOOGLE_DRIVE_MOVED_FOLDER_ID
    # is not set in the environment. Created/found under the products root.
    moved_folder_name:  str = "MOVED_FOLDER"
    typo_cutoff:        float = 0.65


@dataclass
class OptimizeConfig:
    # Photo settings
    target_size:           int   = 2000
    target_padding_pct:    float = 8.0
    white_threshold:       int   = 245
    jpg_quality:           int   = 85
    max_file_mb:           float = 2.0
    output_subdir_suffix:  str   = "-optimized"
    report_tab:            str   = "Optimize Report"

    # Model settings
    model_dest_subdir:        str   = "models_optimized"
    model_target_texture_px:  int   = 1024
    model_decim_target_fine:  float = 0.25   # retention for finely-tessellated meshes
    model_decim_target_med:   float = 0.55   # retention for moderate density
    model_decim_target_coarse: float = 0.85  # retention for already-coarse meshes
    model_decim_max_stretch:  float = 0.10   # reject if longest output edge > this × bbox diag
    model_unit_name:          str   = "millimeter"
    model_unit_meter:         float = 0.001
    model_up_axis:            str   = "Z_UP"


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
    csv:        CsvConfig                  = field(default_factory=CsvConfig)
    drive:      DriveConfig                = field(default_factory=DriveConfig)
    diagnose:   DiagnoseConfig             = field(default_factory=DiagnoseConfig)
    lifestyle:  LifestyleConfig            = field(default_factory=LifestyleConfig)
    models:     ModelsConfig               = field(default_factory=ModelsConfig)
    scaffold:   ScaffoldConfig             = field(default_factory=ScaffoldConfig)
    optimize:   OptimizeConfig             = field(default_factory=OptimizeConfig)
    generate:   GenerateConfig             = field(default_factory=GenerateConfig)
    paths:      InputPaths                 = field(default_factory=InputPaths)
    categories: dict[str, CategoryConfig]  = field(default_factory=dict)

    def structure_for(self, category: str) -> str:
        """Return the Drive structure override for `category`, falling back to drive.structure."""
        cat = self.categories.get(category.strip().lower())
        if cat and cat.structure:
            return cat.structure
        return self.drive.structure

    @classmethod
    def load(cls, path: Path) -> "PipelineConfig":
        with open(path, "rb") as fh:
            raw = tomllib.load(fh)

        def _build(dc, section: dict):
            known = {f.name for f in fields(dc)}
            return dc(**{k: v for k, v in section.items() if k in known})

        categories: dict[str, CategoryConfig] = {}
        for name, section in (raw.get("categories") or {}).items():
            categories[name.strip().lower()] = _build(CategoryConfig, section)

        # Nested photos config under [generate.photos] (and optional
        # [generate.photos.material_columns] override map).
        gen_raw = raw.get("generate", {}) or {}
        photos_raw = gen_raw.get("photos", {}) or {}
        material_columns_override = photos_raw.get("material_columns")
        model_costs_override = photos_raw.get("model_costs")
        # _build strips unknown keys; pull dict-valued sections into the
        # dataclass manually below. `inpaint` is silently ignored if present
        # in older configs (we removed the inpaint flow in favor of multi-image
        # gpt-image / gemini edits).
        photos_section = {
            k: v for k, v in photos_raw.items()
            if k not in ("material_columns", "model_costs", "inpaint")
        }
        # Back-compat: pre-multi-provider configs used `model = "..."`. Migrate
        # to a single-element `models = ["..."]` if the new key isn't set.
        if "model" in photos_section and "models" not in photos_section:
            photos_section["models"] = [photos_section.pop("model")]
        else:
            photos_section.pop("model", None)  # drop ignored legacy key
        photos_cfg = _build(PhotosGenerateConfig, photos_section)
        if isinstance(material_columns_override, dict):
            photos_cfg.material_columns = {
                str(k).strip().lower(): str(v).strip().lower()
                for k, v in material_columns_override.items()
            }
        if isinstance(model_costs_override, dict):
            # Merge user overrides on top of defaults so partial overrides work.
            for k, v in model_costs_override.items():
                photos_cfg.model_costs[str(k).strip()] = float(v)
        # Nested video config under [generate.video] (with optional
        # [generate.video.model_costs] override map).
        video_raw = gen_raw.get("video", {}) or {}
        video_model_costs_override = video_raw.get("model_costs")
        video_section = {
            k: v for k, v in video_raw.items() if k != "model_costs"
        }
        video_cfg = _build(VideoGenerateConfig, video_section)
        if isinstance(video_model_costs_override, dict):
            for k, v in video_model_costs_override.items():
                video_cfg.model_costs[str(k).strip()] = float(v)
        # Re-run __post_init__ logic since _build skips it when fields are
        # assigned after construction.
        if not video_cfg.cost_per_video_usd and video_cfg.models:
            video_cfg.cost_per_video_usd = video_cfg.model_costs.get(
                video_cfg.models[0], 0.30,
            )

        # Build top-level GenerateConfig with the nested photos + video.
        gen_top = {k: v for k, v in gen_raw.items() if k not in ("photos", "video")}
        generate_cfg = _build(GenerateConfig, gen_top)
        generate_cfg.photos = photos_cfg
        generate_cfg.video = video_cfg

        return cls(
            csv=_build(CsvConfig, raw.get("csv", {})),
            drive=_build(DriveConfig, raw.get("drive", {})),
            diagnose=_build(DiagnoseConfig, raw.get("diagnose", {})),
            lifestyle=_build(LifestyleConfig, raw.get("lifestyle", {})),
            models=_build(ModelsConfig, raw.get("models", {})),
            scaffold=_build(ScaffoldConfig, raw.get("scaffold", {})),
            optimize=_build(OptimizeConfig, raw.get("optimize", {})),
            generate=generate_cfg,
            paths=_build(InputPaths, raw.get("paths", {}).get("input", {})),
            categories=categories,
        )

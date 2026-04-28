"""3D model optimization pipeline.

Five-stage pipeline ported from `3d-model-optimization/sofa_optimized_bundle/`:

  1. sanitize  — rename materials (strip parens/spaces) and dedupe textures by md5
  2. split     — break the master OBJ into one sub-OBJ per material group
  3. decimate  — adaptive per-mesh quadric edge-collapse with bridging-triangle sanity check
  4. merge     — concat decimated sub-OBJs back into a single OBJ + MTL
  5. export    — Collada (.dae) for SketchUp + GLB for web/three.js

The dry-run path is light: it only downloads the MTL + texture metadata so it can
flag duplicate textures, oversized textures, and unused textures without paying
for an OBJ download. Pass --execute to run the full pipeline.
"""
from __future__ import annotations

import hashlib
import os
import shutil
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Generator, NamedTuple

from asset_sdk.adapters import drive
from asset_sdk.config import OptimizeConfig

_TEXTURE_EXTS = {".jpg", ".jpeg", ".png", ".tif", ".tiff", ".bmp", ".webp"}


# ---------------------------------------------------------------------------
# Targets
# ---------------------------------------------------------------------------

@dataclass
class ModelTarget:
    sku: str
    supplier: str
    sku_folder_id: str
    src_folder_id: str
    src_subdir: str           # e.g. "models/obj"
    dest_subdir: str          # e.g. "models_optimized"
    obj_file: dict            # {id, name, size}
    mtl_files: list[dict]
    texture_files: list[dict]
    other_files: list[dict]


def _resolve_subfolder(parent_id: str, rel_path: str) -> str | None:
    current = parent_id
    for part in rel_path.split("/"):
        children = drive.list_folders(current)
        if part not in children:
            return None
        current = children[part]
    return current


def find_targets(
    root_folder_id: str,
    structure: str,
    src_subdir: str,
    dest_subdir: str,
    sku_filter: str | None = None,
    supplier_filter: str | None = None,
) -> list[ModelTarget]:
    targets: list[ModelTarget] = []

    def _process(sku: str, sku_id: str, sup: str) -> None:
        if sku_filter and sku != sku_filter:
            return
        src_id = _resolve_subfolder(sku_id, src_subdir)
        if not src_id:
            return
        items = drive.list_children_meta(src_id)
        files = [i for i in items if i["kind"] == "file"]
        objs = [f for f in files if f["name"].lower().endswith(".obj")]
        if not objs:
            return
        mtls = [f for f in files if f["name"].lower().endswith(".mtl")]
        texs = [f for f in files if Path(f["name"]).suffix.lower() in _TEXTURE_EXTS]
        others = [
            f for f in files
            if not f["name"].lower().endswith((".obj", ".mtl"))
            and Path(f["name"]).suffix.lower() not in _TEXTURE_EXTS
        ]
        # Pick the largest OBJ (handles cases where there are stray small ones).
        obj = max(objs, key=lambda f: int(f.get("size") or 0))
        targets.append(ModelTarget(
            sku=sku, supplier=sup,
            sku_folder_id=sku_id, src_folder_id=src_id,
            src_subdir=src_subdir, dest_subdir=dest_subdir,
            obj_file=obj, mtl_files=mtls, texture_files=texs, other_files=others,
        ))

    if structure == "flat":
        for sku, sid in drive.list_folders(root_folder_id).items():
            _process(sku, sid, "")
    else:
        for sup_name, sup_id in drive.list_folders(root_folder_id).items():
            if supplier_filter and sup_name.lower() != supplier_filter.lower():
                continue
            for sku, sid in drive.list_folders(sup_id).items():
                _process(sku, sid, sup_name)

    return targets


# ---------------------------------------------------------------------------
# MTL parsing
# ---------------------------------------------------------------------------

def parse_mtl(mtl_path: str) -> tuple[list[str], dict[str, str]]:
    """Return (materials, {material: texture_filename}) for a Wavefront MTL file."""
    materials: list[str] = []
    tex_refs: dict[str, str] = {}
    current: str | None = None
    with open(mtl_path, "r", errors="replace") as f:
        for line in f:
            line = line.strip()
            if line.startswith("newmtl "):
                current = line[7:].strip()
                materials.append(current)
            elif line.startswith("map_Kd ") and current:
                tex_refs[current] = os.path.basename(line[7:].strip())
    return materials, tex_refs


# ---------------------------------------------------------------------------
# Analysis (dry run)
# ---------------------------------------------------------------------------

@dataclass
class ModelAnalysis:
    sku: str
    supplier: str
    obj_file_name: str
    obj_size_mb: float
    materials: list[str]
    texture_refs: dict[str, str]
    texture_count: int
    total_texture_mb: float
    duplicate_groups: list[list[str]] = field(default_factory=list)
    oversized: list[tuple[str, int, int]] = field(default_factory=list)
    unused_textures: list[str] = field(default_factory=list)
    actions: list[str] = field(default_factory=list)


def _human_mb(b: int) -> str:
    return f"{b / 1024 / 1024:.1f}MB"


def analyze(
    targets: list[ModelTarget],
    cfg: OptimizeConfig,
) -> Generator[tuple[ModelTarget, "ModelAnalysis | None", str | None], None, None]:
    """Light-weight analysis: only MTL is downloaded, texture stats come from Drive metadata."""
    target_tex_px = cfg.model_target_texture_px
    for t in targets:
        try:
            yield t, _analyze_one(t, target_tex_px), None
        except Exception as exc:
            yield t, None, str(exc)


def _analyze_one(t: ModelTarget, target_tex_px: int) -> ModelAnalysis:
    obj_size = int(t.obj_file.get("size") or 0)

    # Parse the (small) MTL for material names + texture references.
    materials: list[str] = []
    tex_refs: dict[str, str] = {}
    with tempfile.TemporaryDirectory() as tmp:
        for mtl in t.mtl_files:
            mtl_path = os.path.join(tmp, mtl["name"])
            drive.download_file(mtl["id"], mtl_path)
            mats, refs = parse_mtl(mtl_path)
            materials.extend(mats)
            tex_refs.update(refs)

    # Texture stats from Drive metadata (already fetched in find_targets via list_children_meta).
    total_tex_bytes = sum(int(f.get("size") or 0) for f in t.texture_files)

    # Duplicates by md5
    by_md5: dict[str, list[str]] = {}
    for f in t.texture_files:
        md5 = f.get("md5")
        if md5:
            by_md5.setdefault(md5, []).append(f["name"])
    duplicates = [sorted(names) for names in by_md5.values() if len(names) > 1]

    # Oversized
    oversized: list[tuple[str, int, int]] = []
    for f in t.texture_files:
        w, h = f.get("width"), f.get("height")
        if w and h and (w > target_tex_px or h > target_tex_px):
            oversized.append((f["name"], int(w), int(h)))

    referenced = set(tex_refs.values())
    unused = sorted(f["name"] for f in t.texture_files if f["name"] not in referenced)

    actions: list[str] = []
    if obj_size > 5 * 1024 * 1024:
        actions.append(f"decimate (OBJ {_human_mb(obj_size)})")
    if duplicates:
        actions.append(f"dedupe textures ({len(duplicates)} groups)")
    if oversized:
        actions.append(f"resize {len(oversized)} textures → ≤{target_tex_px}px")
    if unused:
        actions.append(f"drop {len(unused)} unused textures")
    actions.append("export GLB")
    actions.append("export DAE")

    return ModelAnalysis(
        sku=t.sku, supplier=t.supplier,
        obj_file_name=t.obj_file["name"],
        obj_size_mb=obj_size / 1024 / 1024,
        materials=materials, texture_refs=tex_refs,
        texture_count=len(t.texture_files),
        total_texture_mb=total_tex_bytes / 1024 / 1024,
        duplicate_groups=duplicates, oversized=oversized, unused_textures=unused,
        actions=actions,
    )


def to_sheet_rows(analyses: list[ModelAnalysis]) -> tuple[list[str], list[list]]:
    headers = [
        "SKU", "Supplier", "OBJ File", "OBJ Size",
        "Materials", "Textures", "Texture Size",
        "Duplicates", "Oversized", "Unused", "Actions",
    ]
    rows: list[list] = []
    for a in analyses:
        rows.append([
            a.sku,
            a.supplier,
            a.obj_file_name,
            f"{a.obj_size_mb:.1f}MB",
            f"{len(a.materials)}",
            f"{a.texture_count}",
            f"{a.total_texture_mb:.1f}MB",
            "; ".join("=".join(g) for g in a.duplicate_groups) or "—",
            "; ".join(f"{n} ({w}×{h})" for n, w, h in a.oversized) or "—",
            ", ".join(a.unused_textures) or "—",
            "; ".join(a.actions) if a.actions else "—",
        ])
    return headers, rows


# ---------------------------------------------------------------------------
# Stage helpers — sanitize / split / decimate / merge
# ---------------------------------------------------------------------------

def _sanitize_name(s: str) -> str:
    out = s.replace(" ", "_").replace("(", "").replace(")", "").replace("/", "_")
    return "".join(c for c in out if c.isalnum() or c in "_-")


def _stage_sanitize(
    work: str, obj_path: str, mtl_path: str | None, texture_paths: list[str],
) -> tuple[str, str, dict, dict, set]:
    """Rename materials, dedupe textures by md5.

    Returns: (sanitized_obj, sanitized_mtl, mat_rename, tex_rename, canonical_textures)
      mat_rename: {old_mat: new_mat}
      tex_rename: {old_tex_filename: canonical_filename}
    """
    out_dir = os.path.join(work, "sanitized")
    os.makedirs(out_dir, exist_ok=True)

    # Material rename map (built from MTL or OBJ usemtl directives).
    mat_rename: dict[str, str] = {}

    if mtl_path:
        with open(mtl_path, "r", errors="replace") as f:
            for line in f:
                if line.startswith("newmtl "):
                    old = line[7:].strip()
                    mat_rename[old] = _sanitize_name(old) or "mat"

    # Also collect any usemtl referenced in OBJ that wasn't in the MTL.
    with open(obj_path, "r", errors="replace") as f:
        for line in f:
            if line.startswith("usemtl "):
                old = line[7:].strip()
                if old not in mat_rename:
                    mat_rename[old] = _sanitize_name(old) or "mat"

    # Dedupe textures by md5.
    tex_rename: dict[str, str] = {}
    canonical_by_md5: dict[str, str] = {}
    canonical_textures: set[str] = set()
    for path in texture_paths:
        with open(path, "rb") as f:
            md5 = hashlib.md5(f.read()).hexdigest()
        name = os.path.basename(path)
        if md5 not in canonical_by_md5:
            canonical_by_md5[md5] = name
            canonical_textures.add(name)
        tex_rename[name] = canonical_by_md5[md5]

    # Write sanitized MTL.
    sanitized_mtl_name = "model.mtl"
    sanitized_mtl_path = os.path.join(out_dir, sanitized_mtl_name)
    if mtl_path:
        with open(mtl_path, "r", errors="replace") as fin, open(
            sanitized_mtl_path, "w", newline="\n"
        ) as fout:
            for line in fin:
                if line.startswith("newmtl "):
                    old = line[7:].strip()
                    fout.write(f"newmtl {mat_rename.get(old, _sanitize_name(old))}\n")
                elif line.startswith("map_Kd "):
                    old_tex = os.path.basename(line[7:].strip())
                    fout.write(f"map_Kd {tex_rename.get(old_tex, old_tex)}\n")
                else:
                    fout.write(line)
    else:
        # Synthesise a minimal MTL.
        with open(sanitized_mtl_path, "w", newline="\n") as fout:
            for new_name in sorted(set(mat_rename.values())):
                fout.write(f"newmtl {new_name}\nKa 0 0 0\nKd 0.8 0.8 0.8\nd 1\n\n")

    # Write sanitized OBJ.
    sanitized_obj_name = "model.obj"
    sanitized_obj_path = os.path.join(out_dir, sanitized_obj_name)
    with open(obj_path, "r", errors="replace") as fin, open(
        sanitized_obj_path, "w", newline="\n"
    ) as fout:
        for line in fin:
            if line.startswith("usemtl "):
                old = line[7:].strip()
                fout.write(f"usemtl {mat_rename.get(old, _sanitize_name(old))}\n")
            elif line.startswith("mtllib "):
                fout.write(f"mtllib {sanitized_mtl_name}\n")
            else:
                fout.write(line)

    # Copy canonical textures into the sanitized dir.
    for name in canonical_textures:
        src = next((p for p in texture_paths if os.path.basename(p) == name), None)
        if src:
            shutil.copy(src, os.path.join(out_dir, name))

    return sanitized_obj_path, sanitized_mtl_path, mat_rename, tex_rename, canonical_textures


def _stage_split(work: str, obj_path: str) -> list[tuple[str, str]]:
    """Split OBJ into per-material sub-OBJs. Returns [(safe_material, sub_obj_path), ...]."""
    out_dir = os.path.join(work, "split")
    os.makedirs(out_dir, exist_ok=True)

    verts: list[str] = []
    norms: list[str] = []
    uvs: list[str] = []
    groups: dict[str, list[str]] = {}
    current = "default"

    with open(obj_path, "r", errors="replace") as f:
        for line in f:
            if not line:
                continue
            if line.startswith("v "):
                verts.append(line.rstrip("\r\n"))
            elif line.startswith("vn "):
                norms.append(line.rstrip("\r\n"))
            elif line.startswith("vt "):
                uvs.append(line.rstrip("\r\n"))
            elif line.startswith("usemtl "):
                current = line[7:].strip()
                groups.setdefault(current, [])
            elif line.startswith("f "):
                groups.setdefault(current, []).append(line.rstrip("\r\n"))

    sub_paths: list[tuple[str, str]] = []
    for mat, faces in groups.items():
        if not faces:
            continue
        used_v: dict[int, int] = {}
        used_vt: dict[int, int] = {}
        used_vn: dict[int, int] = {}
        new_face_lines: list[str] = []
        for fline in faces:
            tokens = fline.split()[1:]
            new_tokens = []
            for tok in tokens:
                parts = tok.split("/")
                vi = int(parts[0])
                vti = int(parts[1]) if len(parts) > 1 and parts[1] else 0
                vni = int(parts[2]) if len(parts) > 2 and parts[2] else 0
                if vi not in used_v:
                    used_v[vi] = len(used_v) + 1
                if vti and vti not in used_vt:
                    used_vt[vti] = len(used_vt) + 1
                if vni and vni not in used_vn:
                    used_vn[vni] = len(used_vn) + 1
                ref = str(used_v[vi])
                if vti or vni:
                    ref += "/" + (str(used_vt[vti]) if vti else "")
                    if vni:
                        ref += "/" + str(used_vn[vni])
                new_tokens.append(ref)
            new_face_lines.append("f " + " ".join(new_tokens))

        safe = _sanitize_name(mat)
        sub_path = os.path.join(out_dir, f"{safe}.obj")
        with open(sub_path, "w", newline="\n") as out:
            out.write(f"# split: material {mat}\n")
            for old_idx, _ in sorted(used_v.items(), key=lambda x: x[1]):
                out.write(verts[old_idx - 1] + "\n")
            for old_idx, _ in sorted(used_vt.items(), key=lambda x: x[1]):
                out.write(uvs[old_idx - 1] + "\n")
            for old_idx, _ in sorted(used_vn.items(), key=lambda x: x[1]):
                out.write(norms[old_idx - 1] + "\n")
            out.write("\n".join(new_face_lines) + "\n")
        sub_paths.append((safe, sub_path))
    return sub_paths


def _parse_obj_geom(path: str):
    """Parse an OBJ for decimation. Returns (verts, uvs, norms, triangle_faces)."""
    import numpy as np
    verts: list[tuple[float, float, float]] = []
    uvs_l: list[tuple[float, float]] = []
    norms_l: list[tuple[float, float, float]] = []
    faces: list[tuple] = []
    with open(path, "r", errors="replace") as f:
        for line in f:
            if not line:
                continue
            if line[0] == "v":
                if line.startswith("v "):
                    p = line.split()
                    verts.append((float(p[1]), float(p[2]), float(p[3])))
                elif line.startswith("vt "):
                    p = line.split()
                    uvs_l.append((float(p[1]), float(p[2]) if len(p) > 2 else 0.0))
                elif line.startswith("vn "):
                    p = line.split()
                    norms_l.append((float(p[1]), float(p[2]), float(p[3])))
            elif line.startswith("f "):
                tokens = line.split()[1:]
                refs = []
                for tok in tokens:
                    parts = tok.split("/")
                    vi = int(parts[0]) - 1
                    vti = int(parts[1]) - 1 if len(parts) > 1 and parts[1] else -1
                    vni = int(parts[2]) - 1 if len(parts) > 2 and parts[2] else -1
                    refs.append((vi, vti, vni))
                # Fan triangulate
                for i in range(1, len(refs) - 1):
                    faces.append((refs[0], refs[i], refs[i + 1]))
    return (
        np.array(verts, dtype=np.float32),
        np.array(uvs_l, dtype=np.float32) if uvs_l else None,
        np.array(norms_l, dtype=np.float32) if norms_l else None,
        faces,
    )


def _write_obj_geom(path: str, verts, uvs, norms, tri_faces, header: str = "") -> None:
    with open(path, "w", newline="\n") as f:
        if header:
            f.write(header + "\n")
        for v in verts:
            f.write(f"v {v[0]:.5f} {v[1]:.5f} {v[2]:.5f}\n")
        if uvs is not None:
            for vt in uvs:
                f.write(f"vt {vt[0]:.5f} {vt[1]:.5f}\n")
        if norms is not None:
            for vn in norms:
                f.write(f"vn {vn[0]:.5f} {vn[1]:.5f} {vn[2]:.5f}\n")
        has_uv, has_n = uvs is not None, norms is not None
        for (a, b, c) in tri_faces:
            def ref(i: int) -> str:
                s = str(i + 1)
                if has_uv or has_n:
                    s += "/" + (str(i + 1) if has_uv else "")
                    if has_n:
                        s += "/" + str(i + 1)
                return s
            f.write(f"f {ref(int(a))} {ref(int(b))} {ref(int(c))}\n")


def _adaptive_target(
    face_count: int, median_ratio: float, p99_ratio: float, cfg: OptimizeConfig,
) -> int:
    if face_count < 1000:
        return face_count
    if p99_ratio > 0.05:
        return face_count
    if median_ratio > 0.01:
        return int(face_count * cfg.model_decim_target_coarse)
    if median_ratio > 0.005:
        return int(face_count * cfg.model_decim_target_med)
    return int(face_count * cfg.model_decim_target_fine)


def _stage_decimate_one(in_path: str, out_path: str, label: str, cfg: OptimizeConfig) -> None:
    import numpy as np
    import fast_simplification as fs

    verts, uvs, norms, faces = _parse_obj_geom(in_path)
    if len(faces) == 0:
        _write_obj_geom(out_path, verts, uvs, norms, [], f"# empty {label}")
        return

    tri_arr = np.array([(a[0], b[0], c[0]) for (a, b, c) in faces], dtype=np.int32)

    # Edge density
    tris = verts[tri_arr]
    edges = np.concatenate([
        np.linalg.norm(tris[:, 1] - tris[:, 0], axis=1),
        np.linalg.norm(tris[:, 2] - tris[:, 1], axis=1),
        np.linalg.norm(tris[:, 0] - tris[:, 2], axis=1),
    ])
    diag = float(np.linalg.norm(verts.max(axis=0) - verts.min(axis=0))) or 1.0
    median_ratio = float(np.median(edges) / diag)
    p99_ratio = float(np.percentile(edges, 99) / diag)
    f_target = _adaptive_target(len(faces), median_ratio, p99_ratio, cfg)

    # Per-vertex UV/normal (one per position, first reference wins).
    n_v = len(verts)
    v_uv = np.zeros((n_v, 2), dtype=np.float32)
    v_n = np.zeros((n_v, 3), dtype=np.float32)
    v_uv_set = np.zeros(n_v, dtype=bool)
    v_n_set = np.zeros(n_v, dtype=bool)
    for tri in faces:
        for (vi, vti, vni) in tri:
            if uvs is not None and vti >= 0 and not v_uv_set[vi]:
                v_uv[vi] = uvs[vti]
                v_uv_set[vi] = True
            if norms is not None and vni >= 0 and not v_n_set[vi]:
                v_n[vi] = norms[vni]
                v_n_set[vi] = True
    v_n[~v_n_set] = (0.0, 0.0, 1.0)

    if f_target >= len(faces):
        _write_obj_geom(
            out_path, verts,
            v_uv if uvs is not None else None,
            v_n if norms is not None else None,
            tri_arr, f"# pass-through {label}",
        )
        return

    _, _, collapses = fs.simplify(
        verts, tri_arr, target_count=f_target, return_collapses=True,
    )
    dec_pos, dec_tri, indice_mapping = fs.replay_simplification(verts, tri_arr, collapses)

    # Reject decimation that produced bridging triangles.
    diag_d = float(np.linalg.norm(dec_pos.max(axis=0) - dec_pos.min(axis=0))) or 1.0
    dt = dec_pos[dec_tri]
    longest = float(np.maximum.reduce([
        np.linalg.norm(dt[:, 1] - dt[:, 0], axis=1).max(),
        np.linalg.norm(dt[:, 2] - dt[:, 1], axis=1).max(),
        np.linalg.norm(dt[:, 0] - dt[:, 2], axis=1).max(),
    ]))
    if longest > cfg.model_decim_max_stretch * diag_d:
        _write_obj_geom(
            out_path, verts,
            v_uv if uvs is not None else None,
            v_n if norms is not None else None,
            tri_arr, f"# pass-through (decimation rejected) {label}",
        )
        return

    # Map UV/normal to output verts (average across collapsed originals).
    n_out = len(dec_pos)
    out_uv = np.zeros((n_out, 2), dtype=np.float32)
    out_n = np.zeros((n_out, 3), dtype=np.float32)
    counts = np.zeros(n_out, dtype=np.int32)
    indice_mapping = np.asarray(indice_mapping)
    valid = (indice_mapping >= 0) & (indice_mapping < n_out)
    np.add.at(out_uv, indice_mapping[valid], v_uv[valid])
    np.add.at(out_n, indice_mapping[valid], v_n[valid])
    np.add.at(counts, indice_mapping[valid], 1)
    nz = counts > 0
    out_uv[nz] /= counts[nz, None]
    out_n[nz] /= counts[nz, None]
    nlen = np.linalg.norm(out_n, axis=1, keepdims=True)
    nlen[nlen == 0] = 1.0
    out_n /= nlen

    _write_obj_geom(
        out_path, dec_pos,
        out_uv if uvs is not None else None,
        out_n if norms is not None else None,
        dec_tri, f"# decimated {label}",
    )


def _stage_decimate(work: str, sub_paths: list[tuple[str, str]], cfg: OptimizeConfig) -> list[tuple[str, str]]:
    out_dir = os.path.join(work, "decimated")
    os.makedirs(out_dir, exist_ok=True)
    out: list[tuple[str, str]] = []
    for safe, sub_path in sub_paths:
        out_path = os.path.join(out_dir, f"{safe}.obj")
        _stage_decimate_one(sub_path, out_path, safe, cfg)
        out.append((safe, out_path))
    return out


def _stage_merge(
    work: str,
    decimated: list[tuple[str, str]],
    sanitized_mtl: str,
    canonical_textures: set[str],
    sanitized_dir: str,
) -> tuple[str, str]:
    """Concat decimated sub-OBJs, copy MTL + textures into a flat output dir."""
    out_dir = os.path.join(work, "merged")
    os.makedirs(out_dir, exist_ok=True)
    obj_out = os.path.join(out_dir, "model.obj")
    mtl_out = os.path.join(out_dir, "model.mtl")

    with open(obj_out, "w", newline="\n") as out:
        out.write(f"# optimized\nmtllib {os.path.basename(mtl_out)}\n\n")
        v_off = vt_off = vn_off = 0
        for safe, sub in decimated:
            with open(sub) as inp:
                lines = inp.readlines()
            lv = sum(1 for L in lines if L.startswith("v "))
            lvt = sum(1 for L in lines if L.startswith("vt "))
            lvn = sum(1 for L in lines if L.startswith("vn "))
            out.write(f"\ng {safe}\nusemtl {safe}\n")
            for L in lines:
                if L.startswith(("v ", "vt ", "vn ")):
                    out.write(L)
                elif L.startswith("f "):
                    tokens = L.split()[1:]
                    new = []
                    for tok in tokens:
                        ps = tok.split("/")
                        vi = int(ps[0]) + v_off if ps[0] else 0
                        vti = (int(ps[1]) + vt_off) if (len(ps) > 1 and ps[1]) else 0
                        vni = (int(ps[2]) + vn_off) if (len(ps) > 2 and ps[2]) else 0
                        t = str(vi)
                        if vti or vni:
                            t += "/" + (str(vti) if vti else "")
                            if vni:
                                t += "/" + str(vni)
                        new.append(t)
                    out.write("f " + " ".join(new) + "\n")
            v_off += lv
            vt_off += lvt
            vn_off += lvn

    shutil.copy(sanitized_mtl, mtl_out)
    for name in canonical_textures:
        src = os.path.join(sanitized_dir, name)
        if os.path.exists(src):
            shutil.copy(src, os.path.join(out_dir, name))

    return obj_out, mtl_out


def _stage_resize_textures(merged_dir: str, target_px: int) -> int:
    """Downscale textures whose longer edge exceeds target_px. Returns count resized."""
    from PIL import Image
    resized = 0
    for name in os.listdir(merged_dir):
        if Path(name).suffix.lower() not in _TEXTURE_EXTS:
            continue
        path = os.path.join(merged_dir, name)
        try:
            with Image.open(path) as img:
                w, h = img.size
                if max(w, h) <= target_px:
                    continue
                if w >= h:
                    new_w, new_h = target_px, int(round(h * target_px / w))
                else:
                    new_h, new_w = target_px, int(round(w * target_px / h))
                img = img.convert("RGB") if img.mode in ("RGBA", "LA", "P") else img
                img = img.resize((new_w, new_h), Image.LANCZOS)
                # Preserve format by extension
                ext = Path(name).suffix.lower()
                if ext in (".jpg", ".jpeg"):
                    img.save(path, "JPEG", quality=85, optimize=True)
                else:
                    img.save(path)
                resized += 1
        except Exception:
            # Texture failed to process — leave the original in place.
            pass
    return resized


def _stage_to_dae(merged_obj: str, dae_out: str, cfg: OptimizeConfig) -> None:
    import numpy as np
    import trimesh
    import collada
    from collada import asset, source, geometry, scene

    sc = trimesh.load(merged_obj, force="scene", process=False)

    # Collect material → texture map by re-reading the MTL.
    mtl_path = os.path.join(os.path.dirname(merged_obj), "model.mtl")
    _materials, tex_refs = parse_mtl(mtl_path) if os.path.exists(mtl_path) else ([], {})

    mesh = collada.Collada()
    mesh.assetInfo.unitname = cfg.model_unit_name
    mesh.assetInfo.unitmeter = cfg.model_unit_meter
    mesh.assetInfo.upaxis = (
        asset.UP_AXIS.Z_UP if cfg.model_up_axis.upper() == "Z_UP" else asset.UP_AXIS.Y_UP
    )

    images_by_tex: dict[str, "collada.material.CImage"] = {}
    for tex in {t for t in tex_refs.values() if t}:
        img_id = "img_" + _sanitize_name(tex.replace(".", "_"))
        img = collada.material.CImage(img_id, tex)
        mesh.images.append(img)
        images_by_tex[tex] = img

    materials = {}
    for mat_name in sc.geometry.keys():
        eff_id = f"eff_{mat_name}"
        tex = tex_refs.get(mat_name)
        if tex and tex in images_by_tex:
            surf = collada.material.Surface(f"{eff_id}_surface", images_by_tex[tex], "2D")
            samp = collada.material.Sampler2D(f"{eff_id}_sampler", surf)
            tmap = collada.material.Map(samp, "UVSET0")
            eff = collada.material.Effect(
                eff_id, [surf, samp], "lambert",
                emission=(0.0, 0.0, 0.0, 1.0),
                ambient=(0.0, 0.0, 0.0, 1.0),
                diffuse=tmap,
                double_sided=True,
            )
        else:
            eff = collada.material.Effect(
                eff_id, [], "lambert",
                emission=(0.0, 0.0, 0.0, 1.0),
                ambient=(0.0, 0.0, 0.0, 1.0),
                diffuse=(0.5, 0.5, 0.5, 1.0),
                double_sided=True,
            )
        mesh.effects.append(eff)
        m = collada.material.Material(f"mat_{mat_name}", mat_name, eff)
        materials[mat_name] = m
        mesh.materials.append(m)

    geom_nodes = []
    for name, g in sc.geometry.items():
        verts = np.asarray(g.vertices, dtype=np.float32)
        faces = np.asarray(g.faces, dtype=np.int32)
        uvs = None
        if hasattr(g.visual, "uv") and g.visual.uv is not None:
            uvs = np.asarray(g.visual.uv, dtype=np.float32)

        vert_src = source.FloatSource(f"{name}_verts", verts.flatten(), ("X", "Y", "Z"))
        srcs = [vert_src]
        ilist = source.InputList()
        ilist.addInput(0, "VERTEX", f"#{name}_verts")
        if uvs is not None and len(uvs) == len(verts):
            uv_src = source.FloatSource(f"{name}_uvs", uvs.flatten(), ("S", "T"))
            srcs.append(uv_src)
            ilist.addInput(1, "TEXCOORD", f"#{name}_uvs", set="0")
            idx = np.empty((len(faces), 3, 2), dtype=np.int32)
            idx[:, :, 0] = faces
            idx[:, :, 1] = faces
            indices = idx.flatten()
        else:
            indices = faces.flatten()
        geo = geometry.Geometry(mesh, f"geom_{name}", name, srcs)
        triset = geo.createTriangleSet(indices, ilist, name)
        geo.primitives.append(triset)
        mesh.geometries.append(geo)
        matnode = scene.MaterialNode(name, materials[name], inputs=[("UVSET0", "TEXCOORD", "0")])
        gnode = scene.GeometryNode(geo, [matnode])
        geom_nodes.append(scene.Node(f"node_{name}", children=[gnode]))

    visual = scene.Scene("VisualSceneNode", geom_nodes)
    mesh.scenes.append(visual)
    mesh.scene = visual
    mesh.write(dae_out)


def _stage_to_glb(merged_obj: str, glb_out: str) -> None:
    import trimesh
    sc = trimesh.load(merged_obj, force="scene", process=False)
    sc.export(glb_out)


# ---------------------------------------------------------------------------
# Execute
# ---------------------------------------------------------------------------

class ModelExecuteProgress(NamedTuple):
    sku: str
    supplier: str
    sku_index: int
    sku_total: int
    stage: str           # download | sanitize | split | decimate | merge | resize | dae | glb | upload | done | error
    detail: str
    error: str | None = None


def execute(
    targets: list[ModelTarget], cfg: OptimizeConfig,
) -> Generator[ModelExecuteProgress, None, None]:
    """Run the full pipeline on each target and upload results to <sku>/<dest_subdir>/."""
    for sku_idx, t in enumerate(targets, 1):
        try:
            yield from _process_one(t, cfg, sku_idx, len(targets))
        except Exception as exc:
            yield ModelExecuteProgress(
                sku=t.sku, supplier=t.supplier,
                sku_index=sku_idx, sku_total=len(targets),
                stage="error", detail=t.obj_file["name"], error=str(exc),
            )


def _process_one(
    t: ModelTarget, cfg: OptimizeConfig, sku_idx: int, sku_total: int,
) -> Generator[ModelExecuteProgress, None, None]:
    def _yield(stage: str, detail: str = "") -> ModelExecuteProgress:
        return ModelExecuteProgress(
            sku=t.sku, supplier=t.supplier,
            sku_index=sku_idx, sku_total=sku_total,
            stage=stage, detail=detail,
        )

    with tempfile.TemporaryDirectory() as work:
        # 1. Download OBJ + MTL + textures
        yield _yield("download", t.obj_file["name"])
        in_dir = os.path.join(work, "in")
        os.makedirs(in_dir, exist_ok=True)
        obj_local = os.path.join(in_dir, t.obj_file["name"])
        drive.download_file(t.obj_file["id"], obj_local)
        mtl_local = None
        if t.mtl_files:
            mtl_local = os.path.join(in_dir, t.mtl_files[0]["name"])
            drive.download_file(t.mtl_files[0]["id"], mtl_local)
        tex_locals: list[str] = []
        for tex in t.texture_files:
            p = os.path.join(in_dir, tex["name"])
            drive.download_file(tex["id"], p)
            tex_locals.append(p)

        # 2. Sanitize
        yield _yield("sanitize")
        san_obj, san_mtl, mat_rename, tex_rename, canonical = _stage_sanitize(
            work, obj_local, mtl_local, tex_locals,
        )
        sanitized_dir = os.path.dirname(san_obj)

        # 3. Split
        yield _yield("split")
        sub_paths = _stage_split(work, san_obj)

        # 4. Decimate
        yield _yield("decimate", f"{len(sub_paths)} sub-meshes")
        decimated = _stage_decimate(work, sub_paths, cfg)

        # 5. Merge
        yield _yield("merge")
        merged_obj, merged_mtl = _stage_merge(work, decimated, san_mtl, canonical, sanitized_dir)
        merged_dir = os.path.dirname(merged_obj)

        # 6. Resize textures
        n_resized = _stage_resize_textures(merged_dir, cfg.model_target_texture_px)
        if n_resized:
            yield _yield("resize", f"{n_resized} textures → ≤{cfg.model_target_texture_px}px")

        # 7. Export GLB
        yield _yield("glb")
        glb_path = os.path.join(merged_dir, "model.glb")
        _stage_to_glb(merged_obj, glb_path)

        # 8. Export DAE
        yield _yield("dae")
        dae_path = os.path.join(merged_dir, "model.dae")
        _stage_to_dae(merged_obj, dae_path, cfg)

        # 9. Upload everything in merged_dir to <sku>/<dest_subdir>/
        yield _yield("upload")
        dest_id = drive.find_or_create_folder(t.dest_subdir, t.sku_folder_id)
        existing = {f["name"] for f in drive.list_files(dest_id)}
        for fname in sorted(os.listdir(merged_dir)):
            if fname in existing:
                continue
            local = os.path.join(merged_dir, fname)
            mime = _guess_mime(fname)
            drive.upload_file(local, dest_id, fname, mime)

        yield _yield("done")


def _guess_mime(name: str) -> str:
    ext = Path(name).suffix.lower()
    return {
        ".obj": "model/obj",
        ".mtl": "text/plain",
        ".dae": "model/vnd.collada+xml",
        ".glb": "model/gltf-binary",
        ".gltf": "model/gltf+json",
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".png": "image/png",
        ".webp": "image/webp",
    }.get(ext, "application/octet-stream")

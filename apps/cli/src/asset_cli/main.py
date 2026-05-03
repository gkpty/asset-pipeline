from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

import typer
from dotenv import load_dotenv
from rich.console import Console
from rich.progress import BarColumn, MofNCompleteColumn, Progress, SpinnerColumn, TextColumn

load_dotenv()

console = Console()
app = typer.Typer(name="asset", help="Asset optimization pipeline CLI.")


@app.command()
def init(
    sheet_id: str = typer.Option(
        ..., envvar="GOOGLE_SHEETS_MASTER_ID",
        help="Google Sheets file ID for the master product list.",
    ),
    folder_id: str = typer.Option(
        ..., envvar="GOOGLE_DRIVE_ROOT_FOLDER_ID",
        help="Google Drive parent folder ID (contains category subfolders like products/, materials/).",
    ),
    category: str = typer.Option(
        "products", "--category",
        help="Subfolder under the parent root to verify (products, materials, …). Default: products.",
    ),
) -> None:
    """Check credentials and confirm access to the Google Sheet, the Drive parent
    folder, and the requested category subfolder."""
    from asset_sdk.adapters.drive import get_item_name, resolve_category_folder
    from asset_sdk.adapters.sheets import get_spreadsheet_title

    console.print("[bold]Checking Google credentials and resource access…[/bold]\n")

    try:
        title = get_spreadsheet_title(sheet_id)
        console.print(f"[green]✓[/green] Google Sheet found:  [bold]{title}[/bold]")
        console.print(f"    ID: {sheet_id}")
    except Exception as exc:
        console.print(f"[red]✗[/red] Google Sheet not accessible: {exc}")

    console.print()

    try:
        name = get_item_name(folder_id)
        console.print(f"[green]✓[/green] Parent folder found:  [bold]{name}[/bold]")
        console.print(f"    ID: {folder_id}")
    except Exception as exc:
        console.print(f"[red]✗[/red] Parent folder not accessible: {exc}")

    console.print()

    try:
        category_id = resolve_category_folder(folder_id, category)
        console.print(
            f"[green]✓[/green] Category subfolder found:  [bold]{category}[/bold]"
        )
        console.print(f"    ID: {category_id}")
    except Exception as exc:
        console.print(f"[red]✗[/red] Category subfolder {category!r} not found: {exc}")


@app.command()
def diagnose(
    config_path: Path = typer.Option(
        Path("pipeline.config.toml"),
        envvar="PIPELINE_CONFIG_PATH",
        help="Path to pipeline.config.toml.",
    ),
    folder_id: str = typer.Option(
        ..., envvar="GOOGLE_DRIVE_ROOT_FOLDER_ID",
        help="Google Drive parent folder ID (contains category subfolders).",
    ),
    sheet_id: str = typer.Option(
        ..., envvar="GOOGLE_SHEETS_MASTER_ID",
        help="Google Sheets file ID.",
    ),
    category: str = typer.Option(
        "products", "--category",
        help="Subfolder under the parent root: products, materials, … (default: products).",
    ),
    tab: Optional[str] = typer.Option(
        None, help="Sheet tab with the SKU list. Defaults to the category name.",
    ),
    sku_col: Optional[str] = typer.Option(
        None, help="SKU column header. Defaults to csv.sku_column in config.",
    ),
    supplier_col: Optional[str] = typer.Option(
        None, help="Supplier column header. Defaults to csv.supplier_column in config.",
    ),
    report_tab: Optional[str] = typer.Option(
        None, help="Tab to write the report into. Defaults to '<diagnose.report_tab> - <Category>'.",
    ),
) -> None:
    """
    Scan a Google Drive category folder (products/materials/...) and report its
    structure against a Google Sheets SKU list. Most options default to values
    in pipeline.config.toml.
    """
    from asset_sdk.adapters import drive
    from asset_sdk.adapters.sheets import read_rows, write_report
    from asset_sdk.config import PipelineConfig
    from asset_sdk.stages.diagnose import run, to_sheet_rows

    cfg = PipelineConfig.load(config_path)
    category_folder_id = drive.resolve_category_folder(folder_id, category)

    # Resolve: CLI flag → category default → config default
    _tab          = tab          or category
    _sku_col      = sku_col      or cfg.csv.sku_column
    _supplier_col = supplier_col or cfg.csv.supplier_column
    _report_tab   = report_tab   or f"{cfg.diagnose.report_tab} - {category.title()}"
    _structure    = cfg.structure_for(category)

    with Progress(SpinnerColumn(), TextColumn("{task.description}"), console=console) as progress:
        t = progress.add_task(f"Reading '{_tab}' tab…", total=None)
        sheet_rows = read_rows(sheet_id, _tab)
        progress.update(t, description=f"Read {len(sheet_rows)} rows from '{_tab}'")
        progress.stop_task(t)

        t = progress.add_task(f"Scanning {category} drive…", total=None)
        report = run(category_folder_id, sheet_rows, _sku_col, _supplier_col, cfg.paths, _structure)
        progress.update(t, description=f"Scanned {len(report.rows)} rows")
        progress.stop_task(t)

        t = progress.add_task(f"Writing report to '{_report_tab}'…", total=None)
        headers, rows = to_sheet_rows(report, cfg.paths)
        write_report(sheet_id, _report_tab, headers, rows)
        progress.update(t, description=f"Report written → '{_report_tab}'")
        progress.stop_task(t)

    ok          = sum(1 for r in report.rows if r.status == "OK"          and not r.is_duplicate)
    incomplete  = sum(1 for r in report.rows if r.status == "INCOMPLETE"  and not r.is_duplicate)
    missing     = sum(1 for r in report.rows if r.status == "MISSING DIR")
    orphans     = sum(1 for r in report.rows if r.status == "ORPHAN DIR"  and not r.is_duplicate)
    duplicates  = sum(1 for r in report.rows if r.is_duplicate)
    delete_n    = sum(1 for r in report.rows if r.suggested_action == "DELETE")
    merge_n     = sum(1 for r in report.rows if r.suggested_action == "MERGE")

    console.print()
    console.print("[bold]Diagnose complete[/bold]")
    console.print(f"  [green]OK[/green]           {ok}")
    console.print(f"  [yellow]Incomplete[/yellow]   {incomplete}")
    console.print(f"  [yellow]Duplicates[/yellow]   {duplicates}  (delete={delete_n}, merge={merge_n})")
    console.print(f"  [red]Missing dir[/red]  {missing}")
    console.print(f"  [red]Orphan dirs[/red]  {orphans}")


@app.command("rename-lifestyle-photos")
def rename_lifestyle_photos(
    config_path: Path = typer.Option(
        Path("pipeline.config.toml"),
        envvar="PIPELINE_CONFIG_PATH",
        help="Path to pipeline.config.toml.",
    ),
    lifestyle_folder_id: str = typer.Option(
        ..., envvar="GOOGLE_DRIVE_LIFESTYLE_FOLDER_ID",
        help="Google Drive folder containing the lifestyle photo folders.",
    ),
    root_folder_id: str = typer.Option(
        ..., envvar="GOOGLE_DRIVE_ROOT_FOLDER_ID",
        help="Google Drive parent folder ID (the products subfolder is resolved automatically).",
    ),
    sheet_id: str = typer.Option(
        ..., envvar="GOOGLE_SHEETS_MASTER_ID",
        help="Google Sheets file ID.",
    ),
    tab: Optional[str] = typer.Option(
        None, help="Sheet tab with the SKU list. Defaults to 'products'.",
    ),
    sku_col: Optional[str] = typer.Option(
        None, help="SKU column header. Defaults to csv.sku_column in config.",
    ),
    parent_product_col: Optional[str] = typer.Option(
        None, help="Parent product column header. Defaults to csv.parent_product_column in config.",
    ),
    report_tab: Optional[str] = typer.Option(
        None, help="Tab to write the report into. Defaults to '<lifestyle.report_tab> - Products'.",
    ),
    execute: bool = typer.Option(
        False, "--execute",
        help="Rename folders in Drive to their selected SKU. Omit for a dry run.",
    ),
) -> None:
    """
    Map lifestyle photo folders (named by parent product) to SKUs and write a
    rename report. Products-only — operates on the products subfolder under
    GOOGLE_DRIVE_ROOT_FOLDER_ID. Pass --execute to actually rename the folders.
    """
    from asset_sdk.adapters import drive
    from asset_sdk.adapters.sheets import read_rows, write_report
    from asset_sdk.config import PipelineConfig
    from asset_sdk.stages.rename_lifestyle import build_report, execute_copy, to_sheet_rows

    cfg = PipelineConfig.load(config_path)
    products_folder_id = drive.resolve_category_folder(root_folder_id, "products")

    _tab               = tab               or "products"
    _sku_col           = sku_col           or cfg.csv.sku_column
    _parent_product_col = parent_product_col or cfg.csv.parent_product_column
    _report_tab        = report_tab        or f"{cfg.lifestyle.report_tab} - Products"

    with Progress(SpinnerColumn(), TextColumn("{task.description}"), console=console) as progress:
        t = progress.add_task(f"Reading '{_tab}' tab…", total=None)
        sheet_rows = read_rows(sheet_id, _tab)
        progress.update(t, description=f"Read {len(sheet_rows)} rows from '{_tab}'")
        progress.stop_task(t)

        t = progress.add_task("Scanning lifestyle folders…", total=None)
        entries = build_report(lifestyle_folder_id, sheet_rows, _sku_col, _parent_product_col)
        progress.update(t, description=f"Found {len(entries)} lifestyle folders")
        progress.stop_task(t)

    copied = 0
    skipped = 0
    if execute:
        copy_bar = Progress(
            SpinnerColumn(),
            TextColumn("{task.description}"),
            BarColumn(bar_width=30),
            MofNCompleteColumn(),
            console=console,
        )
        total_folders = sum(1 for e in entries if e.selected_sku)
        skipped = 0
        with copy_bar:
            overall_task = copy_bar.add_task(
                "Overall", total=total_folders, completed=0
            )
            file_task = copy_bar.add_task("Preparing…", total=1)
            current_entry: str | None = None
            for p in execute_copy(
                entries,
                products_folder_id,
                cfg.structure_for("products"),
                cfg.paths.lifestyle_photos,
            ):
                if p.file_total == 0:
                    # All files already present — advance overall bar, skip file bar.
                    skipped += 1
                    copy_bar.advance(overall_task)
                    folders_done = int(copy_bar.tasks[overall_task].completed)
                    copy_bar.update(
                        overall_task,
                        description=f"Copied {folders_done}/{total_folders} lifestyle folders ({skipped} already done)",
                    )
                    continue
                if p.entry_name != current_entry:
                    current_entry = p.entry_name
                    copy_bar.reset(file_task, total=p.file_total)
                copy_bar.update(
                    file_task,
                    description=(
                        f"  lifestyle/[bold]{p.entry_name}[/bold]"
                        f" → products/[bold]{p.dest_sku}[/bold]"
                    ),
                    advance=1,
                )
                if p.file_index == p.file_total:
                    copy_bar.advance(overall_task)
                    folders_done = int(copy_bar.tasks[overall_task].completed)
                    copy_bar.update(
                        overall_task,
                        description=f"Copied {folders_done}/{total_folders} lifestyle folders ({skipped} already done)",
                    )
                copied += 1

    with Progress(SpinnerColumn(), TextColumn("{task.description}"), console=console) as progress:
        t = progress.add_task(f"Writing report to '{_report_tab}'…", total=None)
        headers, rows = to_sheet_rows(entries)
        write_report(sheet_id, _report_tab, headers, rows)
        progress.update(t, description=f"Report written → '{_report_tab}'")
        progress.stop_task(t)

    in_sheet    = sum(1 for e in entries if e.in_sheet)
    not_in_sheet = sum(1 for e in entries if not e.in_sheet)
    multi       = sum(1 for e in entries if e.multiple_skus)

    console.print()
    if execute:
        console.print(f"[bold]Copy complete[/bold]  ({copied} files copied, {skipped} folders already done)")
    else:
        console.print("[bold]Dry run complete[/bold] (pass --execute to copy files)")
    console.print(f"  [green]Matched[/green]        {in_sheet}")
    console.print(f"  [yellow]Multiple SKUs[/yellow]  {multi}")
    console.print(f"  [red]Not in sheet[/red]   {not_in_sheet}")


@app.command("copy-models-into-products")
def copy_models_into_products(
    config_path: Path = typer.Option(
        Path("pipeline.config.toml"),
        envvar="PIPELINE_CONFIG_PATH",
        help="Path to pipeline.config.toml.",
    ),
    models_folder_id: str = typer.Option(
        ..., envvar="GOOGLE_DRIVE_MODELS_FOLDER_ID",
        help="Google Drive folder containing per-SKU model folders.",
    ),
    root_folder_id: str = typer.Option(
        ..., envvar="GOOGLE_DRIVE_ROOT_FOLDER_ID",
        help="Google Drive parent folder ID (the products subfolder is resolved automatically).",
    ),
    sheet_id: str = typer.Option(
        ..., envvar="GOOGLE_SHEETS_MASTER_ID",
        help="Google Sheets file ID (used only to write the report).",
    ),
    report_tab: Optional[str] = typer.Option(
        None, help="Tab to write the report into. Defaults to '<models.report_tab> - Products'.",
    ),
    execute: bool = typer.Option(
        False, "--execute",
        help="Copy model files into each SKU's product folder. Omit for a dry run.",
    ),
) -> None:
    """
    Scan the shared models folder, compare against the products Drive, and report on each
    SKU's contents (OBJ/SKP/DWG/CAD/PDF/GLTF). On --execute, copies model files into each
    SKU's product folder; orphan SKUs (in models but not in products) are created.
    PDF files are copied into the /diagram subfolder. Products-only.
    """
    from asset_sdk.adapters import drive
    from asset_sdk.adapters.sheets import write_report
    from asset_sdk.config import PipelineConfig
    from asset_sdk.stages.copy_models import build_report, execute_copy, to_sheet_rows

    cfg = PipelineConfig.load(config_path)
    products_folder_id = drive.resolve_category_folder(root_folder_id, "products")
    _report_tab = report_tab or f"{cfg.models.report_tab} - Products"

    with Progress(SpinnerColumn(), TextColumn("{task.description}"), console=console) as progress:
        t = progress.add_task("Scanning models folder & products drive…", total=None)
        entries, missing_skus = build_report(
            models_folder_id, products_folder_id, cfg.structure_for("products"),
        )
        progress.update(t, description=f"Found {len(entries)} SKU folders in models")
        progress.stop_task(t)

        t = progress.add_task(f"Writing report to '{_report_tab}'…", total=None)
        headers, rows = to_sheet_rows(entries, missing_skus)
        write_report(sheet_id, _report_tab, headers, rows)
        progress.update(t, description=f"Report written → '{_report_tab}'")
        progress.stop_task(t)

    copied = 0
    skipped = 0
    if execute:
        total_skus = len(entries)
        copy_bar = Progress(
            SpinnerColumn(),
            TextColumn("{task.description}"),
            BarColumn(bar_width=30),
            MofNCompleteColumn(),
            console=console,
        )
        with copy_bar:
            overall_task = copy_bar.add_task("Overall", total=total_skus, completed=0)
            file_task = copy_bar.add_task("Preparing…", total=1)
            current_sku: str | None = None
            for p in execute_copy(entries, products_folder_id, cfg.structure_for("products"), cfg.paths):
                if p.file_total == 0:
                    skipped += 1
                    copy_bar.advance(overall_task)
                    done = int(copy_bar.tasks[overall_task].completed)
                    copy_bar.update(
                        overall_task,
                        description=f"Copied {done}/{total_skus} SKUs ({skipped} already done)",
                    )
                    continue
                if p.sku_name != current_sku:
                    current_sku = p.sku_name
                    copy_bar.reset(file_task, total=p.file_total)
                copy_bar.update(
                    file_task,
                    description=f"  [bold]{p.sku_name}[/bold] / {p.source_dir}",
                    advance=1,
                )
                if p.file_index == p.file_total:
                    copy_bar.advance(overall_task)
                    done = int(copy_bar.tasks[overall_task].completed)
                    copy_bar.update(
                        overall_task,
                        description=f"Copied {done}/{total_skus} SKUs ({skipped} already done)",
                    )
                copied += 1

    orphan_count = sum(1 for e in entries if e.is_orphan)
    has_extras   = sum(1 for e in entries if e.extra_items)
    has_nested   = sum(1 for e in entries if e.has_nested_models)
    matched      = len(entries) - orphan_count

    console.print()
    if execute:
        console.print(f"[bold]Copy complete[/bold]  ({copied} files copied, {skipped} SKUs already done)")
    else:
        console.print("[bold]Dry run complete[/bold] (pass --execute to copy files)")
    console.print(f"  [green]Matched in products[/green]    {matched}")
    console.print(f"  [yellow]Orphans (created in products)[/yellow] {orphan_count}")
    console.print(f"  [yellow]Nested models folder[/yellow]   {has_nested}")
    console.print(f"  [yellow]Has extra items[/yellow]        {has_extras}")
    console.print(f"  [red]Missing from models[/red]    {len(missing_skus)}")


@app.command("upload-local-files")
def upload_local_files(
    input_dir: Path = typer.Option(
        ..., "--input",
        help="Local directory containing the files to upload.",
    ),
    asset_type: str = typer.Option(
        ..., "--type",
        help="Asset type (e.g. photo, lifestyle, video, diagram, assembly_instructions, "
             "carton_layout, barcode, obj, skp, dwg, gltf).",
    ),
    supplier: Optional[str] = typer.Option(
        None, "--supplier",
        help="Restrict SKU matching to one supplier (faster + fewer false positives).",
    ),
    config_path: Path = typer.Option(
        Path("pipeline.config.toml"),
        envvar="PIPELINE_CONFIG_PATH",
        help="Path to pipeline.config.toml.",
    ),
    root_folder_id: str = typer.Option(
        ..., envvar="GOOGLE_DRIVE_ROOT_FOLDER_ID",
        help="Google Drive parent folder ID (contains category subfolders).",
    ),
    sheet_id: str = typer.Option(
        ..., envvar="GOOGLE_SHEETS_MASTER_ID",
        help="Google Sheets file ID.",
    ),
    category: str = typer.Option(
        "products", "--category",
        help="Subfolder under the parent root: products, materials, … (default: products).",
    ),
    tab: Optional[str] = typer.Option(
        None, help="Sheet tab with the SKU list. Defaults to the category name.",
    ),
    report_tab: Optional[str] = typer.Option(
        None, help="Tab to write the report into. Defaults to 'Upload - <type> - <Category>'.",
    ),
    execute: bool = typer.Option(
        False, "--execute",
        help="Read the (possibly edited) report and upload files. Omit for a dry run.",
    ),
) -> None:
    """
    Generic local-file uploader. Dry run scans the input directory, infers a destination
    SKU per file (filename → supplier ref → product name → PDF content → fuzzy), and
    writes a report to a sheet tab. Edit that tab as needed, then re-run with --execute
    to upload each file to <category>/<supplier>/<sku>/<type subdir>/.
    """
    from asset_sdk.adapters import drive
    from asset_sdk.adapters.sheets import read_rows, write_report
    from asset_sdk.config import PipelineConfig
    from asset_sdk.stages.upload_local_files import (
        build_report,
        execute_copy,
        resolve_type_subdir,
        to_sheet_rows,
    )

    cfg = PipelineConfig.load(config_path)
    category_folder_id = drive.resolve_category_folder(root_folder_id, category)
    subdir = resolve_type_subdir(asset_type, cfg.paths)

    _tab            = tab        or category
    _report_tab     = report_tab or f"Upload - {asset_type} - {category.title()}"
    _sku_col        = cfg.csv.sku_column
    _name_col       = cfg.csv.name_column
    _supplier_col   = cfg.csv.supplier_column
    _supplier_ref_col = cfg.csv.supplier_ref_column

    if not execute:
        with Progress(SpinnerColumn(), TextColumn("{task.description}"), console=console) as progress:
            t = progress.add_task(f"Reading '{_tab}' tab…", total=None)
            sheet_rows = read_rows(sheet_id, _tab)
            progress.update(t, description=f"Read {len(sheet_rows)} rows from '{_tab}'")
            progress.stop_task(t)

            t = progress.add_task(f"Scanning {input_dir}…", total=None)
            matches = build_report(
                input_dir, subdir, sheet_rows,
                _sku_col, _name_col, _supplier_col, _supplier_ref_col,
                cfg.structure_for(category), supplier,
            )
            progress.update(t, description=f"Found {len(matches)} files")
            progress.stop_task(t)

            t = progress.add_task(f"Writing report to '{_report_tab}'…", total=None)
            headers, rows = to_sheet_rows(matches)
            write_report(sheet_id, _report_tab, headers, rows)
            progress.update(t, description=f"Report written → '{_report_tab}'")
            progress.stop_task(t)

        by_conf: dict[str, int] = {}
        for m in matches:
            by_conf[m.confidence] = by_conf.get(m.confidence, 0) + 1

        console.print()
        console.print(f"[bold]Dry run complete[/bold] (subdir = {subdir})")
        console.print(f"  [green]HIGH[/green]    {by_conf.get('HIGH', 0)}")
        console.print(f"  [yellow]MEDIUM[/yellow]  {by_conf.get('MEDIUM', 0)}")
        console.print(f"  [yellow]LOW[/yellow]     {by_conf.get('LOW', 0)}")
        console.print(f"  [red]NONE[/red]    {by_conf.get('NONE', 0)}")
        console.print(f"\nReview '{_report_tab}', edit any wrong SKUs, then re-run with --execute.")
        return

    # ------------------ EXECUTE ------------------
    with Progress(SpinnerColumn(), TextColumn("{task.description}"), console=console) as progress:
        t = progress.add_task(f"Reading report '{_report_tab}'…", total=None)
        report_rows = read_rows(sheet_id, _report_tab)
        progress.update(t, description=f"Read {len(report_rows)} rows from '{_report_tab}'")
        progress.stop_task(t)

    actionable = sum(1 for r in report_rows if r.get("Destination SKU", "").strip())
    uploaded = 0
    skipped = 0
    upload_bar = Progress(
        SpinnerColumn(),
        TextColumn("{task.description}"),
        BarColumn(bar_width=30),
        MofNCompleteColumn(),
        console=console,
    )
    with upload_bar:
        bar_task = upload_bar.add_task("Uploading…", total=actionable, completed=0)
        for p in execute_copy(report_rows, input_dir, subdir, category_folder_id, cfg.structure_for(category)):
            label = "skipped" if p.skipped else "uploaded"
            upload_bar.update(
                bar_task,
                description=f"{label}: [bold]{p.rel_path}[/bold] → {p.sku}",
                advance=1,
            )
            if p.skipped:
                skipped += 1
            else:
                uploaded += 1

    console.print()
    console.print(f"[bold]Upload complete[/bold]  ({uploaded} uploaded, {skipped} skipped)")


@app.command("rename")
def rename(
    config_path: Path = typer.Option(
        Path("pipeline.config.toml"),
        envvar="PIPELINE_CONFIG_PATH",
        help="Path to pipeline.config.toml.",
    ),
    root_folder_id: str = typer.Option(
        ..., envvar="GOOGLE_DRIVE_ROOT_FOLDER_ID",
        help="Google Drive parent folder ID (contains category subfolders).",
    ),
    sheet_id: str = typer.Option(
        ..., envvar="GOOGLE_SHEETS_MASTER_ID",
        help="Google Sheets file ID (where the diagnose report lives).",
    ),
    category: str = typer.Option(
        "products", "--category",
        help="Subfolder under the parent root: products, materials, … (default: products).",
    ),
    report_tab: Optional[str] = typer.Option(
        None, help="Diagnose report tab. Defaults to '<diagnose.report_tab> - <Category>'.",
    ),
    execute: bool = typer.Option(
        False, "--execute",
        help="Apply the renames in Drive. Omit for a dry run.",
    ),
) -> None:
    """
    Read the Diagnose Report and rename Drive folders according to the
    'Suggested Rename' column. Duplicate rows are skipped — resolve duplicates
    first (DELETE/MERGE), then run rename.
    """
    from rich.table import Table

    from asset_sdk.adapters import drive
    from asset_sdk.adapters.sheets import read_rows
    from asset_sdk.config import PipelineConfig
    from asset_sdk.stages.rename_skus import build_plan, execute_renames

    cfg = PipelineConfig.load(config_path)
    category_folder_id = drive.resolve_category_folder(root_folder_id, category)
    _report_tab = report_tab or f"{cfg.diagnose.report_tab} - {category.title()}"

    with Progress(SpinnerColumn(), TextColumn("{task.description}"), console=console) as progress:
        t = progress.add_task(f"Reading '{_report_tab}'…", total=None)
        report_rows = read_rows(sheet_id, _report_tab)
        progress.update(t, description=f"Read {len(report_rows)} rows from '{_report_tab}'")
        progress.stop_task(t)

        t = progress.add_task("Resolving Drive folders…", total=None)
        plans, warnings = build_plan(report_rows, category_folder_id, cfg.structure_for(category))
        progress.update(
            t,
            description=f"Resolved {len(plans)} renames ({len(warnings)} warnings)",
        )
        progress.stop_task(t)

    if plans:
        table = Table(title=f"Planned renames ({len(plans)})")
        table.add_column("Supplier", style="cyan")
        table.add_column("Current SKU")
        table.add_column("→")
        table.add_column("New SKU", style="green")
        for p in plans:
            table.add_row(p.supplier, p.sku, "→", p.new_sku)
        console.print(table)
    else:
        console.print("[yellow]No renames planned.[/yellow]")

    for w in warnings:
        console.print(f"[yellow]⚠[/yellow] {w}")

    if not execute:
        console.print(
            f"\n[bold]Dry run complete[/bold]"
            + (f" — pass --execute to perform {len(plans)} renames." if plans else ".")
        )
        return

    if not plans:
        return

    rename_bar = Progress(
        SpinnerColumn(),
        TextColumn("{task.description}"),
        BarColumn(bar_width=30),
        MofNCompleteColumn(),
        console=console,
    )
    with rename_bar:
        bar_task = rename_bar.add_task("Renaming…", total=len(plans), completed=0)
        for p in execute_renames(plans):
            rename_bar.update(
                bar_task,
                description=f"  [bold]{p.plan.supplier}/{p.plan.sku}[/bold] → [green]{p.plan.new_sku}[/green]",
                advance=1,
            )

    console.print(f"\n[bold]Renamed {len(plans)} folders.[/bold] Re-run diagnose to refresh the report.")


@app.command("dedupe")
def dedupe(
    config_path: Path = typer.Option(
        Path("pipeline.config.toml"),
        envvar="PIPELINE_CONFIG_PATH",
        help="Path to pipeline.config.toml.",
    ),
    root_folder_id: str = typer.Option(
        ..., envvar="GOOGLE_DRIVE_ROOT_FOLDER_ID",
        help="Google Drive parent folder ID (contains category subfolders).",
    ),
    sheet_id: str = typer.Option(
        ..., envvar="GOOGLE_SHEETS_MASTER_ID",
        help="Google Sheets file ID (where the diagnose report lives).",
    ),
    category: str = typer.Option(
        "products", "--category",
        help="Subfolder under the parent root: products, materials, … (default: products).",
    ),
    report_tab: Optional[str] = typer.Option(
        None, help="Diagnose report tab. Defaults to '<diagnose.report_tab> - <Category>'.",
    ),
    execute: bool = typer.Option(
        False, "--execute",
        help="Apply DELETE/MERGE actions in Drive. Omit for a dry run.",
    ),
) -> None:
    """
    Read the Diagnose Report and act on duplicate rows according to the
    'Suggested Action' column. DELETE trashes the duplicate folder.
    MERGE copies any unique files into the primary (preserving subfolder
    structure) before trashing the duplicate. Trashed items are recoverable
    from Drive's bin.
    """
    from rich.table import Table

    from asset_sdk.adapters import drive
    from asset_sdk.adapters.sheets import read_rows
    from asset_sdk.config import PipelineConfig
    from asset_sdk.stages.dedupe import build_plan, execute as run_dedupe

    cfg = PipelineConfig.load(config_path)
    category_folder_id = drive.resolve_category_folder(root_folder_id, category)
    _report_tab = report_tab or f"{cfg.diagnose.report_tab} - {category.title()}"

    with Progress(SpinnerColumn(), TextColumn("{task.description}"), console=console) as progress:
        t = progress.add_task(f"Reading '{_report_tab}'…", total=None)
        report_rows = read_rows(sheet_id, _report_tab)
        progress.update(t, description=f"Read {len(report_rows)} rows from '{_report_tab}'")
        progress.stop_task(t)

        t = progress.add_task("Resolving Drive folders…", total=None)
        plans, warnings = build_plan(report_rows, category_folder_id, cfg.structure_for(category))
        progress.update(
            t,
            description=f"Resolved {len(plans)} actions ({len(warnings)} warnings)",
        )
        progress.stop_task(t)

    if plans:
        table = Table(title=f"Planned dedupe actions ({len(plans)})")
        table.add_column("Action", style="bold")
        table.add_column("Duplicate")
        table.add_column("Duplicate ID", style="dim")
        table.add_column("→")
        table.add_column("Primary")
        table.add_column("Primary ID", style="dim")
        for p in plans:
            colour = "red" if p.action == "DELETE" else "yellow"
            table.add_row(
                f"[{colour}]{p.action}[/{colour}]",
                f"{p.supplier}/{p.sku}",
                p.dup_folder_id,
                "→",
                f"{p.primary_supplier}/{p.sku}",
                p.primary_folder_id,
            )
        console.print(table)
    else:
        console.print("[yellow]No dedupe actions planned.[/yellow]")

    for w in warnings:
        console.print(f"[yellow]⚠[/yellow] {w}")

    delete_n = sum(1 for p in plans if p.action == "DELETE")
    merge_n  = sum(1 for p in plans if p.action == "MERGE")

    if not execute:
        console.print(
            f"\n[bold]Dry run complete[/bold] — {delete_n} delete, {merge_n} merge."
            + (" Pass --execute to apply." if plans else "")
        )
        return

    if not plans:
        return

    dedupe_bar = Progress(
        SpinnerColumn(),
        TextColumn("{task.description}"),
        BarColumn(bar_width=30),
        MofNCompleteColumn(),
        console=console,
    )
    total_copied = 0
    with dedupe_bar:
        bar_task = dedupe_bar.add_task("Processing…", total=len(plans), completed=0)
        for prog in run_dedupe(plans):
            colour = "red" if prog.plan.action == "DELETE" else "yellow"
            extra = f" ({prog.files_copied} files copied)" if prog.plan.action == "MERGE" else ""
            dedupe_bar.update(
                bar_task,
                description=(
                    f"[{colour}]{prog.plan.action}[/{colour}] "
                    f"[bold]{prog.plan.supplier}/{prog.plan.sku}[/bold]{extra}"
                ),
                advance=1,
            )
            total_copied += prog.files_copied

    console.print(
        f"\n[bold]Dedupe complete[/bold] — "
        f"{delete_n} deleted, {merge_n} merged ({total_copied} files copied during merges). "
        f"Re-run diagnose to refresh the report."
    )


@app.command("regroup")
def regroup(
    config_path: Path = typer.Option(
        Path("pipeline.config.toml"),
        envvar="PIPELINE_CONFIG_PATH",
        help="Path to pipeline.config.toml.",
    ),
    root_folder_id: str = typer.Option(
        ..., envvar="GOOGLE_DRIVE_ROOT_FOLDER_ID",
        help="Google Drive parent folder ID (contains category subfolders).",
    ),
    category: str = typer.Option(
        ..., "--category",
        help="Subfolder under the parent root to regroup (materials, upholstery, …).",
    ),
    subdir: str = typer.Option(
        "", "--subdir",
        help="Optional subdir to nest files into: <sku>/<subdir>/1.<ext>. Default: no subdir.",
    ),
    execute: bool = typer.Option(
        False, "--execute",
        help="Apply the moves in Drive. Omit for a dry run.",
    ),
) -> None:
    """
    Regroup a flat-file category into per-SKU folders.

    Converts <category>/<sku>.<ext> → <category>/<sku>/1.<ext>. Use this to
    migrate categories like materials and upholstery from a flat layout into
    the SKU-folder convention so the rest of the pipeline (diagnose, organize,
    optimize, etc.) can operate on them uniformly.

    Layout (flat vs per-supplier) is auto-detected. Idempotent: SKU folders
    that already contain a file are skipped, and re-running after a successful
    migration is a no-op.
    """
    from rich.table import Table

    from asset_sdk.adapters import drive
    from asset_sdk.stages.regroup import build_plan, execute as run_regroup, summarise

    category_folder_id = drive.resolve_category_folder(root_folder_id, category)

    with Progress(SpinnerColumn(), TextColumn("{task.description}"), console=console) as progress:
        t = progress.add_task(f"Scanning {category} drive…", total=None)
        plans = build_plan(category_folder_id, subdir)
        progress.update(t, description=f"Found {len(plans)} files to consider")
        progress.stop_task(t)

    counts = summarise(plans)

    if not plans:
        console.print(f"[yellow]No flat files found under {category}/.[/yellow]")
        return

    PREVIEW = 30
    table = Table(title=f"Regroup plan: {category} ({counts['actionable']} moves, {counts['skipped']} skipped)")
    table.add_column("Supplier", style="cyan")
    table.add_column("File")
    table.add_column("→")
    nest = f"/{subdir}" if subdir else ""
    table.add_column(f"Target (<sku>{nest}/<name>)")
    table.add_column("Note", style="yellow")
    for p in plans[:PREVIEW]:
        note = p.skip_reason or ""
        target = f"{p.sku}{nest}/{p.target_name}" if not p.skip_reason else "—"
        table.add_row(p.supplier or "(flat)", p.file_name, "→", target, note)
    if len(plans) > PREVIEW:
        table.caption = f"… and {len(plans) - PREVIEW} more"
    console.print(table)

    if not execute:
        console.print(
            f"\n[bold]Dry run complete[/bold] — pass --execute to apply "
            f"{counts['actionable']} moves ({counts['skipped']} skipped)."
        )
        return

    if counts["actionable"] == 0:
        console.print("[yellow]Nothing actionable — done.[/yellow]")
        return

    bar = Progress(
        SpinnerColumn(),
        TextColumn("{task.description}"),
        BarColumn(bar_width=30),
        MofNCompleteColumn(),
        console=console,
    )
    moved = errored = skipped = 0
    with bar:
        bar_task = bar.add_task("Regrouping…", total=len(plans), completed=0)
        for prog in run_regroup(plans, subdir):
            p = prog.plan
            if prog.error:
                errored += 1
                bar.update(
                    bar_task,
                    description=f"  [red]err[/red] {p.file_name}: {prog.error}",
                    advance=1,
                )
            elif prog.done:
                moved += 1
                bar.update(
                    bar_task,
                    description=f"  [green]ok[/green] {p.file_name} → {p.sku}{nest}/{p.target_name}",
                    advance=1,
                )
            else:
                skipped += 1
                bar.update(
                    bar_task,
                    description=f"  [dim]skip[/dim] {p.file_name}: {p.skip_reason}",
                    advance=1,
                )

    console.print(
        f"\n[bold]Regroup complete[/bold]  "
        f"({moved} moved, {skipped} skipped, {errored} errored)"
    )


# ---------------------------------------------------------------------------
# generate
# ---------------------------------------------------------------------------

# Asset kinds that the "copy from siblings" strategy supports. Each maps to a
# config key on `paths.input` whose value is the relative path to walk.
_COPY_FROM_SIBLINGS_KINDS = {
    "assembly_instructions": "assembly_instructions",
    "diagram":               "diagram",
    "models_dwg":            "models_dwg",
}


@app.command("generate")
def generate(
    asset_type: str = typer.Option(
        ..., "--type",
        help=("Asset type(s) to generate. Copy-from-siblings supports comma-separated "
              "values and the 'all' shortcut: e.g. 'diagram', 'diagram,models_dwg', "
              "'all'. Render mode is barcode-only. Photos mode is photos-only."),
    ),
    config_path: Path = typer.Option(
        Path("pipeline.config.toml"),
        envvar="PIPELINE_CONFIG_PATH",
        help="Path to pipeline.config.toml.",
    ),
    root_folder_id: str = typer.Option(
        ..., envvar="GOOGLE_DRIVE_ROOT_FOLDER_ID",
        help="Google Drive parent folder ID (contains category subfolders).",
    ),
    sheet_id: str = typer.Option(
        ..., envvar="GOOGLE_SHEETS_MASTER_ID",
        help="Google Sheets file ID.",
    ),
    category: str = typer.Option(
        "products", "--category",
        help="Subfolder under the parent root: products, materials, … (default: products).",
    ),
    tab: Optional[str] = typer.Option(
        None, help="Sheet tab with the SKU list. Defaults to the category name.",
    ),
    report_tab: Optional[str] = typer.Option(
        None, help="Tab to write the report into. Defaults to '<generate.report_tab> - <Type> - <Category>'.",
    ),
    sku_filter: Optional[str] = typer.Option(
        None, "--sku",
        help="Limit to a single SKU. Filters both the dry-run report and the execute set. "
             "Useful for spot-testing one row before running the full batch.",
    ),
    action_filter: Optional[str] = typer.Option(
        None, "--action",
        help="(--type photos) Limit to a single action: 'copy' or 'generate'. "
             "Filters the dry-run report and the execute set.",
    ),
    models: Optional[str] = typer.Option(
        None, "--models",
        help=("(--type photos) Comma-separated list of models tried in order, "
              "cycling on retry. Provider auto-detected from prefix (gpt-* → "
              "OpenAI, gemini-* → Google). Default: see generate.photos.models in config."),
    ),
    quality: Optional[str] = typer.Option(
        None, "--quality",
        help="(--type photos) Override generate.photos.quality: low | medium | high.",
    ),
    cost_per_image: Optional[float] = typer.Option(
        None, "--cost-per-image",
        help="(--type photos) Override the per-image cost used for estimates. "
             "Without this flag, cost auto-derives from --quality.",
    ),
    budget: Optional[float] = typer.Option(
        None, "--budget",
        help="(--type photos --execute) Hard pre-flight cost ceiling in USD "
             "(worst-case incl. retries). Run aborts before any API call if exceeded.",
    ),
    no_verify: bool = typer.Option(
        False, "--no-verify",
        help="(--type photos --execute) Skip the Claude-vision QA loop. Faster + "
             "cheaper but no automatic retries on visibly-wrong outputs.",
    ),
    debug: bool = typer.Option(
        False, "--debug",
        help="(--type photos --execute) Save every rejected attempt to "
             "<sku>/photos/failed_generations/ on Drive for review. Useful for "
             "diagnosing why a model keeps producing wrong outputs.",
    ),
    max_retries: Optional[int] = typer.Option(
        None, "--max-retries",
        help="(--type photos --execute) Override generate.photos.max_retries (default: 2). "
             "Each retry costs another generation + verification call.",
    ),
    execute: bool = typer.Option(
        False, "--execute",
        help="Read the (possibly edited) report and apply. Omit for a dry run.",
    ),
) -> None:
    """
    Generate missing assets for SKUs in a category.

    Three strategies, dispatched by --type:

      - assembly_instructions / diagram / models_dwg → copy from a sibling SKU
        sharing the same parent_product. Tie-break: first sibling in sheet
        row order that has files.

      - barcode → render a Code-128 label image from the sheet's barcode
        column and upload to <sku>/<barcode-subdir>/<sku>.jpg.

      - photos → for each SKU missing photos, pick a sibling with photos under
        the same parent_product, resolve material reference photos for any
        material columns whose values differ, and call gpt-image-1 to produce
        N outputs (matching sibling's photo count). Cost is estimated in the
        dry-run report; --budget enforces a hard pre-flight ceiling on execute.

    Dry run scans, builds a plan, writes a report tab. Edit that tab as needed
    (change Source SKU, set Action=SKIP, fix barcode strings, etc.) then re-run
    with --execute to apply.
    """
    from rich.table import Table

    from asset_sdk.adapters import drive
    from asset_sdk.adapters.sheets import read_rows, write_report
    from asset_sdk.config import PipelineConfig

    cfg = PipelineConfig.load(config_path)

    # Parse --type FIRST so a bad value fails fast before any Drive calls.
    requested = [t.strip().lower() for t in asset_type.split(",") if t.strip()]
    if requested == ["all"]:
        requested = sorted(_COPY_FROM_SIBLINGS_KINDS.keys())
    if not requested:
        raise typer.BadParameter("--type cannot be empty.")

    # Validate --action up front. Stored as upper-case for plan comparison.
    action_filter_norm: Optional[str] = None
    if action_filter is not None:
        af = action_filter.strip().lower()
        if af not in ("copy", "generate"):
            raise typer.BadParameter(
                f"--action must be 'copy' or 'generate' (got {action_filter!r})"
            )
        action_filter_norm = af.upper()

    sku_filter_norm: Optional[str] = (sku_filter.strip() if sku_filter else None) or None

    def _apply_filters(plans_list):
        """Filter a plan list by --sku / --action. No-op if neither flag set."""
        out = plans_list
        if sku_filter_norm is not None:
            out = [p for p in out if p.sku == sku_filter_norm]
        if action_filter_norm is not None:
            out = [p for p in out if p.action == action_filter_norm]
        return out

    # Barcode and photos are exclusive single-type modes (their report shapes differ
    # from each other and from the copy-from-siblings report).
    if "barcode" in requested:
        if len(requested) > 1:
            raise typer.BadParameter(
                "--type barcode cannot be combined with other types (different report shape)."
            )
        type_label = "Barcode"
    elif "photos" in requested:
        if len(requested) > 1:
            raise typer.BadParameter(
                "--type photos cannot be combined with other types (different report shape)."
            )
        type_label = "Photos"
    else:
        unsupported = [t for t in requested if t not in _COPY_FROM_SIBLINGS_KINDS]
        if unsupported:
            raise typer.BadParameter(
                f"Unsupported --type values: {', '.join(unsupported)}. "
                f"Supported: barcode, photos, "
                f"{', '.join(sorted(_COPY_FROM_SIBLINGS_KINDS))}, "
                f"or comma-separated combinations / 'all'."
            )
        if len(requested) == 1:
            type_label = requested[0].replace("_", " ").title()
        elif sorted(requested) == sorted(_COPY_FROM_SIBLINGS_KINDS.keys()):
            type_label = "All"
        else:
            type_label = " + ".join(t.replace("_", " ").title() for t in requested)

    category_folder_id = drive.resolve_category_folder(root_folder_id, category)
    structure = cfg.structure_for(category)
    _tab = tab or category
    _report_tab = report_tab or f"{cfg.generate.report_tab} - {type_label} - {category.title()}"

    # ------------------ BARCODE ------------------
    if requested == ["barcode"]:
        from asset_sdk.stages.generate_barcodes import (
            build_plan as bp_barcode,
            execute as run_barcode,
            summarise as sum_barcode,
            to_sheet_rows as rows_barcode,
        )
        barcode_subdir = cfg.paths.barcode

        if not execute:
            with Progress(SpinnerColumn(), TextColumn("{task.description}"), console=console) as progress:
                t = progress.add_task(f"Reading '{_tab}' tab…", total=None)
                sheet_rows = read_rows(sheet_id, _tab)
                progress.update(t, description=f"Read {len(sheet_rows)} rows from '{_tab}'")
                progress.stop_task(t)

                t = progress.add_task(f"Scanning {category} drive for existing barcodes…", total=None)
                plans = bp_barcode(
                    category_folder_id, structure, sheet_rows,
                    cfg.csv.sku_column, cfg.csv.supplier_column, cfg.csv.barcode_column,
                    barcode_subdir,
                )
                progress.update(t, description=f"Built plan: {len(plans)} entries")
                progress.stop_task(t)

                # Apply --sku / --action filters before writing the report.
                plans = _apply_filters(plans)

                t = progress.add_task(f"Writing report to '{_report_tab}'…", total=None)
                headers, rows = rows_barcode(plans)
                write_report(sheet_id, _report_tab, headers, rows)
                progress.update(t, description=f"Report written → '{_report_tab}'")
                progress.stop_task(t)

            counts = sum_barcode(plans)
            console.print()
            console.print(
                f"[bold]Dry run complete[/bold] — {counts['to_generate']} barcodes to generate, "
                f"{counts['skipped']} skipped."
            )
            console.print(
                f"\nReview '{_report_tab}', edit any wrong barcodes / actions, "
                "then re-run with --execute."
            )
            return

        # ----- Execute -----
        with Progress(SpinnerColumn(), TextColumn("{task.description}"), console=console) as progress:
            t = progress.add_task(f"Reading report '{_report_tab}'…", total=None)
            report_rows = read_rows(sheet_id, _report_tab)
            progress.update(t, description=f"Read {len(report_rows)} rows from '{_report_tab}'")
            progress.stop_task(t)

        # Apply --sku / --action filters to the report rows before execute.
        if sku_filter_norm:
            report_rows = [r for r in report_rows if (r.get("SKU") or "").strip() == sku_filter_norm]
        if action_filter_norm:
            report_rows = [
                r for r in report_rows
                if (r.get("Action") or "").strip().upper() == action_filter_norm
            ]

        actionable = sum(1 for r in report_rows if (r.get("Action") or "").strip().upper() == "GENERATE")
        if actionable == 0:
            console.print("[yellow]Nothing to generate.[/yellow]")
            return

        bar = Progress(
            SpinnerColumn(),
            TextColumn("{task.description}"),
            BarColumn(bar_width=30),
            MofNCompleteColumn(),
            console=console,
        )
        generated = errored = 0
        with bar:
            bar_task = bar.add_task("Generating barcodes…", total=actionable, completed=0)
            for prog in run_barcode(report_rows, category_folder_id, structure, barcode_subdir):
                if prog.error:
                    errored += 1
                    bar.update(
                        bar_task,
                        description=f"  [red]err[/red] {prog.sku}: {prog.error}",
                        advance=1,
                    )
                else:
                    generated += 1
                    bar.update(
                        bar_task,
                        description=f"  [green]ok[/green] {prog.sku} ({prog.barcode})",
                        advance=1,
                    )

        console.print(
            f"\n[bold]Barcode generation complete[/bold]  "
            f"({generated} generated, {errored} errored)"
        )
        return

    # ------------------ PHOTOS ------------------
    if requested == ["photos"]:
        from asset_sdk.stages.generate_photos import (
            build_plan as bp_photos,
            execute as run_photos,
            summarise as sum_photos,
            to_sheet_rows as rows_photos,
        )
        photos_cfg = cfg.generate.photos
        _quality = (quality or photos_cfg.quality).strip().lower()
        if _quality not in ("low", "medium", "high"):
            raise typer.BadParameter(
                f"--quality must be low | medium | high (got {_quality!r})"
            )
        # Resolve models list: CLI override → config default. Validates the
        # provider prefix per entry so a typo fails fast.
        if models:
            _models = [m.strip() for m in models.split(",") if m.strip()]
        else:
            _models = list(photos_cfg.models)
        if not _models:
            raise typer.BadParameter("--models must list at least one model")
        from asset_sdk.stages.generate_photos import _provider_for as _detect_provider
        for m in _models:
            try:
                _detect_provider(m)
            except Exception as exc:
                raise typer.BadParameter(str(exc))
        # Per-model pricing: primary cost = first model in cycle; worst case
        # cycles through all models (1 + max_retries attempts). --cost-per-image
        # overrides both with a flat value (legacy behavior).
        from asset_sdk.config import _QUALITY_COST_USD as _Q_COST
        # Update cost_per_image_usd in case --quality differs from the config default.
        photos_cfg.cost_per_image_usd = _Q_COST.get(_quality, photos_cfg.cost_per_image_usd)
        _verify_enabled = photos_cfg.verify_enabled and not no_verify
        _max_retries = max_retries if max_retries is not None else photos_cfg.max_retries
        if _max_retries < 0:
            raise typer.BadParameter("--max-retries must be >= 0")
        if cost_per_image is not None:
            _primary_cost = float(cost_per_image)
            _worst_per_image = _primary_cost * (1 + _max_retries)
        else:
            _primary_cost = photos_cfg.cost_for(_models[0])
            _worst_per_image = photos_cfg.worst_case_cost_per_image(_models, _max_retries)
        _cost_per = _primary_cost  # used by build_plan for the per-row cost column
        photos_subdir = cfg.paths.product_photos

        # Read the sheet + build plans (used by both dry-run and execute).
        with Progress(SpinnerColumn(), TextColumn("{task.description}"), console=console) as progress:
            t = progress.add_task(f"Reading '{_tab}' tab…", total=None)
            sheet_rows = read_rows(sheet_id, _tab)
            progress.update(t, description=f"Read {len(sheet_rows)} rows from '{_tab}'")
            progress.stop_task(t)

            t = progress.add_task(f"Scanning {category} drive + resolving materials…", total=None)
            plans = bp_photos(
                parent_root_id=root_folder_id,
                category_folder_id=category_folder_id,
                structure=structure,
                sheet_rows=sheet_rows,
                sku_col=cfg.csv.sku_column,
                supplier_col=cfg.csv.supplier_column,
                parent_product_col=cfg.csv.parent_product_column,
                photos_subdir=photos_subdir,
                material_columns=photos_cfg.material_columns,
                cost_per_image_usd=_cost_per,
                part_col=cfg.csv.part_column,
                size_col=cfg.csv.size_column,
            )
            progress.update(t, description=f"Built plan: {len(plans)} entries")
            progress.stop_task(t)

        # Apply --sku / --action filters. Affects both the dry-run report and execute.
        plans = _apply_filters(plans)
        if sku_filter_norm or action_filter_norm:
            console.print(
                f"[dim]Filtered to {len(plans)} entries[/dim]"
                + (f" (sku={sku_filter_norm!r})" if sku_filter_norm else "")
                + (f" (action={action_filter_norm!r})" if action_filter_norm else "")
            )

        counts = sum_photos(plans)

        if not execute:
            with Progress(SpinnerColumn(), TextColumn("{task.description}"), console=console) as progress:
                t = progress.add_task(f"Writing report to '{_report_tab}'…", total=None)
                headers, rows = rows_photos(plans, photos_cfg.material_columns)
                write_report(sheet_id, _report_tab, headers, rows)
                progress.update(t, description=f"Report written → '{_report_tab}'")
                progress.stop_task(t)

            console.print()
            # Cost ranges: best case = 1 attempt per image; worst case = 1 + max_retries.
            best_case = counts['total_cost_usd']
            # Per-model worst-case (cycles through models on retry — Gemini and
            # GPT may have different costs).
            worst_case = round(counts['total_images'] * _worst_per_image, 4)
            verify_note = (
                f", verifier={photos_cfg.verify_model}, retries up to {_max_retries}"
                if _verify_enabled else ", verifier OFF"
            )
            console.print(
                f"[bold]Dry run complete[/bold] — "
                f"{counts['to_copy']} SKUs to copy ({counts['copy_images']} images, $0), "
                f"{counts['to_generate']} SKUs to generate "
                f"({counts['total_images']} images, ~${best_case:.2f} best case "
                f"/ ~${worst_case:.2f} worst case{verify_note}), "
                f"{counts['skipped']} skipped."
            )
            console.print(
                f"\nReview '{_report_tab}' (COPY rows are listed first, then GENERATE). "
                "Edit Action/Source SKU/material columns to override, then re-run with "
                "--execute (and --budget to set a hard cost cap; pass --no-verify to skip the QA loop)."
            )
            return

        # ----- Execute -----
        # Re-read the report so user edits to Action / Source SKU / material columns are honored.
        with Progress(SpinnerColumn(), TextColumn("{task.description}"), console=console) as progress:
            t = progress.add_task(f"Reading report '{_report_tab}'…", total=None)
            report_rows = read_rows(sheet_id, _report_tab)
            progress.update(t, description=f"Read {len(report_rows)} rows from '{_report_tab}'")
            progress.stop_task(t)

        # Honor user edits to the Action column. Both COPY and GENERATE are actionable.
        actionable_skus = {
            (r.get("SKU") or "").strip()
            for r in report_rows
            if (r.get("Action") or "").strip().upper() in ("COPY", "GENERATE")
        }
        runnable = [
            p for p in plans
            if p.action in ("COPY", "GENERATE") and p.sku in actionable_skus
        ]
        # Run COPYs before GENERATEs: cheap operations finish first, no API risk.
        runnable.sort(key=lambda p: 0 if p.action == "COPY" else 1)

        runnable_copy_count = sum(1 for p in runnable if p.action == "COPY")
        runnable_gen_count = sum(1 for p in runnable if p.action == "GENERATE")
        runnable_copy_images = sum(p.photo_count for p in runnable if p.action == "COPY")
        runnable_gen_images = sum(p.photo_count for p in runnable if p.action == "GENERATE")
        runnable_best_cost = round(sum(p.cost_usd for p in runnable), 4)  # COPY rows are 0
        # Worst-case sums per-attempt costs across the model cycle (Gemini + GPT
        # may differ in cost), so this is more accurate than `best × (1+retries)`.
        runnable_worst_cost = round(runnable_gen_images * _worst_per_image, 4)
        runnable_image_count = runnable_copy_images + runnable_gen_images

        if not runnable:
            console.print("[yellow]Nothing to do (no rows with Action=COPY or GENERATE matched).[/yellow]")
            return

        # Pre-flight budget check uses worst-case cost (every shot retries N times)
        # so the budget is a hard ceiling, not a maybe.
        if budget is not None and runnable_worst_cost > float(budget):
            console.print(
                f"[red]Aborted:[/red] worst-case AI cost "
                f"${runnable_worst_cost:.2f} (best ${runnable_best_cost:.2f}) "
                f"exceeds --budget ${float(budget):.2f}. "
                f"({runnable_gen_count} GENERATE SKUs / {runnable_gen_images} images, "
                f"max {_max_retries} retries each.)"
            )
            console.print(
                "Set Action=SKIP on GENERATE rows you don't want, lower --max-retries, or raise --budget."
            )
            raise typer.Exit(1)

        verify_status = "ON" if _verify_enabled else "OFF"
        console.print(
            f"[bold]Running[/bold] {runnable_copy_count} COPYs ({runnable_copy_images} files) + "
            f"{runnable_gen_count} GENERATEs ({runnable_gen_images} images, "
            f"~${runnable_best_cost:.2f}–${runnable_worst_cost:.2f}, "
            f"models={_models}, quality={_quality}, "
            f"verify={verify_status}, retries={_max_retries})."
        )

        # Stage emits its own per-step logs via `logger=console.print`. We don't
        # use a progress bar here because line-by-line output (prompt, generating,
        # verifying, upload) is what the user actually needs to debug failures.
        # `prog` events from the stage are summarized after the stream ends.
        generated = errored = skipped = 0
        for prog in run_photos(
            plans=runnable,
            parent_root_id=root_folder_id,
            category_folder_id=category_folder_id,
            structure=structure,
            photos_subdir=photos_subdir,
            material_columns=photos_cfg.material_columns,
            models=_models,
            quality=_quality,
            size=photos_cfg.size,
            verify_enabled=_verify_enabled,
            verify_model=photos_cfg.verify_model,
            max_retries=_max_retries,
            debug=debug,
            logger=console.print,
        ):
            if prog.error:
                if prog.skipped:
                    skipped += 1
                else:
                    errored += 1
            else:
                generated += 1

        console.print(
            f"\n[bold]Photo generation complete[/bold]  "
            f"({generated} generated, {skipped} skipped, {errored} errored)"
        )
        return

    # ------------------ COPY FROM SIBLINGS (one or many kinds) ------------------
    # Build {type_label: rel_path} for the kinds we'll process.
    kind_paths = {
        kind: getattr(cfg.paths, _COPY_FROM_SIBLINGS_KINDS[kind])
        for kind in requested
    }

    from asset_sdk.stages.generate_from_siblings import (
        _build_sku_index,
        build_plan as bp_copy,
        execute as run_copy,
        summarise as sum_copy,
        to_sheet_rows as rows_copy,
    )

    if not execute:
        with Progress(SpinnerColumn(), TextColumn("{task.description}"), console=console) as progress:
            t = progress.add_task(f"Reading '{_tab}' tab…", total=None)
            sheet_rows = read_rows(sheet_id, _tab)
            progress.update(t, description=f"Read {len(sheet_rows)} rows from '{_tab}'")
            progress.stop_task(t)

            t = progress.add_task(f"Indexing {category} drive…", total=None)
            sku_index = _build_sku_index(category_folder_id, structure)
            progress.update(t, description=f"Indexed {len(sku_index)} SKU folders")
            progress.stop_task(t)

            plans = []
            for kind, kind_path in kind_paths.items():
                t = progress.add_task(f"Scanning for {kind_path}/ across SKUs…", total=None)
                plans.extend(bp_copy(
                    category_folder_id, structure, sheet_rows,
                    cfg.csv.sku_column, cfg.csv.supplier_column, cfg.csv.parent_product_column,
                    kind, kind_path, sku_index=sku_index,
                    part_col=cfg.csv.part_column,
                ))
                progress.update(t, description=f"Built plan for {kind}")
                progress.stop_task(t)

            # Apply --sku / --action filters before writing the report.
            plans = _apply_filters(plans)

            t = progress.add_task(f"Writing report to '{_report_tab}'…", total=None)
            headers, rows = rows_copy(plans)
            write_report(sheet_id, _report_tab, headers, rows)
            progress.update(t, description=f"Report written → '{_report_tab}'")
            progress.stop_task(t)

        counts = sum_copy(plans)
        console.print()
        console.print(
            f"[bold]Dry run complete[/bold] — {counts['to_copy']} copies planned across "
            f"{len(kind_paths)} type(s), {counts['skipped']} skipped."
        )
        console.print(
            f"\nReview '{_report_tab}', edit any Source SKU / Type / Action you want to "
            "override, then re-run with --execute."
        )
        return

    # ----- Execute -----
    with Progress(SpinnerColumn(), TextColumn("{task.description}"), console=console) as progress:
        t = progress.add_task(f"Reading report '{_report_tab}'…", total=None)
        report_rows = read_rows(sheet_id, _report_tab)
        progress.update(t, description=f"Read {len(report_rows)} rows from '{_report_tab}'")
        progress.stop_task(t)

    # Apply --sku / --action filters to the report rows before execute.
    if sku_filter_norm:
        report_rows = [r for r in report_rows if (r.get("SKU") or "").strip() == sku_filter_norm]
    if action_filter_norm:
        report_rows = [
            r for r in report_rows
            if (r.get("Action") or "").strip().upper() == action_filter_norm
        ]

    actionable = sum(1 for r in report_rows if (r.get("Action") or "").strip().upper() == "COPY")
    if actionable == 0:
        console.print("[yellow]Nothing to copy.[/yellow]")
        return

    bar = Progress(
        SpinnerColumn(),
        TextColumn("{task.description}"),
        BarColumn(bar_width=30),
        MofNCompleteColumn(),
        console=console,
    )
    copied = skipped = errored = 0
    current_key: tuple[str, str] | None = None
    with bar:
        # Total advances once per (sku, kind) actionable group, not per file.
        bar_task = bar.add_task("Copying…", total=actionable, completed=0)
        for prog in run_copy(report_rows, category_folder_id, structure, kind_paths):
            label = "skip" if prog.skipped else "ok"
            colour = "yellow" if prog.skipped else "green"
            if prog.error:
                colour = "red"
                label = "err"
                errored += 1
            elif prog.skipped:
                skipped += 1
            else:
                copied += 1

            key = (prog.sku, prog.kind)
            advance = 1 if current_key != key else 0
            current_key = key
            extra = f": {prog.error}" if prog.error else ""
            bar.update(
                bar_task,
                description=(
                    f"  [{colour}]{label}[/{colour}] "
                    f"[{prog.kind}] {prog.sku} ← {prog.source_sku} "
                    f"({prog.file_index}/{prog.file_total}){extra}"
                ),
                advance=advance,
            )

    console.print(
        f"\n[bold]Copy complete[/bold]  "
        f"({copied} files copied, {skipped} skipped, {errored} errored)"
    )


def _optimize_models(
    cfg,
    sku_filter,
    supplier_filter,
    output_subdir,
    report_tab,
    sheet_id,
    category_folder_id,
    category,
    execute,
):
    """Dispatched from `optimize --type model`."""
    from rich.table import Table

    from asset_sdk.adapters.sheets import write_report
    from asset_sdk.stages.optimize_models import (
        analyze as analyze_models,
        execute as run_models,
        find_targets as find_model_targets,
        to_sheet_rows as model_rows,
    )

    src_subdir = "models/obj"
    dest_subdir = output_subdir or cfg.optimize.model_dest_subdir
    _report_tab = report_tab or f"{cfg.optimize.report_tab} - model - {category.title()}"

    with Progress(SpinnerColumn(), TextColumn("{task.description}"), console=console) as progress:
        t = progress.add_task(f"Scanning {category} drive for models…", total=None)
        targets = find_model_targets(
            category_folder_id, cfg.structure_for(category), src_subdir, dest_subdir,
            sku_filter, supplier_filter,
        )
        progress.update(t, description=f"Found {len(targets)} SKUs with OBJ files")
        progress.stop_task(t)

    if not targets:
        console.print("[yellow]No SKUs with an OBJ in models/obj/ found.[/yellow]")
        return

    console.print(
        f"[bold]Settings[/bold]: textures→{cfg.optimize.model_target_texture_px}px, "
        f"decim fine={cfg.optimize.model_decim_target_fine}, "
        f"med={cfg.optimize.model_decim_target_med}, "
        f"coarse={cfg.optimize.model_decim_target_coarse}, "
        f"dest='{dest_subdir}/'"
    )

    if not execute:
        analyses = []
        errors: list[str] = []
        bar = Progress(
            SpinnerColumn(),
            TextColumn("{task.description}"),
            BarColumn(bar_width=30),
            MofNCompleteColumn(),
            console=console,
        )
        with bar:
            bar_task = bar.add_task("Analyzing models…", total=len(targets), completed=0)
            for tgt, analysis, err in analyze_models(targets, cfg.optimize):
                if analysis is not None:
                    analyses.append(analysis)
                else:
                    errors.append(f"{tgt.supplier}/{tgt.sku}/{tgt.obj_file['name']}: {err}")
                bar.update(
                    bar_task,
                    description=f"  [bold]{tgt.supplier}/{tgt.sku}[/bold]  {tgt.obj_file['name']}",
                    advance=1,
                )

        with Progress(SpinnerColumn(), TextColumn("{task.description}"), console=console) as progress:
            t = progress.add_task(f"Writing report to '{_report_tab}'…", total=None)
            headers, rows = model_rows(analyses)
            write_report(sheet_id, _report_tab, headers, rows)
            progress.update(t, description=f"Report written → '{_report_tab}'")
            progress.stop_task(t)

        n_dup = sum(1 for a in analyses if a.duplicate_groups)
        n_oversize = sum(1 for a in analyses if a.oversized)
        n_unused = sum(1 for a in analyses if a.unused_textures)

        console.print()
        console.print(
            f"[bold]Dry run complete[/bold]  ({len(analyses)} analyzed, {len(errors)} errors)"
        )
        console.print(f"  [yellow]Duplicate textures[/yellow]  {n_dup} SKUs")
        console.print(f"  [yellow]Oversized textures[/yellow]  {n_oversize} SKUs")
        console.print(f"  [yellow]Unused textures[/yellow]     {n_unused} SKUs")
        for e in errors[:10]:
            console.print(f"  [red]error[/red] {e}")
        if len(errors) > 10:
            console.print(f"  [red]…and {len(errors) - 10} more errors[/red]")
        console.print(
            f"\nReview '{_report_tab}', then re-run with --execute to "
            f"optimize {len(analyses)} models."
        )
        return

    # ----- Execute -----
    table = Table(title=f"Models to optimize ({len(targets)})")
    table.add_column("Supplier")
    table.add_column("SKU")
    table.add_column("OBJ")
    table.add_column("Size", justify="right")
    table.add_column("Textures", justify="right")
    PREVIEW = 30
    for t in targets[:PREVIEW]:
        size_mb = int(t.obj_file.get("size") or 0) / 1024 / 1024
        table.add_row(
            t.supplier, t.sku, t.obj_file["name"],
            f"{size_mb:.1f}MB", str(len(t.texture_files)),
        )
    if len(targets) > PREVIEW:
        table.caption = f"… and {len(targets) - PREVIEW} more"
    console.print(table)

    bar = Progress(
        SpinnerColumn(),
        TextColumn("{task.description}"),
        BarColumn(bar_width=30),
        MofNCompleteColumn(),
        console=console,
    )
    completed = errored = 0
    with bar:
        bar_task = bar.add_task("Optimizing models…", total=len(targets), completed=0)
        current_sku: str | None = None
        for p in run_models(targets, cfg.optimize):
            label = f"  [bold]{p.supplier}/{p.sku}[/bold] ({p.sku_index}/{p.sku_total})"
            if p.stage == "error":
                bar.update(bar_task, description=f"{label}  [red]error[/red]: {p.error}")
                errored += 1
                bar.advance(bar_task)
                current_sku = p.sku
            elif p.stage == "done":
                completed += 1
                bar.advance(bar_task)
                current_sku = p.sku
            else:
                bar.update(
                    bar_task,
                    description=f"{label}  {p.stage}{(': ' + p.detail) if p.detail else ''}",
                )

    console.print(
        f"\n[bold]Model optimization complete[/bold]  "
        f"({completed} optimized, {errored} errored)"
    )


@app.command("optimize")
def optimize(
    asset_type: str = typer.Option(
        "photo", "--type",
        help="Asset type to optimize. photo / lifestyle (image pipeline) or model (3D pipeline).",
    ),
    sku_filter: Optional[str] = typer.Option(
        None, "--sku", help="Optimize only this SKU.",
    ),
    supplier_filter: Optional[str] = typer.Option(
        None, "--supplier", help="Optimize only SKUs under this supplier.",
    ),
    output_subdir: Optional[str] = typer.Option(
        None, "--output-subdir",
        help="Destination subfolder name. Defaults to '<src_subdir><suffix>' from config.",
    ),
    config_path: Path = typer.Option(
        Path("pipeline.config.toml"),
        envvar="PIPELINE_CONFIG_PATH",
        help="Path to pipeline.config.toml.",
    ),
    root_folder_id: str = typer.Option(
        ..., envvar="GOOGLE_DRIVE_ROOT_FOLDER_ID",
        help="Google Drive parent folder ID (contains category subfolders).",
    ),
    sheet_id: str = typer.Option(
        ..., envvar="GOOGLE_SHEETS_MASTER_ID",
        help="Google Sheets file ID (where the dry-run report is written).",
    ),
    category: str = typer.Option(
        "products", "--category",
        help="Subfolder under the parent root: products, materials, … (default: products).",
    ),
    report_tab: Optional[str] = typer.Option(
        None,
        help="Report tab for the dry-run analysis. Defaults to '<optimize.report_tab> - <Category>'.",
    ),
    target_size: Optional[int] = typer.Option(None, help="Override target_size from config."),
    quality: Optional[int] = typer.Option(None, help="Override JPG quality (0-95)."),
    execute: bool = typer.Option(
        False, "--execute",
        help="Download/optimize/upload. Omit for a dry run with sheet report.",
    ),
) -> None:
    """
    Standardise product images: convert to JPG, clean near-white background to pure
    white, recenter on a square canvas with consistent padding, resize to a target
    dimension, and cap file size. Output goes to a sibling subfolder so originals
    are preserved (configurable via --output-subdir or [optimize] config).
    """
    from rich.table import Table

    from asset_sdk.adapters import drive
    from asset_sdk.adapters.sheets import write_report
    from asset_sdk.config import PipelineConfig
    from asset_sdk.stages.upload_local_files import resolve_type_subdir

    cfg = PipelineConfig.load(config_path)
    category_folder_id = drive.resolve_category_folder(root_folder_id, category)
    opt_cfg = cfg.optimize

    # Dispatch model pipeline early — it has different download/optimize semantics.
    if asset_type.lower() in ("model", "models", "obj"):
        _optimize_models(
            cfg, sku_filter, supplier_filter, output_subdir, report_tab,
            sheet_id, category_folder_id, category, execute,
        )
        return

    src_subdir = resolve_type_subdir(asset_type, cfg.paths)
    dest_subdir = output_subdir or f"{src_subdir}{cfg.optimize.output_subdir_suffix}"
    _report_tab = report_tab or f"{cfg.optimize.report_tab} - {category.title()}"

    if target_size is not None:
        opt_cfg.target_size = target_size
    if quality is not None:
        opt_cfg.jpg_quality = quality

    from asset_sdk.stages.optimize_photos import (
        analyze,
        execute as run_optimize,
        find_targets,
        to_sheet_rows,
    )

    with Progress(SpinnerColumn(), TextColumn("{task.description}"), console=console) as progress:
        t = progress.add_task(f"Scanning {category} drive…", total=None)
        targets = find_targets(
            category_folder_id, cfg.structure_for(category), src_subdir, dest_subdir,
            sku_filter, supplier_filter,
        )
        total_files = sum(len(t.files) for t in targets)
        progress.update(
            t, description=f"Found {total_files} images across {len(targets)} SKUs",
        )
        progress.stop_task(t)

    if not targets:
        console.print("[yellow]Nothing to optimize.[/yellow]")
        return

    console.print(
        f"[bold]Settings[/bold]: target={opt_cfg.target_size}px, "
        f"padding={opt_cfg.target_padding_pct}%, quality={opt_cfg.jpg_quality}, "
        f"max={opt_cfg.max_file_mb}MB, dest='{dest_subdir}/'"
    )

    if not execute:
        # Dry run: download each image, analyze it, write a report to the sheet.
        analyses: list = []
        errors: list[str] = []

        analyze_bar = Progress(
            SpinnerColumn(),
            TextColumn("{task.description}"),
            BarColumn(bar_width=30),
            MofNCompleteColumn(),
            console=console,
        )
        with analyze_bar:
            bar_task = analyze_bar.add_task(
                "Analyzing images…", total=total_files, completed=0,
            )
            for tgt, file_meta, analysis, error in analyze(targets, opt_cfg):
                if analysis is not None:
                    analyses.append(analysis)
                else:
                    errors.append(f"{tgt.supplier}/{tgt.sku}/{file_meta['name']}: {error}")
                analyze_bar.update(
                    bar_task,
                    description=f"  [bold]{tgt.supplier}/{tgt.sku}[/bold]  {file_meta['name']}",
                    advance=1,
                )

        with Progress(SpinnerColumn(), TextColumn("{task.description}"), console=console) as progress:
            t = progress.add_task(f"Writing report to '{_report_tab}'…", total=None)
            headers, rows = to_sheet_rows(analyses, opt_cfg)
            write_report(
                sheet_id, _report_tab, headers, rows,
                value_input_option="USER_ENTERED",
                row_height_px=120,
            )
            progress.update(t, description=f"Report written → '{_report_tab}'")
            progress.stop_task(t)

        n_bg = sum(1 for a in analyses if a.has_background)
        n_dup = sum(1 for a in analyses if a.is_duplicate_of)
        n_non_square = sum(1 for a in analyses if a.aspect_label != "1:1")
        n_format = sum(1 for a in analyses if a.current_format != "JPEG")
        total_current_mb = sum(a.current_size_bytes for a in analyses) / 1024 / 1024
        total_target_mb = sum(a.target_size_bytes for a in analyses) / 1024 / 1024

        console.print()
        console.print(
            f"[bold]Dry run complete[/bold]  ({len(analyses)} analyzed, "
            f"{len(errors)} errors)"
        )
        console.print(f"  [yellow]Has background[/yellow]    {n_bg}")
        console.print(f"  [yellow]Non-square aspect[/yellow] {n_non_square}")
        console.print(f"  [yellow]Non-JPEG format[/yellow]   {n_format}")
        console.print(f"  [magenta]Duplicates[/magenta]      {n_dup}")
        console.print(
            f"  [green]Total size[/green]      {total_current_mb:.1f}MB → {total_target_mb:.1f}MB"
        )
        for e in errors[:10]:
            console.print(f"  [red]error[/red] {e}")
        if len(errors) > 10:
            console.print(f"  [red]…and {len(errors) - 10} more errors[/red]")
        console.print(
            f"\nReview '{_report_tab}' (edit Remove BG / Pad to Square / etc. as needed), "
            f"then re-run with --execute."
        )
        return

    # ----- Execute -----
    # Read back the (possibly-edited) report and apply per-row action flags.
    with Progress(SpinnerColumn(), TextColumn("{task.description}"), console=console) as progress:
        t = progress.add_task(f"Reading report '{_report_tab}'…", total=None)
        report_rows = read_rows(sheet_id, _report_tab)
        progress.update(t, description=f"Read {len(report_rows)} rows from '{_report_tab}'")
        progress.stop_task(t)

    actionable = [
        r for r in report_rows
        if not (r.get("Duplicate Of") or "").strip()
        and any(
            (r.get(c) or "").strip().upper() in ("TRUE", "YES", "Y", "1", "T")
            for c in ("Convert JPG", "Remove BG", "Pad to Square", "Resize", "Compress")
        )
    ]

    table = Table(title=f"Optimization plan ({len(actionable)} actionable / {len(report_rows)} rows)")
    table.add_column("Supplier")
    table.add_column("SKU")
    table.add_column("Images", justify="right")
    table.add_column("Source")
    table.add_column("→")
    table.add_column("Output")
    PREVIEW = 30
    for t in targets[:PREVIEW]:
        table.add_row(t.supplier, t.sku, str(len(t.files)),
                      f"{t.src_subdir}/", "→", f"{t.dest_subdir}/")
    if len(targets) > PREVIEW:
        table.caption = f"… and {len(targets) - PREVIEW} more SKUs"
    console.print(table)

    if not actionable:
        console.print("[yellow]Nothing to do (no rows with TRUE action flags).[/yellow]")
        return

    bar = Progress(
        SpinnerColumn(),
        TextColumn("{task.description}"),
        BarColumn(bar_width=30),
        MofNCompleteColumn(),
        console=console,
    )
    processed = skipped = 0
    with bar:
        bar_task = bar.add_task("Optimizing…", total=len(report_rows), completed=0)
        for p in run_optimize(report_rows, targets, opt_cfg):
            label = "skip" if p.skipped else "ok"
            bar.update(
                bar_task,
                description=(
                    f"[bold]{p.supplier}/{p.sku}[/bold] "
                    f"({p.sku_index}/{p.sku_total})  {label}: {p.file_name}"
                ),
                advance=1,
            )
            if p.skipped:
                skipped += 1
            else:
                processed += 1

    console.print(
        f"\n[bold]Optimization complete[/bold]  "
        f"({processed} optimized, {skipped} already done)"
    )


@app.command("scaffold")
def scaffold(
    config_path: Path = typer.Option(
        Path("pipeline.config.toml"),
        envvar="PIPELINE_CONFIG_PATH",
        help="Path to pipeline.config.toml.",
    ),
    root_folder_id: str = typer.Option(
        ..., envvar="GOOGLE_DRIVE_ROOT_FOLDER_ID",
        help="Google Drive parent folder ID (contains category subfolders).",
    ),
    sheet_id: str = typer.Option(
        ..., envvar="GOOGLE_SHEETS_MASTER_ID",
        help="Google Sheets file ID.",
    ),
    category: str = typer.Option(
        "products", "--category",
        help="Subfolder under the parent root: products, materials, … (default: products).",
    ),
    moved_folder_id: Optional[str] = typer.Option(
        None, envvar="GOOGLE_DRIVE_MOVED_FOLDER_ID",
        help="Drive folder ID to quarantine non-canonical dirs into (with --clean --move). "
             "If empty, scaffold creates/finds scaffold.moved_folder_name under the category root.",
    ),
    tab: Optional[str] = typer.Option(
        None, help="Sheet tab with the SKU list. Defaults to the category name.",
    ),
    report_tab: Optional[str] = typer.Option(
        None, help="Tab to write the scaffold report into. Defaults to '<scaffold.report_tab> - <Category>'.",
    ),
    fix: bool = typer.Option(
        False, "--fix",
        help="Fix loose files (move into right subdir), rename typo'd folders, flag duplicates.",
    ),
    clean: bool = typer.Option(
        False, "--clean",
        help="Delete junk files (.DS_Store etc.) and remove non-canonical directories.",
    ),
    move_unknown: bool = typer.Option(
        False, "--move",
        help="With --clean: move non-canonical dirs into MOVED_FOLDER instead of deleting.",
    ),
    internal_dirs: Optional[str] = typer.Option(
        None, "--internal-dirs",
        help="Comma-separated list of subdir paths to scaffold inside each SKU "
             "(e.g. 'photos' or 'photos,lifestyle,models/dwg'). When omitted, the "
             "full canonical set from paths.input is scaffolded. Use this for "
             "categories like upholstery that only need a photos folder.",
    ),
    execute: bool = typer.Option(
        False, "--execute",
        help="Apply the planned actions in Drive. Omit for a dry run.",
    ),
) -> None:
    """
    Scaffold the category drive: create missing SKU folders from the sheet, ensure each
    SKU has the canonical subdir structure from paths.input. Optional --fix moves loose
    files into the right subdir and corrects typo'd folder names. Optional --clean
    deletes junk files and non-canonical dirs (use --move to quarantine them under
    MOVED_FOLDER instead). All flags work with or without --execute.
    """
    from rich.table import Table

    from asset_sdk.adapters import drive
    from asset_sdk.adapters.sheets import read_rows, write_report
    from asset_sdk.config import PipelineConfig
    from asset_sdk.stages.scaffold import (
        build_plan,
        execute as run_scaffold,
        summarise,
        to_sheet_rows,
    )

    cfg = PipelineConfig.load(config_path)

    # Resolve --internal-dirs FIRST so a typo fails fast before the Drive call.
    _internal_dirs: Optional[list[str]] = None
    if internal_dirs is not None:
        _internal_dirs = [d.strip() for d in internal_dirs.split(",") if d.strip()]
        known_paths = {e[2] for e in cfg.paths.entries()}
        unknown = [d for d in _internal_dirs if d not in known_paths]
        if unknown:
            raise typer.BadParameter(
                f"--internal-dirs contains unknown paths: {', '.join(unknown)}. "
                f"Known paths: {', '.join(sorted(known_paths))}"
            )
        console.print(
            f"[dim]--internal-dirs: only scaffolding {_internal_dirs}[/dim]"
        )

    category_folder_id = drive.resolve_category_folder(root_folder_id, category)
    _tab        = tab        or category
    _sku_col    = cfg.csv.sku_column
    _supplier_col = cfg.csv.supplier_column
    _report_tab = report_tab or f"{cfg.scaffold.report_tab} - {category.title()}"

    if move_unknown and not clean:
        console.print("[yellow]--move only applies with --clean. Ignoring.[/yellow]")
        move_unknown = False

    with Progress(SpinnerColumn(), TextColumn("{task.description}"), console=console) as progress:
        t = progress.add_task(f"Reading '{_tab}'…", total=None)
        sheet_rows = read_rows(sheet_id, _tab)
        progress.update(t, description=f"Read {len(sheet_rows)} rows from '{_tab}'")
        progress.stop_task(t)

        t = progress.add_task(f"Scanning {category} drive…", total=None)
        actions = build_plan(
            category_folder_id, sheet_rows, _sku_col, _supplier_col,
            cfg.paths, cfg.structure_for(category),
            fix=fix, clean=clean, move_unknown=move_unknown,
            typo_cutoff=cfg.scaffold.typo_cutoff,
            internal_dirs=_internal_dirs,
        )
        progress.update(t, description=f"Built plan: {len(actions)} actions")
        progress.stop_task(t)

        t = progress.add_task(f"Writing report to '{_report_tab}'…", total=None)
        headers, rows = to_sheet_rows(actions)
        write_report(sheet_id, _report_tab, headers, rows)
        progress.update(t, description=f"Report written → '{_report_tab}'")
        progress.stop_task(t)

    counts = summarise(actions)

    table = Table(title="Scaffold plan summary")
    table.add_column("Action", style="bold")
    table.add_column("Count", justify="right")
    label_colour = {
        "CREATE_SKU":     "green",
        "CREATE_SUBDIR":  "green",
        "RENAME_DIR":     "yellow",
        "MOVE_FILE":      "yellow",
        "DELETE_FILE":    "red",
        "DELETE_DIR":     "red",
        "MOVE_DIR":       "yellow",
        "DUPLICATE_DIR":  "magenta",
    }
    for kind in (
        "CREATE_SKU", "CREATE_SUBDIR", "RENAME_DIR",
        "MOVE_FILE", "DELETE_FILE", "MOVE_DIR", "DELETE_DIR", "DUPLICATE_DIR",
    ):
        if counts.get(kind):
            colour = label_colour.get(kind, "white")
            table.add_row(f"[{colour}]{kind}[/{colour}]", str(counts[kind]))
    console.print(table)

    if not execute:
        actionable = sum(v for k, v in counts.items() if k != "DUPLICATE_DIR")
        console.print(
            f"\n[bold]Dry run complete[/bold] — {actionable} actionable items "
            f"({counts.get('DUPLICATE_DIR', 0)} duplicates flagged). "
            f"Pass --execute to apply."
        )
        return

    actionable_actions = [a for a in actions if a.kind != "DUPLICATE_DIR"]
    if not actionable_actions:
        console.print("[yellow]Nothing actionable — done.[/yellow]")
        return

    bar = Progress(
        SpinnerColumn(),
        TextColumn("{task.description}"),
        BarColumn(bar_width=30),
        MofNCompleteColumn(),
        console=console,
    )
    applied = errored = 0
    with bar:
        bar_task = bar.add_task("Applying…", total=len(actionable_actions), completed=0)
        for prog in run_scaffold(
            actions, category_folder_id, cfg.structure_for(category),
            cfg.scaffold.moved_folder_name,
            moved_folder_id or "",
        ):
            a = prog.action
            if prog.error:
                errored += 1
                bar.update(
                    bar_task,
                    description=f"  [red]{a.kind}[/red] {a.supplier}/{a.sku}: {prog.error}",
                    advance=1,
                )
            else:
                applied += 1
                bar.update(
                    bar_task,
                    description=f"  [green]{a.kind}[/green] {a.supplier}/{a.sku}",
                    advance=1,
                )

    console.print(
        f"\n[bold]Scaffold complete[/bold] — {applied} applied, {errored} errored."
    )


@app.command("permissions")
def permissions(
    asset_type: str = typer.Option(
        ..., "--type",
        help="Subfolder type to target (photo, lifestyle, video, diagram, models_obj, etc.).",
    ),
    access: str = typer.Option(
        ..., "--access",
        help="public  → anyone with the link can view  |  private → remove anyone-link access",
    ),
    sku_filter: Optional[str] = typer.Option(None, "--sku", help="Limit to one SKU."),
    supplier_filter: Optional[str] = typer.Option(
        None, "--supplier", help="Limit to one supplier.",
    ),
    config_path: Path = typer.Option(
        Path("pipeline.config.toml"),
        envvar="PIPELINE_CONFIG_PATH",
        help="Path to pipeline.config.toml.",
    ),
    root_folder_id: str = typer.Option(
        ..., envvar="GOOGLE_DRIVE_ROOT_FOLDER_ID",
        help="Google Drive parent folder ID (contains category subfolders).",
    ),
    category: str = typer.Option(
        "products", "--category",
        help="Subfolder under the parent root: products, materials, … (default: products).",
    ),
    detailed: bool = typer.Option(
        False, "--detailed",
        help="Print one row per file (current → target access) instead of just summary counts.",
    ),
    execute: bool = typer.Option(
        False, "--execute",
        help="Apply the permission changes. Omit for a dry run.",
    ),
) -> None:
    """
    Set Drive file permissions on every file under <category>/<sku>/<type-subdir>/ recursively.

    Examples:
      uv run asset permissions --type photo --access public --execute
      uv run asset permissions --type models_obj --access private --execute
      uv run asset permissions --type lifestyle --access public --supplier mansa --execute
      uv run asset permissions --type photo --access public --detailed
      uv run asset permissions --type photo --access public --category materials --execute

    public  = grants 'anyone with the link can view' to every file
    private = removes any 'anyone' permission, leaving only explicitly-shared users
    """
    from rich.table import Table

    from asset_sdk.adapters import drive
    from asset_sdk.config import PipelineConfig
    from asset_sdk.stages.permissions import (
        execute as run_perms,
        find_targets,
        summarise,
    )
    from asset_sdk.stages.upload_local_files import resolve_type_subdir

    if access not in ("public", "private"):
        raise typer.BadParameter("--access must be 'public' or 'private'")

    cfg = PipelineConfig.load(config_path)
    category_folder_id = drive.resolve_category_folder(root_folder_id, category)
    src_subdir = resolve_type_subdir(asset_type, cfg.paths)

    with Progress(SpinnerColumn(), TextColumn("{task.description}"), console=console) as progress:
        t = progress.add_task(f"Scanning {category}/<sku>/{src_subdir}/ files…", total=None)
        targets = find_targets(
            category_folder_id, cfg.structure_for(category), src_subdir,
            sku_filter, supplier_filter,
        )
        progress.update(t, description=f"Found {len(targets)} files across SKUs")
        progress.stop_task(t)

    if not targets:
        console.print(f"[yellow]No files found under <sku>/{src_subdir}/.[/yellow]")
        return

    counts = summarise(targets, access)

    table = Table(title=f"Permission plan: {access} on <sku>/{src_subdir}/ ({counts['total']} files)")
    table.add_column("Action", style="bold")
    table.add_column("Count", justify="right")
    if access == "public":
        table.add_row("[green]Make public[/green]", str(counts["to_change"]))
    else:
        table.add_row("[yellow]Make private[/yellow]", str(counts["to_change"]))
    table.add_row("Already in target state", str(counts["no_change"]))
    console.print(table)

    if detailed:
        target_label = "anyone:reader" if access == "public" else "private"
        detail = Table(title=f"Per-file plan ({len(targets)} files)")
        detail.add_column("Supplier", style="cyan")
        detail.add_column("SKU")
        detail.add_column("File")
        detail.add_column("Current")
        detail.add_column("→")
        detail.add_column("Target")
        detail.add_column("Action", style="bold")
        for t in targets:
            current = (
                f"anyone:{t.current_anyone_role}"
                if t.current_anyone_role
                else "private"
            )
            if access == "public":
                action = (
                    "[dim]no change[/dim]"
                    if t.current_anyone_role == "reader"
                    else "[green]make public[/green]"
                )
            else:
                action = (
                    "[dim]no change[/dim]"
                    if t.current_anyone_role is None
                    else "[yellow]make private[/yellow]"
                )
            detail.add_row(
                t.supplier, t.sku, t.file_name,
                current, "→", target_label, action,
            )
        console.print(detail)

    if not execute:
        console.print(
            f"\n[bold]Dry run complete[/bold] — pass --execute to update "
            f"{counts['to_change']} files."
        )
        return

    if counts["to_change"] == 0:
        console.print("\n[green]Nothing to do — all files are already in the target state.[/green]")
        return

    bar = Progress(
        SpinnerColumn(),
        TextColumn("{task.description}"),
        BarColumn(bar_width=30),
        MofNCompleteColumn(),
        console=console,
    )
    changed = unchanged = errored = 0
    with bar:
        bar_task = bar.add_task("Updating…", total=len(targets), completed=0)
        for p in run_perms(targets, access):
            if p.action == "error":
                errored += 1
                bar.update(
                    bar_task,
                    description=f"  [red]error[/red] {p.target.supplier}/{p.target.sku}/{p.target.file_name}: {p.error}",
                    advance=1,
                )
            elif p.action == "no_change":
                unchanged += 1
                bar.update(bar_task, description=f"  skip: {p.target.file_name}", advance=1)
            else:
                changed += 1
                colour = "green" if p.action == "made_public" else "yellow"
                bar.update(
                    bar_task,
                    description=f"  [{colour}]{p.action}[/{colour}] {p.target.supplier}/{p.target.sku}/{p.target.file_name}",
                    advance=1,
                )

    console.print(
        f"\n[bold]Permissions updated[/bold]  "
        f"({changed} changed, {unchanged} unchanged, {errored} errored)"
    )


@app.command("organize")
def organize(
    rename: bool = typer.Option(
        False, "--rename",
        help="Read saved photo orders from the DB and rename files in Drive (1.jpg, 2.jpg, ...).",
    ),
    execute: bool = typer.Option(
        False, "--execute",
        help="With --rename: actually perform the renames. Without: dry run.",
    ),
    api_port: int = typer.Option(8000, help="Port for the FastAPI backend."),
    web_port: int = typer.Option(3000, help="Port for the Next.js dev server."),
    no_browser: bool = typer.Option(False, "--no-browser", help="Don't auto-open the browser."),
    config_path: Path = typer.Option(
        Path("pipeline.config.toml"),
        envvar="PIPELINE_CONFIG_PATH",
        help="Path to pipeline.config.toml.",
    ),
    root_folder_id: Optional[str] = typer.Option(
        None, envvar="GOOGLE_DRIVE_ROOT_FOLDER_ID",
        help="Google Drive parent folder ID (contains category subfolders).",
    ),
    category: str = typer.Option(
        "products", "--category",
        help="Subfolder under the parent root: products, materials, … (default: products).",
    ),
) -> None:
    """
    Run the photo-reorder web app, or apply saved orders to Drive (--rename).

    Without --rename: starts the FastAPI backend and Next.js dev server, then opens a
    browser to the SKU grid. Click a SKU to drag-and-drop photos into the desired order
    and click Save. The order is persisted in the photo_orders table.

    With --rename: reads every saved order from photo_orders and (with --execute) renames
    the files in Drive sequentially as 1.<ext>, 2.<ext>, …
    """
    if rename:
        _organize_rename(config_path, root_folder_id, category, execute)
        return
    _organize_serve(api_port, web_port, no_browser, category)


def _organize_serve(api_port: int, web_port: int, no_browser: bool, category: str) -> None:
    """Start the FastAPI backend + Next.js dev server, open the browser."""
    import shutil
    import signal
    import subprocess
    import time
    import webbrowser

    repo_root = Path(__file__).resolve().parents[4]
    web_dir = repo_root / "apps" / "web"
    if not web_dir.exists():
        console.print(f"[red]apps/web directory not found at {web_dir}[/red]")
        raise typer.Exit(1)

    pnpm = shutil.which("pnpm")
    if not pnpm:
        console.print("[red]pnpm not found on PATH. Install pnpm first.[/red]")
        raise typer.Exit(1)

    # Always run pnpm install (idempotent + cheap; catches new deps in package.json).
    console.print("[bold]Syncing web dependencies (pnpm install)…[/bold]")
    r = subprocess.run([pnpm, "install"], cwd=str(web_dir))
    if r.returncode != 0:
        raise typer.Exit(r.returncode)

    api_proc: subprocess.Popen | None = None
    web_proc: subprocess.Popen | None = None

    def _cleanup(*_args):  # signal handler — terminate both children
        for p, name in ((web_proc, "web"), (api_proc, "api")):
            if p and p.poll() is None:
                console.print(f"[yellow]Stopping {name}…[/yellow]")
                p.terminate()
                try:
                    p.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    p.kill()

    signal.signal(signal.SIGINT, _cleanup)
    signal.signal(signal.SIGTERM, _cleanup)

    import urllib.error
    import urllib.request

    def _wait_for(url: str, timeout: float, label: str) -> bool:
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                urllib.request.urlopen(url, timeout=1).read()
                return True
            except (urllib.error.URLError, ConnectionRefusedError, OSError):
                # Bail out early if either child has died.
                if (api_proc and api_proc.poll() is not None) or (
                    web_proc and web_proc.poll() is not None
                ):
                    return False
                time.sleep(0.4)
        console.print(f"[red]{label} did not become reachable within {timeout:.0f}s.[/red]")
        return False

    try:
        console.print(f"[bold]Starting API on :{api_port} (category={category})…[/bold]")
        api_env = os.environ.copy()
        api_env["GOOGLE_DRIVE_CATEGORY"] = category
        api_proc = subprocess.Popen(
            [
                "uv", "run", "uvicorn", "asset_api.main:app",
                "--host", "127.0.0.1", "--port", str(api_port),
            ],
            cwd=str(repo_root),
            env=api_env,
        )

        if not _wait_for(f"http://127.0.0.1:{api_port}/api/health", 30.0, "API"):
            console.print(
                "[red]The API failed to start. Check the terminal output above for "
                "tracebacks (most common: GOOGLE_OAUTH_CREDENTIALS missing, "
                "DATABASE_URL not set, or port 8000 already in use).[/red]"
            )
            return

        console.print(f"[bold]Starting web on :{web_port}…[/bold]")
        env = os.environ.copy()
        env["PORT"] = str(web_port)
        web_proc = subprocess.Popen(
            [pnpm, "dev", "--port", str(web_port)],
            cwd=str(web_dir),
            env=env,
        )

        if not _wait_for(f"http://127.0.0.1:{web_port}/", 60.0, "Web"):
            console.print("[red]The Next.js dev server did not start. See the log above.[/red]")
            return

        url = f"http://localhost:{web_port}"
        console.print(f"\n[green]✓ API ready on :{api_port}, web ready on :{web_port}[/green]")
        console.print(f"[green]Open {url} in your browser.[/green]")
        if not no_browser:
            webbrowser.open(url)
        console.print("[bold]Press Ctrl+C to stop.[/bold]\n")

        # Wait for either process to exit (or for Ctrl+C).
        while True:
            time.sleep(1)
            if api_proc.poll() is not None or web_proc.poll() is not None:
                break

    finally:
        _cleanup()


def _organize_rename(
    config_path: Path,
    root_folder_id: Optional[str],
    category: str,
    execute: bool,
) -> None:
    """Read saved orders and rename files in Drive sequentially."""
    import asyncio
    from pathlib import PurePosixPath as _PP

    from rich.table import Table

    from asset_db.models import PhotoOrder
    from asset_db.session import get_sessionmaker
    from asset_sdk.adapters import drive
    from asset_sdk.config import PipelineConfig
    from sqlalchemy import select

    if not root_folder_id:
        root_folder_id = os.environ.get("GOOGLE_DRIVE_ROOT_FOLDER_ID")
    if not root_folder_id:
        console.print("[red]GOOGLE_DRIVE_ROOT_FOLDER_ID is not set.[/red]")
        raise typer.Exit(1)

    category_folder_id = drive.resolve_category_folder(root_folder_id, category)

    cfg = PipelineConfig.load(config_path)
    photos_subdir = cfg.paths.product_photos
    structure = cfg.structure_for(category)

    # Pull all saved orders from the DB.
    async def _load_orders() -> list[PhotoOrder]:
        Session = get_sessionmaker()
        async with Session() as session:
            res = await session.execute(select(PhotoOrder))
            return list(res.scalars().all())

    orders = asyncio.run(_load_orders())
    if not orders:
        console.print("[yellow]No saved photo orders found in the database.[/yellow]")
        return

    # Index SKU folders by name.
    if structure == "flat":
        sku_index = {name: ("", fid) for name, fid in drive.list_folders(category_folder_id).items()}
    else:
        sku_index = {}
        for sup_name, sup_id in drive.list_folders(category_folder_id).items():
            for name, fid in drive.list_folders(sup_id).items():
                sku_index[name] = (sup_name, fid)

    table = Table(title=f"Photo orders to apply ({len(orders)})")
    table.add_column("Supplier")
    table.add_column("SKU")
    table.add_column("Photos", justify="right")
    table.add_column("Status")

    plans: list[tuple[str, str, str, list[tuple[str, str, str]]]] = []
    # plan tuple: (supplier, sku, photos_folder_id, [(file_id, current_name, target_name), ...])

    for order in orders:
        supplier, sku_id = sku_index.get(order.sku, ("", None))
        if sku_id is None:
            table.add_row("", order.sku, str(len(order.items)), "[red]SKU folder not found[/red]")
            continue
        # Resolve photos subfolder
        current = sku_id
        photos_id: str | None = sku_id
        for part in photos_subdir.split("/"):
            children = drive.list_folders(current)
            if part not in children:
                photos_id = None
                break
            current = children[part]
            photos_id = current
        if photos_id is None:
            table.add_row(supplier, order.sku, str(len(order.items)), "[red]photos/ not found[/red]")
            continue

        existing = {f["id"]: f["name"] for f in drive.list_files(photos_id)}

        # Build the rename list for items still present.
        renames: list[tuple[str, str, str]] = []
        position = 0
        for item in order.items:
            fid = item.get("file_id")
            if fid not in existing:
                continue
            position += 1
            current_name = existing[fid]
            ext = _PP(current_name).suffix.lower() or ".jpg"
            target_name = f"{position}{ext}"
            renames.append((fid, current_name, target_name))

        plans.append((supplier, order.sku, photos_id, renames))
        status = f"{len(renames)} → 1..{len(renames)}"
        table.add_row(supplier, order.sku, str(len(order.items)), status)

    console.print(table)

    if not execute:
        n = sum(len(p[3]) for p in plans)
        console.print(
            f"\n[bold]Dry run complete[/bold] — pass --execute to rename {n} files."
        )
        return

    bar = Progress(
        SpinnerColumn(),
        TextColumn("{task.description}"),
        BarColumn(bar_width=30),
        MofNCompleteColumn(),
        console=console,
    )
    total = sum(len(p[3]) for p in plans)
    if total == 0:
        console.print("[yellow]Nothing to rename.[/yellow]")
        return
    renamed = errored = 0
    with bar:
        bar_task = bar.add_task("Renaming…", total=total, completed=0)
        # Two-phase rename to avoid name collisions: first to a temp prefix, then to final.
        TEMP_PREFIX = "__reorder_tmp__"
        for supplier, sku, _, renames in plans:
            # Phase 1: rename each to a temp name.
            for fid, current, target in renames:
                try:
                    drive.rename_item(fid, f"{TEMP_PREFIX}{target}")
                except Exception as exc:
                    errored += 1
                    bar.update(
                        bar_task,
                        description=f"  [red]err[/red] {sku}/{current}: {exc}",
                        advance=0,
                    )
            # Phase 2: rename from temp to final.
            for fid, current, target in renames:
                try:
                    drive.rename_item(fid, target)
                    renamed += 1
                    bar.update(
                        bar_task,
                        description=f"  [green]ok[/green] {sku}/{current} → {target}",
                        advance=1,
                    )
                except Exception as exc:
                    errored += 1
                    bar.update(
                        bar_task,
                        description=f"  [red]err[/red] {sku}/{current}: {exc}",
                        advance=1,
                    )

    console.print(
        f"\n[bold]Rename complete[/bold]  ({renamed} renamed, {errored} errored)"
    )


if __name__ == "__main__":
    app()

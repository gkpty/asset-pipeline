from __future__ import annotations

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
        help="Google Drive root folder ID containing the SKU folders.",
    ),
) -> None:
    """Check credentials and confirm access to the Google Sheet and Drive folder."""
    from asset_sdk.adapters.drive import get_item_name
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
        console.print(f"[green]✓[/green] Drive folder found:  [bold]{name}[/bold]")
        console.print(f"    ID: {folder_id}")
    except Exception as exc:
        console.print(f"[red]✗[/red] Drive folder not accessible: {exc}")


@app.command()
def diagnose(
    config_path: Path = typer.Option(
        Path("pipeline.config.toml"),
        envvar="PIPELINE_CONFIG_PATH",
        help="Path to pipeline.config.toml.",
    ),
    folder_id: str = typer.Option(
        ..., envvar="GOOGLE_DRIVE_ROOT_FOLDER_ID",
        help="Google Drive root folder ID.",
    ),
    sheet_id: str = typer.Option(
        ..., envvar="GOOGLE_SHEETS_MASTER_ID",
        help="Google Sheets file ID.",
    ),
    tab: Optional[str] = typer.Option(
        None, help="Sheet tab with the SKU list. Defaults to csv.tab_name in config.",
    ),
    sku_col: Optional[str] = typer.Option(
        None, help="SKU column header. Defaults to csv.sku_column in config.",
    ),
    supplier_col: Optional[str] = typer.Option(
        None, help="Supplier column header. Defaults to csv.supplier_column in config.",
    ),
    report_tab: Optional[str] = typer.Option(
        None, help="Tab to write the report into. Defaults to diagnose.report_tab in config.",
    ),
) -> None:
    """
    Scan a Google Drive products folder and report its structure against
    a Google Sheets SKU list. Most options default to values in pipeline.config.toml.
    """
    from asset_sdk.adapters.sheets import read_rows, write_report
    from asset_sdk.config import PipelineConfig
    from asset_sdk.stages.diagnose import run, to_sheet_rows

    cfg = PipelineConfig.load(config_path)

    # Resolve: CLI flag → config → (already has a hardcoded default in config dataclass)
    _tab          = tab          or cfg.csv.tab_name
    _sku_col      = sku_col      or cfg.csv.sku_column
    _supplier_col = supplier_col or cfg.csv.supplier_column
    _report_tab   = report_tab   or cfg.diagnose.report_tab
    _structure    = cfg.drive.structure

    with Progress(SpinnerColumn(), TextColumn("{task.description}"), console=console) as progress:
        t = progress.add_task(f"Reading '{_tab}' tab…", total=None)
        sheet_rows = read_rows(sheet_id, _tab)
        progress.update(t, description=f"Read {len(sheet_rows)} rows from '{_tab}'")
        progress.stop_task(t)

        t = progress.add_task("Scanning Drive folder…", total=None)
        report = run(folder_id, sheet_rows, _sku_col, _supplier_col, cfg.paths, _structure)
        progress.update(t, description=f"Scanned {len(report.results)} SKUs")
        progress.stop_task(t)

        t = progress.add_task(f"Writing report to '{_report_tab}'…", total=None)
        headers, rows = to_sheet_rows(report, cfg.paths)
        write_report(sheet_id, _report_tab, headers, rows)
        progress.update(t, description=f"Report written → '{_report_tab}'")
        progress.stop_task(t)

    missing    = sum(1 for r in report.results if not r.sku_dir_found)
    incomplete = sum(1 for r in report.results if r.sku_dir_found and r.missing_subdirs)
    ok         = sum(1 for r in report.results if r.sku_dir_found and not r.missing_subdirs)
    orphans    = len(report.orphan_dirs)

    console.print()
    console.print("[bold]Diagnose complete[/bold]")
    console.print(f"  [green]OK[/green]           {ok}")
    console.print(f"  [yellow]Incomplete[/yellow]   {incomplete}")
    console.print(f"  [red]Missing dir[/red]  {missing}")
    console.print(f"  [red]Orphan dirs[/red]  {orphans}")
    if report.orphan_dirs:
        console.print(f"  Orphans: {', '.join(report.orphan_dirs)}")


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
        help="Google Drive root products folder ID (copy destination).",
    ),
    sheet_id: str = typer.Option(
        ..., envvar="GOOGLE_SHEETS_MASTER_ID",
        help="Google Sheets file ID.",
    ),
    tab: Optional[str] = typer.Option(
        None, help="Sheet tab with the SKU list. Defaults to csv.tab_name in config.",
    ),
    sku_col: Optional[str] = typer.Option(
        None, help="SKU column header. Defaults to csv.sku_column in config.",
    ),
    parent_product_col: Optional[str] = typer.Option(
        None, help="Parent product column header. Defaults to csv.parent_product_column in config.",
    ),
    report_tab: Optional[str] = typer.Option(
        None, help="Tab to write the report into. Defaults to lifestyle.report_tab in config.",
    ),
    execute: bool = typer.Option(
        False, "--execute",
        help="Rename folders in Drive to their selected SKU. Omit for a dry run.",
    ),
) -> None:
    """
    Map lifestyle photo folders (named by parent product) to SKUs and write a
    rename report. Pass --execute to actually rename the folders in Drive.
    """
    from asset_sdk.adapters.sheets import read_rows, write_report
    from asset_sdk.config import PipelineConfig
    from asset_sdk.stages.rename_lifestyle import build_report, execute_copy, to_sheet_rows

    cfg = PipelineConfig.load(config_path)

    _tab               = tab               or cfg.csv.tab_name
    _sku_col           = sku_col           or cfg.csv.sku_column
    _parent_product_col = parent_product_col or cfg.csv.parent_product_column
    _report_tab        = report_tab        or cfg.lifestyle.report_tab

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
                root_folder_id,
                cfg.drive.structure,
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
        help="Google Drive root products folder ID (copy destination).",
    ),
    sheet_id: str = typer.Option(
        ..., envvar="GOOGLE_SHEETS_MASTER_ID",
        help="Google Sheets file ID.",
    ),
    tab: Optional[str] = typer.Option(
        None, help="Sheet tab with the SKU list. Defaults to csv.tab_name in config.",
    ),
    sku_col: Optional[str] = typer.Option(
        None, help="SKU column header. Defaults to csv.sku_column in config.",
    ),
    report_tab: Optional[str] = typer.Option(
        None, help="Tab to write the report into. Defaults to models.report_tab in config.",
    ),
    execute: bool = typer.Option(
        False, "--execute",
        help="Copy model files into each SKU's product folder. Omit for a dry run.",
    ),
) -> None:
    """
    Scan the shared models folder, report on each SKU's contents (OBJ/SKP/DWG/PDF/GLTF),
    flag missing or unexpected items, and optionally copy into the products Drive.
    PDF files are copied into the /diagram subfolder. Pass --execute to copy.
    """
    from asset_sdk.adapters.sheets import read_rows, write_report
    from asset_sdk.config import PipelineConfig
    from asset_sdk.stages.copy_models import (
        ModelCopyProgress,
        build_report,
        execute_copy,
        to_sheet_rows,
    )

    cfg = PipelineConfig.load(config_path)
    _tab        = tab        or cfg.csv.tab_name
    _sku_col    = sku_col    or cfg.csv.sku_column
    _report_tab = report_tab or cfg.models.report_tab

    with Progress(SpinnerColumn(), TextColumn("{task.description}"), console=console) as progress:
        t = progress.add_task(f"Reading '{_tab}' tab…", total=None)
        sheet_rows = read_rows(sheet_id, _tab)
        sheet_skus = {r.get(_sku_col, "").strip() for r in sheet_rows if r.get(_sku_col, "").strip()}
        progress.update(t, description=f"Read {len(sheet_skus)} SKUs from '{_tab}'")
        progress.stop_task(t)

        t = progress.add_task("Scanning models folder…", total=None)
        entries, missing_skus = build_report(
            models_folder_id, sheet_skus, root_folder_id, cfg.drive.structure,
        )
        progress.update(t, description=f"Found {len(entries)} SKU folders in models")
        progress.stop_task(t)

    copied = 0
    skipped = 0
    if execute:
        total_skus = sum(1 for e in entries if e.in_products)
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
            for p in execute_copy(entries, root_folder_id, cfg.drive.structure, cfg.paths):
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

    with Progress(SpinnerColumn(), TextColumn("{task.description}"), console=console) as progress:
        t = progress.add_task(f"Writing report to '{_report_tab}'…", total=None)
        headers, rows = to_sheet_rows(entries, missing_skus)
        write_report(sheet_id, _report_tab, headers, rows)
        progress.update(t, description=f"Report written → '{_report_tab}'")
        progress.stop_task(t)

    not_in_sheet   = sum(1 for e in entries if not e.in_sheet)
    not_in_products = sum(1 for e in entries if not e.in_products)
    has_extras     = sum(1 for e in entries if e.extra_items)

    console.print()
    if execute:
        console.print(f"[bold]Copy complete[/bold]  ({copied} files copied, {skipped} SKUs already done)")
    else:
        console.print("[bold]Dry run complete[/bold] (pass --execute to copy files)")
    console.print(f"  [green]OK[/green]                 {len(entries) - not_in_sheet - not_in_products}")
    console.print(f"  [yellow]Has extra items[/yellow]    {has_extras}")
    console.print(f"  [red]Not in sheet[/red]       {not_in_sheet}")
    console.print(f"  [red]Not in products drive[/red] {not_in_products}")
    console.print(f"  [red]Missing from models[/red]   {len(missing_skus)}")


if __name__ == "__main__":
    app()

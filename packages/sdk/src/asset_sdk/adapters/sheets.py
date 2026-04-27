from __future__ import annotations

import gspread

from asset_sdk.adapters.drive import _get_creds


def _client() -> gspread.Client:
    return gspread.authorize(_get_creds())


def get_spreadsheet_title(sheet_id: str) -> str:
    """Return the title of the spreadsheet, or raise if not found / not accessible."""
    return _client().open_by_key(sheet_id).title


def read_rows(sheet_id: str, tab: str) -> list[dict[str, str]]:
    """Return all rows from *tab* as a list of dicts keyed by header."""
    ws = _client().open_by_key(sheet_id).worksheet(tab)
    return ws.get_all_records()


def write_report(
    sheet_id: str,
    tab: str,
    headers: list[str],
    rows: list[list],
) -> None:
    """Write *headers* + *rows* to *tab*, creating the tab if needed."""
    gc = _client()
    ss = gc.open_by_key(sheet_id)
    try:
        ws = ss.worksheet(tab)
        ws.clear()
    except gspread.exceptions.WorksheetNotFound:
        ws = ss.add_worksheet(title=tab, rows=max(len(rows) + 20, 100), cols=len(headers))
    ws.update([headers, *rows])

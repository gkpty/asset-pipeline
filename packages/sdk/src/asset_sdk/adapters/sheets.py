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
    value_input_option: str = "RAW",
    row_height_px: int | None = None,
) -> None:
    """Write *headers* + *rows* to *tab*, creating the tab if needed.

    value_input_option:
      "RAW"          — values written as-is (default; safe for arbitrary text)
      "USER_ENTERED" — values parsed by Sheets (formulas like =IMAGE(...) evaluated)

    row_height_px:
      When set, applies that height (in pixels) to every data row (header row 1 untouched).
      Useful for reports with embedded =IMAGE() previews so thumbnails are visible.
    """
    gc = _client()
    ss = gc.open_by_key(sheet_id)
    try:
        ws = ss.worksheet(tab)
        ws.clear()
    except gspread.exceptions.WorksheetNotFound:
        ws = ss.add_worksheet(title=tab, rows=max(len(rows) + 20, 100), cols=len(headers))
    ws.update([headers, *rows], value_input_option=value_input_option)

    if row_height_px is not None and rows:
        ss.batch_update({
            "requests": [{
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": ws.id,
                        "dimension": "ROWS",
                        "startIndex": 1,            # row 0 is the header
                        "endIndex": 1 + len(rows),
                    },
                    "properties": {"pixelSize": row_height_px},
                    "fields": "pixelSize",
                }
            }]
        })

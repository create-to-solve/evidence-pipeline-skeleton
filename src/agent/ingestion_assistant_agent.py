from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional, Union
import re

import pandas as pd


# ---------------------------------------------------------------------
# CONSTANTS
# ---------------------------------------------------------------------
HEADER_TOKENS = [
    "local authority",
    "la code",
    "local authority code",
    "code",
    "name",
    "co2",
    "emissions",
    "territorial",
    "kt",
]

LA_CODE_REGEX = re.compile(r"^[EWNS][0-9A-Z]{8}$")


# ---------------------------------------------------------------------
# BASIC HELPERS
# ---------------------------------------------------------------------
def _normalise_str(x: Any) -> str:
    return str(x).strip().lower() if pd.notna(x) else ""


def _score_row_for_header(cells: List[Any]) -> int:
    """Score a row to determine if it looks like a header row."""
    text_cells = [_normalise_str(c) for c in cells]
    score = 0
    for token in HEADER_TOKENS:
        if any(token in c for c in text_cells):
            score += 1

    # Reward rows with ≥3 non-empty cells
    non_empty = sum(1 for c in text_cells if c)
    if non_empty >= 3:
        score += 1
    return score


def _detect_header_row(df: pd.DataFrame, max_scan: int = 20) -> int:
    """Scan early rows and return the best header candidate."""
    best_row = 0
    best_score = -1

    scan_limit = min(max_scan, len(df))

    for idx in range(scan_limit):
        row_values = df.iloc[idx].tolist()
        score = _score_row_for_header(row_values)
        if score > best_score:
            best_score = score
            best_row = idx

    return best_row


# ---------------------------------------------------------------------
# COLUMN DETECTION
# ---------------------------------------------------------------------
def _detect_la_columns(df: pd.DataFrame) -> Dict[str, Optional[str]]:
    """Infer LA code/name columns from names or sample values."""
    la_code_col = None
    la_name_col = None

    norm_cols = {col: _normalise_str(col) for col in df.columns}

    # 1. Name-based detection
    for col, norm in norm_cols.items():
        if "local authority code" in norm or norm == "la code" or (
            "code" in norm and "authority" in norm
        ):
            la_code_col = col
            break

    # 2. Value-based pattern detection
    if la_code_col is None:
        for col in df.columns:
            sample_vals = df[col].dropna().astype(str).head(50)
            match_count = sum(1 for v in sample_vals if LA_CODE_REGEX.match(v.strip()))
            if match_count >= 5:
                la_code_col = col
                break

    # 3. Name column detection
    for col, norm in norm_cols.items():
        if "local authority" in norm and "code" not in norm:
            la_name_col = col
            break

    return {
        "la_code_col": la_code_col,
        "la_name_col": la_name_col,
    }


def _detect_year_and_value_columns(df: pd.DataFrame) -> Dict[str, List[str]]:
    """Identify year-like and value-like numeric columns."""
    year_cols: List[str] = []
    value_cols: List[str] = []

    for col in df.columns:
        norm = _normalise_str(col)

        if re.fullmatch(r"\d{4}", norm):
            year_cols.append(col)
            continue

        if any(token in norm for token in ["emissions", "co2", "kt", "tonnes"]):
            value_cols.append(col)
            continue

    # Fallback: numeric columns with many valid numbers
    if not value_cols:
        for col in df.columns:
            series = pd.to_numeric(df[col], errors="coerce")
            non_null = series.notna().sum()
            if non_null > 0.5 * len(series):
                value_cols.append(col)

    return {
        "year_columns": year_cols,
        "value_columns": value_cols,
    }


# ---------------------------------------------------------------------
# EXCEL ANALYSIS
# ---------------------------------------------------------------------
def _analyse_excel(path: Path, hint: Optional[str]) -> Dict[str, Any]:
    xls = pd.ExcelFile(path)
    issues: List[str] = []
    notes: List[str] = []

    # Filter out obvious metadata sheets
    candidate_sheets = [
        s for s in xls.sheet_names if s.lower() not in ["cover", "contents", "notes"]
    ]
    if not candidate_sheets:
        candidate_sheets = xls.sheet_names
        issues.append("No obvious data sheets; using all sheets.")

    best_sheet = candidate_sheets[0]
    best_sheet_score = -1
    best_header_row = 0

    # Evaluate each candidate sheet
    for sheet in candidate_sheets:
        tmp_df = pd.read_excel(xls, sheet_name=sheet, header=None, nrows=25)
        header_row = _detect_header_row(tmp_df, max_scan=15)

        # Base score: header match + non-empty column count
        header_score = _score_row_for_header(tmp_df.iloc[header_row].tolist())
        non_empty_cols = (tmp_df.iloc[header_row].notna()).sum()
        sheet_score = header_score + non_empty_cols

        # ------------------------------------------------------------
        # HEURISTIC 1: Prefer LA-level tables ("2_*")
        # ------------------------------------------------------------
        if sheet.startswith("2_"):
            sheet_score += 10

        # ------------------------------------------------------------
        # HEURISTIC 2: Penalise summary sheets ("1_*")
        # ------------------------------------------------------------
        if sheet.startswith("1_"):
            sheet_score -= 6

        # ------------------------------------------------------------
        # HEURISTIC 3: Use hint
        # ------------------------------------------------------------
        if hint and "la" in hint.lower():
            if sheet.startswith("2_"):
                sheet_score += 4
            if sheet.startswith("1_"):
                sheet_score -= 4

        # ------------------------------------------------------------
        # HEURISTIC 4: Column-pattern recognition
        # ------------------------------------------------------------
        try:
            cols = [_normalise_str(c) for c in tmp_df.iloc[header_row].tolist()]
            if any("authority" in c for c in cols):
                sheet_score += 3
            if any("code" in c for c in cols):
                sheet_score += 2
            if any("calendar year" in c for c in cols):
                sheet_score += 2
        except Exception:
            pass

        # Retain the best sheet
        if sheet_score > best_sheet_score:
            best_sheet_score = sheet_score
            best_sheet = sheet
            best_header_row = header_row

    notes.append(
        f"Selected sheet '{best_sheet}' with header row {best_header_row} "
        f"(score={best_sheet_score})."
    )

    # Load the chosen sheet
    df = pd.read_excel(xls, sheet_name=best_sheet, header=best_header_row)

    # Detect structure
    la_cols = _detect_la_columns(df)
    yr_val_cols = _detect_year_and_value_columns(df)

    if la_cols["la_code_col"] is None:
        issues.append("Could not detect a Local Authority code column.")
    if la_cols["la_name_col"] is None:
        issues.append("Could not detect a Local Authority name column.")

    confidence = 0.7
    if la_cols["la_code_col"] and la_cols["la_name_col"]:
        confidence = 0.9
    elif not yr_val_cols["value_columns"]:
        confidence = 0.5

    return {
        "file_path": str(path),
        "file_type": "excel",
        "recommended_sheet": best_sheet,
        "recommended_header_row": best_header_row,
        "probable_la_code_column": la_cols["la_code_col"],
        "probable_la_name_column": la_cols["la_name_col"],
        "probable_year_columns": yr_val_cols["year_columns"],
        "probable_value_columns": yr_val_cols["value_columns"],
        "issues_detected": issues,
        "notes": notes,
        "confidence": confidence,
    }


# ---------------------------------------------------------------------
# CSV ANALYSIS
# ---------------------------------------------------------------------
def _analyse_csv(path: Path) -> Dict[str, Any]:
    df = pd.read_csv(path, nrows=200)

    la_cols = _detect_la_columns(df)
    yr_val_cols = _detect_year_and_value_columns(df)

    issues: List[str] = []
    notes: List[str] = []

    if la_cols["la_code_col"] is None:
        issues.append("Could not detect a Local Authority code column.")
    if la_cols["la_name_col"] is None:
        issues.append("Could not detect a Local Authority name column.")

    confidence = 0.7
    if la_cols["la_code_col"] and la_cols["la_name_col"]:
        confidence = 0.9
    elif not yr_val_cols["value_columns"]:
        confidence = 0.5

    return {
        "file_path": str(path),
        "file_type": "csv",
        "recommended_sheet": None,
        "recommended_header_row": 0,
        "probable_la_code_column": la_cols["la_code_col"],
        "probable_la_name_column": la_cols["la_name_col"],
        "probable_year_columns": yr_val_cols["year_columns"],
        "probable_value_columns": yr_val_cols["value_columns"],
        "issues_detected": issues,
        "notes": notes,
        "confidence": confidence,
    }


# ---------------------------------------------------------------------
# PUBLIC ENTRY POINT
# ---------------------------------------------------------------------
def analyze_file(path: Union[str, Path], hint: Optional[str] = None) -> Dict[str, Any]:
    path = Path(path)

    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")

    suffix = path.suffix.lower()

    if suffix in [".xlsx", ".xls"]:
        result = _analyse_excel(path, hint)
    elif suffix == ".csv":
        result = _analyse_csv(path)
    else:
        raise ValueError(f"Unsupported file type: {suffix}")

    if hint:
        result["notes"].append(f"Hint provided: {hint}")

    return result

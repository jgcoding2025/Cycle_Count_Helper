from __future__ import annotations

from core.notes_db import NotesDB, NoteKey
from core.exporter import export_workbook

from core.recommender import apply_recommendations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd
from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QApplication,
    QMainWindow,
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QPushButton,
    QLabel,
    QFileDialog,
    QLineEdit,
    QTableWidget,
    QTableWidgetItem,
    QMessageBox,
    QSplitter,
)

from core.io_excel import load_warehouse_locations, load_recount_workbook
from core.review_builder import build_review_lines


@dataclass
class LoadedPaths:
    warehouse_locations_path: Path | None = None
    recount_path: Path | None = None


class MainWindow(QMainWindow):
    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle("Cycle Count Assistant (MVP)")
        self.resize(1200, 750)

        self.paths = LoadedPaths()

        self.notes_db = NotesDB(Path("data") / "cyclecount_notes.db")

        self.review_df: pd.DataFrame | None = None
        self.transfers_df: pd.DataFrame | None = None
        self.group_df: pd.DataFrame | None = None


        root = QWidget()
        self.setCentralWidget(root)
        root_layout = QVBoxLayout(root)

        # --- Top controls ---
        top = QWidget()
        self.top_layout = QHBoxLayout(top)
        self.top_layout.setContentsMargins(0, 0, 0, 0)

        self.btn_load_locations = QPushButton("Load Warehouse Locations.xlsx")
        self.btn_load_recount = QPushButton("Load Recount Workbook.xlsx")
        self.btn_build_review = QPushButton("Build Review Table")
        self.btn_build_review.setEnabled(False)
        self.btn_export = QPushButton("Export XLSX")
        self.btn_export.setEnabled(False)

        self.session_id = QLineEdit()
        self.session_id.setPlaceholderText("SessionId (e.g., 20250113)")

        self.top_layout.addWidget(self.btn_load_locations)
        self.top_layout.addWidget(self.btn_load_recount)
        self.top_layout.addWidget(QLabel("SessionId:"))
        self.top_layout.addWidget(self.session_id, 1)
        self.top_layout.addWidget(self.btn_build_review)
        self.top_layout.addWidget(self.btn_export)

        root_layout.addWidget(top)

        # --- Status line ---
        self.status_label = QLabel("Load both files to begin.")
        self.status_label.setWordWrap(True)
        root_layout.addWidget(self.status_label)

        # --- Split area: table + details (placeholder) ---
        splitter = QSplitter(Qt.Horizontal)

        self.table = QTableWidget(0, 0)
        self.table.itemChanged.connect(self._on_table_item_changed)
        self._updating_table = False

        self.table.setSortingEnabled(True)
        splitter.addWidget(self.table)

        right_panel = QWidget()
        right_layout = QVBoxLayout(right_panel)
        right_layout.addWidget(QLabel("Group Details (Step 3+)"))
        self.details = QLabel(
            "Once we implement logic, clicking a row will show:\n"
            "- DefaultLocation\n"
            "- Transfer plan\n"
            "- DefaultAfterTransfers\n"
            "- Remaining adjustment\n"
            "- Flags (secured variance, default empty, missing master)"
        )
        self.details.setStyleSheet("QLabel { color: #cccccc; }")
        self.details.setWordWrap(True)
        right_layout.addWidget(self.details, 1)
        splitter.addWidget(right_panel)

        splitter.setStretchFactor(0, 3)
        splitter.setStretchFactor(1, 2)
        root_layout.addWidget(splitter, 1)

        # Wire events
        self.btn_load_locations.clicked.connect(self._pick_locations_file)
        self.btn_load_recount.clicked.connect(self._pick_recount_file)
        self.btn_build_review.clicked.connect(self._build_review_placeholder)
        self.btn_export.clicked.connect(self._export_xlsx)

    def _pick_locations_file(self) -> None:
        path, _ = QFileDialog.getOpenFileName(
            self,
            "Select Warehouse Locations.xlsx",
            "",
            "Excel Files (*.xlsx)",
        )
        if not path:
            return
        self.paths.warehouse_locations_path = Path(path)
        self._update_status()

    def _pick_recount_file(self) -> None:
        path, _ = QFileDialog.getOpenFileName(
            self,
            "Select Recount Workbook.xlsx",
            "",
            "Excel Files (*.xlsx)",
        )
        if not path:
            return
        self.paths.recount_path = Path(path)
        self._update_status()

        if not self.session_id.text().strip():
            stem = self.paths.recount_path.stem
            digits = "".join(ch for ch in stem if ch.isdigit())
            if len(digits) >= 8:
                self.session_id.setText(digits[:8])

    def _on_table_item_changed(self, item: QTableWidgetItem) -> None:
        if getattr(self, "_updating_table", False):
            return
        if self.review_df is None:
            return
        if getattr(self, "_col_usernotes", -1) < 0:
            return
        if item.column() != self._col_usernotes:
            return

        row = item.row()
        new_note = item.text()

        sid = self.session_id.text().strip()
        whs = str(self.review_df.at[row, "Whs"])
        it = str(self.review_df.at[row, "Item"])
        lot = str(self.review_df.at[row, "Batch/lot"])
        loc = str(self.review_df.at[row, "Location"])

        key = NoteKey(sid, whs, it, lot, loc)
        updated_at = self.notes_db.upsert_note(key, new_note)

        # Update dataframe + table cell for NoteUpdatedAt
        self.review_df.at[row, "UserNotes"] = new_note
        self.review_df.at[row, "NoteUpdatedAt"] = updated_at

        # update visible NoteUpdatedAt cell if present
        try:
            col_updated = list(self.review_df.columns).index("NoteUpdatedAt")
            self._updating_table = True
            self.table.item(row, col_updated).setText(updated_at)
        finally:
            self._updating_table = False


    def _update_status(self) -> None:
        loc = str(self.paths.warehouse_locations_path) if self.paths.warehouse_locations_path else "(not loaded)"
        rec = str(self.paths.recount_path) if self.paths.recount_path else "(not loaded)"
        self.status_label.setText(f"Warehouse Locations: {loc}\nRecount Workbook: {rec}")

        ready = self.paths.warehouse_locations_path is not None and self.paths.recount_path is not None
        self.btn_build_review.setEnabled(ready)

    def _build_review_placeholder(self) -> None:
        if not self.paths.warehouse_locations_path or not self.paths.recount_path:
            QMessageBox.warning(self, "Missing files", "Please load both files first.")
            return

        sid = self.session_id.text().strip()
        if not sid:
            QMessageBox.warning(self, "Missing SessionId", "Please enter a SessionId (e.g., 20250113).")
            return

        try:
            self.status_label.setText("Loading Excel files...")
            QApplication.processEvents()

            loc_df = load_warehouse_locations(self.paths.warehouse_locations_path)
            rec_df = load_recount_workbook(self.paths.recount_path)
            review_df = build_review_lines(sid, rec_df, loc_df)

            # Step 3: recommendations + transfer plan + group summary
            review_df, transfers_df, group_df = apply_recommendations(review_df)

            # Merge in persisted notes
            notes_map = self.notes_db.read_notes_for_session(sid)

            review_df["UserNotes"] = ""
            review_df["NoteUpdatedAt"] = ""

            for i in range(len(review_df)):
                whs = str(review_df.at[i, "Whs"])
                item = str(review_df.at[i, "Item"])
                lot = str(review_df.at[i, "Batch/lot"])
                loc = str(review_df.at[i, "Location"])
                key = NoteKey(sid, whs, item, lot, loc)
                if key in notes_map:
                    note, updated = notes_map[key]
                    review_df.at[i, "UserNotes"] = note
                    review_df.at[i, "NoteUpdatedAt"] = updated


            self.review_df = review_df
            self.btn_export.setEnabled(True)

            self.transfers_df = transfers_df
            self.group_df = group_df

        except Exception as e:
            self.btn_export.setEnabled(False)
            QMessageBox.critical(self, "Load/Build Error", str(e))
            return

        self._set_table_from_df(self.review_df)

        adj_groups = 0 if self.group_df is None else int((self.group_df["RemainingAdjustmentQty"] != 0).sum())
        transfer_lines = 0 if self.transfers_df is None else len(self.transfers_df)

        self.status_label.setText(
            f"Loaded {len(loc_df):,} locations and {len(rec_df):,} recount rows.\n"
            f"Review_Lines rows: {len(review_df):,} | Transfer suggestions: {transfer_lines:,} | Groups w/ adjustment: {adj_groups:,}"
        )

    def _set_table(self, headers: list[str], rows: list[list[str]]) -> None:
        self.table.clear()
        self.table.setColumnCount(len(headers))
        self.table.setRowCount(len(rows))
        self.table.setHorizontalHeaderLabels(headers)

        for r, row in enumerate(rows):
            for c, val in enumerate(row):
                item = QTableWidgetItem(str(val))
                item.setFlags(item.flags() & ~Qt.ItemIsEditable)
                self.table.setItem(r, c, item)

        self.table.resizeColumnsToContents()

    def _set_table_from_df(self, df: "pd.DataFrame") -> None:
        import pandas as pd

        self._updating_table = True
        try:
            headers = [str(c) for c in df.columns]
            self.table.clear()
            self.table.setColumnCount(len(headers))
            self.table.setRowCount(len(df))
            self.table.setHorizontalHeaderLabels(headers)

            # Remember column index for UserNotes
            self._col_usernotes = headers.index("UserNotes") if "UserNotes" in headers else -1

            for r in range(len(df)):
                row = df.iloc[r]
                for c, col in enumerate(headers):
                    val = row[col]
                    if pd.isna(val):
                        s = ""
                    else:
                        if isinstance(val, float) and val.is_integer():
                            s = str(int(val))
                        else:
                            s = str(val)

                    item = QTableWidgetItem(s)

                    if c == self._col_usernotes:
                        item.setFlags(item.flags() | Qt.ItemIsEditable)
                    else:
                        item.setFlags(item.flags() & ~Qt.ItemIsEditable)

                    self.table.setItem(r, c, item)

            self.table.resizeColumnsToContents()
        finally:
            self._updating_table = False

    def _export_xlsx(self) -> None:
        if self.review_df is None or self.group_df is None or self.transfers_df is None:
            QMessageBox.warning(self, "Nothing to export", "Build the review table first.")
            return

        sid = self.session_id.text().strip() or "Session"
        default_name = f"CycleCountReview_{sid}.xlsx"

        path, _ = QFileDialog.getSaveFileName(
            self,
            "Save Export Workbook",
            default_name,
            "Excel Files (*.xlsx)",
        )
        if not path:
            return

        try:
            export_workbook(
                Path(path),
                self.review_df,
                self.group_df,
                self.transfers_df,
            )
        except Exception as e:
            QMessageBox.critical(self, "Export failed", str(e))
            return

        QMessageBox.information(self, "Export complete", f"Saved:\n{path}")



def run_app() -> None:
    app = QApplication([])
    win = MainWindow()
    win.show()
    app.exec()

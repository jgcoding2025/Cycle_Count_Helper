from __future__ import annotations

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
from core.recommender import apply_recommendations
from core.notes_db import NotesDB, NoteKey
from core.exporter import export_workbook


@dataclass
class LoadedPaths:
    warehouse_locations_path: Path | None = None
    recount_path: Path | None = None


class MainWindow(QMainWindow):
    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle("Cycle Count Assistant")
        self.resize(1300, 800)

        self.paths = LoadedPaths()
        self.review_df: pd.DataFrame | None = None
        self.transfers_df: pd.DataFrame | None = None
        self.group_df: pd.DataFrame | None = None

        self.notes_db = NotesDB(Path("data") / "cyclecount_notes.db")

        root = QWidget()
        self.setCentralWidget(root)
        root_layout = QVBoxLayout(root)

        # ---------- TOP CONTROLS ----------
        top = QWidget()
        top_layout = QHBoxLayout(top)

        self.btn_load_locations = QPushButton("Load Warehouse Locations")
        self.btn_load_recount = QPushButton("Load Recount File")
        self.btn_build = QPushButton("Build Review")
        self.btn_export = QPushButton("Export XLSX")

        self.btn_build.setEnabled(False)
        self.btn_export.setEnabled(False)

        self.session_id = QLineEdit()
        self.session_id.setPlaceholderText("SessionId (e.g. 20260106)")

        top_layout.addWidget(self.btn_load_locations)
        top_layout.addWidget(self.btn_load_recount)
        top_layout.addWidget(QLabel("Session:"))
        top_layout.addWidget(self.session_id)
        top_layout.addWidget(self.btn_build)
        top_layout.addWidget(self.btn_export)

        root_layout.addWidget(top)

        # ---------- STATUS ----------
        self.status_label = QLabel("Load both files to begin.")
        root_layout.addWidget(self.status_label)

        # ---------- TABLE ----------
        splitter = QSplitter(Qt.Horizontal)

        self.table = QTableWidget()
        self.table.setSortingEnabled(True)
        splitter.addWidget(self.table)

        details = QLabel("Group details will go here later.")
        details.setStyleSheet("color: gray;")
        splitter.addWidget(details)

        splitter.setStretchFactor(0, 3)
        splitter.setStretchFactor(1, 2)
        root_layout.addWidget(splitter, 1)

        # ---------- SIGNALS ----------
        self.btn_load_locations.clicked.connect(self._pick_locations)
        self.btn_load_recount.clicked.connect(self._pick_recount)
        self.btn_build.clicked.connect(self._build_review)
        self.btn_export.clicked.connect(self._export_xlsx)

        self.table.itemChanged.connect(self._on_item_changed)
        self._updating_table = False

    # ---------- FILE PICKERS ----------
    def _pick_locations(self) -> None:
        path, _ = QFileDialog.getOpenFileName(self, "Select Warehouse Locations", "", "Excel (*.xlsx)")
        if path:
            self.paths.warehouse_locations_path = Path(path)
            self._update_ready_state()

    def _pick_recount(self) -> None:
        path, _ = QFileDialog.getOpenFileName(self, "Select Recount File", "", "Excel (*.xlsx)")
        if path:
            self.paths.recount_path = Path(path)
            if not self.session_id.text().strip():
                self.session_id.setText(self.paths.recount_path.stem[:8])
            self._update_ready_state()

    def _update_ready_state(self) -> None:
        ready = self.paths.warehouse_locations_path and self.paths.recount_path
        self.btn_build.setEnabled(bool(ready))

    # ---------- BUILD REVIEW ----------
    def _build_review(self) -> None:
        sid = self.session_id.text().strip()
        if not sid:
            QMessageBox.warning(self, "Missing SessionId", "Enter a SessionId.")
            return

        try:
            loc_df = load_warehouse_locations(self.paths.warehouse_locations_path)
            rec_df = load_recount_workbook(self.paths.recount_path)

            review_df = build_review_lines(sid, rec_df, loc_df)
            review_df, transfers_df, group_df = apply_recommendations(review_df)

            # merge notes
            notes = self.notes_db.read_notes_for_session(sid)
            review_df["UserNotes"] = ""
            review_df["NoteUpdatedAt"] = ""

            for i in range(len(review_df)):
                key = NoteKey(
                    sid,
                    str(review_df.at[i, "Whs"]),
                    str(review_df.at[i, "Item"]),
                    str(review_df.at[i, "Batch/lot"]),
                    str(review_df.at[i, "Location"]),
                )
                if key in notes:
                    review_df.at[i, "UserNotes"] = notes[key][0]
                    review_df.at[i, "NoteUpdatedAt"] = notes[key][1]

            self.review_df = review_df
            self.transfers_df = transfers_df
            self.group_df = group_df

            self._set_table_from_df(review_df)
            self.btn_export.setEnabled(True)

            self.status_label.setText(
                f"Rows: {len(review_df):,} | Transfers: {len(transfers_df):,} | Groups: {len(group_df):,}"
            )

        except Exception as e:
            QMessageBox.critical(self, "Build failed", str(e))

    # ---------- TABLE RENDER ----------
    def _set_table_from_df(self, df: pd.DataFrame) -> None:
        self._updating_table = True
        try:
            headers = list(df.columns)
            self.table.setColumnCount(len(headers))
            self.table.setRowCount(len(df))
            self.table.setHorizontalHeaderLabels(headers)

            self._notes_col = headers.index("UserNotes")

            for r in range(len(df)):
                for c, col in enumerate(headers):
                    item = QTableWidgetItem(str(df.iat[r, c]) if pd.notna(df.iat[r, c]) else "")
                    if c == self._notes_col:
                        item.setFlags(item.flags() | Qt.ItemIsEditable)
                    else:
                        item.setFlags(item.flags() & ~Qt.ItemIsEditable)
                    self.table.setItem(r, c, item)

            self.table.resizeColumnsToContents()
        finally:
            self._updating_table = False

    # ---------- NOTES SAVE ----------
    def _on_item_changed(self, item: QTableWidgetItem) -> None:
        if self._updating_table or item.column() != self._notes_col:
            return

        r = item.row()
        sid = self.session_id.text().strip()

        key = NoteKey(
            sid,
            str(self.review_df.at[r, "Whs"]),
            str(self.review_df.at[r, "Item"]),
            str(self.review_df.at[r, "Batch/lot"]),
            str(self.review_df.at[r, "Location"]),
        )

        ts = self.notes_db.upsert_note(key, item.text())
        self.review_df.at[r, "UserNotes"] = item.text()
        self.review_df.at[r, "NoteUpdatedAt"] = ts

    # ---------- EXPORT ----------
    def _export_xlsx(self) -> None:
        path, _ = QFileDialog.getSaveFileName(self, "Save Export", "", "Excel (*.xlsx)")
        if not path:
            return
        export_workbook(Path(path), self.review_df, self.group_df, self.transfers_df)
        QMessageBox.information(self, "Export Complete", f"Saved to:\n{path}")


def run_app() -> None:
    app = QApplication([])
    win = MainWindow()
    win.show()
    app.exec()

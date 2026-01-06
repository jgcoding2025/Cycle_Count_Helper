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
        self.top_layout = QHBoxLayout(top)
        self.top_layout.setContentsMargins(0, 0, 0, 0)

        self.btn_load_locations = QPushButton("Load Warehouse Locations.xlsx")
        self.btn_load_recount = QPushButton("Load Recount Workbook.xlsx")
        self.btn_build_review = QPushButton("Build Review Table")
        self.btn_build_review.setEnabled(False)
        self.btn_export = QPushButton("Export XLSX")
        self.btn_export.setEnabled(False)

        self.session_id = QLineEdit()
        self.session_id.setPlaceholderText("SessionId (e.g. 20260106)")

        self.top_layout.addWidget(self.btn_load_locations)
        self.top_layout.addWidget(self.btn_load_recount)
        self.top_layout.addWidget(QLabel("SessionId:"))
        self.top_layout.addWidget(self.session_id, 1)
        self.top_layout.addWidget(self.btn_build_review)
        self.top_layout.addWidget(self.btn_export)

        root_layout.addWidget(top)

        filters = QWidget()
        filters_layout = QHBoxLayout(filters)

        self.filter_search = QLineEdit()
        self.filter_search.setPlaceholderText("Search Item / Location / Tag / Description...")

        self.btn_show_all = QPushButton("Show All")
        self.btn_show_actions = QPushButton("Only Actions (Adjust/Transfer/Investigate)")
        self.btn_show_secured = QPushButton("Only Secured Variance")
        self.btn_show_investigate = QPushButton("Only Investigate")

        filters_layout.addWidget(QLabel("Filter:"))
        filters_layout.addWidget(self.filter_search, 1)
        filters_layout.addWidget(self.btn_show_all)
        filters_layout.addWidget(self.btn_show_actions)
        filters_layout.addWidget(self.btn_show_secured)
        filters_layout.addWidget(self.btn_show_investigate)

        root_layout.addWidget(filters)


        # ---------- STATUS ----------
        self.status_label = QLabel("Load both files to begin.")
        root_layout.addWidget(self.status_label)

        # ---------- TABLE ----------
        splitter = QSplitter(Qt.Horizontal)

        self.table = QTableWidget()
        self.table.setSortingEnabled(True)
        splitter.addWidget(self.table)

        self.details = QLabel("Click a row to see group details.")
        self.details.setWordWrap(True)
        self.details.setStyleSheet("color: #cccccc;")
        splitter.addWidget(self.details)

        self.table.itemSelectionChanged.connect(self._on_selection_changed)

        splitter.setStretchFactor(0, 3)
        splitter.setStretchFactor(1, 2)
        root_layout.addWidget(splitter, 1)

        # Wire events
        self.btn_load_locations.clicked.connect(self._pick_locations_file)
        self.btn_load_recount.clicked.connect(self._pick_recount_file)
        self.btn_build_review.clicked.connect(self._build_review_placeholder)
        self.btn_export.clicked.connect(self._export_xlsx)

        self._filter_mode = "ALL"


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

            # Step 3: recommendations + transfer plan + group summary
            review_df, transfers_df, group_df = apply_recommendations(review_df)

            # Merge in persisted notes
            notes_map = self.notes_db.read_notes_for_session(sid)

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

    def _on_selection_changed(self) -> None:
        if self.review_df is None or self.group_df is None:
            return
        items = self.table.selectedItems()
        if not items:
            return

        r = items[0].row()

        whs = str(self.review_df.at[r, "Whs"])
        item = str(self.review_df.at[r, "Item"])
        lot = str(self.review_df.at[r, "Batch/lot"])
        loc = str(self.review_df.at[r, "Location"])
        default_loc = str(self.review_df.at[r, "DefaultLocation"])

        headline = str(self.review_df.at[r, "GroupHeadline"])
        rec_type = str(self.review_df.at[r, "RecommendationType"])
        reason = str(self.review_df.at[r, "Reason"])
        remaining = self.review_df.at[r, "RemainingAdjustmentQty"]
        conf = str(self.review_df.at[r, "Confidence"])
        sev = str(self.review_df.at[r, "Severity"])

        # group row lookup
        gmatch = self.group_df[
            (self.group_df["Whs"] == whs) &
            (self.group_df["Item"] == item) &
            (self.group_df["Batch/lot"] == lot)
        ]
        if not gmatch.empty:
            g = gmatch.iloc[0].to_dict()
            sys_total = g.get("SystemTotal", "")
            count_total = g.get("CountTotal", "")
            net_var = g.get("NetVariance", "")
            st01 = g.get("SysST01", "")
            def_after = g.get("DefaultSystemAfter", "")
            def_count = g.get("DefaultCount", "")
            flags = g.get("Flags", "")
        else:
            sys_total = count_total = net_var = st01 = def_after = def_count = flags = ""

        # transfer lines for this group
        transfer_text = ""
        if self.transfers_df is not None and not self.transfers_df.empty:
            t = self.transfers_df[
                (self.transfers_df["Whs"] == whs) &
                (self.transfers_df["Item"] == item) &
                (self.transfers_df["Batch/lot"] == lot)
            ]
            if not t.empty:
                lines = []
                for _, tr in t.iterrows():
                    lines.append(f"- {tr['Qty']} : {tr['FromLocation']} → {tr['ToLocation']}")
                transfer_text = "\n".join(lines)

        self.details.setText(
            f"Group: {whs} | {item} | {lot}\n"
            f"Selected Location: {loc}\n"
            f"Default: {default_loc}\n\n"
            f"Headline: {headline}\n"
            f"RecommendationType (row): {rec_type}\n"
            f"RemainingAdjustmentQty (group): {remaining}\n"
            f"Confidence: {conf} | Severity: {sev}\n\n"
            f"Totals: System={sys_total}  Count={count_total}  NetVar={net_var}\n"
            f"ST01(System)={st01}  DefaultAfterTransfers={def_after}  DefaultCount={def_count}\n"
            f"Flags: {flags}\n\n"
            f"Transfers:\n{transfer_text if transfer_text else '(none)'}\n\n"
            f"Reason:\n{reason}"
        )

    def _set_filter_mode(self, mode: str) -> None:
        self._filter_mode = mode
        self._apply_filters()

    def _apply_filters(self) -> None:
        if self.review_df is None:
            return

        df = self.review_df.copy()

        mode = getattr(self, "_filter_mode", "ALL")
        if mode == "ACTIONS":
            df = df[df["RecommendationType"].isin(["TRANSFER", "ADJUST", "INVESTIGATE"])]
        elif mode == "SECURED":
            # secured variance shows up as headline or flags; easiest filter is Location Type + Variance
            if "Location Type" in df.columns:
                df = df[(df["Location Type"].astype(str).str.lower() == "secured") & (df["VarianceQty"] != 0)]
        elif mode == "INVESTIGATE":
            df = df[df["RecommendationType"] == "INVESTIGATE"]

        q = self.filter_search.text().strip().lower()
        if q:
            cols = [c for c in ["Item", "Location", "Tag", "Description", "GroupHeadline"] if c in df.columns]
            if cols:
                mask = False
                for c in cols:
                    mask = mask | df[c].astype(str).str.lower().str.contains(q, na=False)
                df = df[mask]

        # Re-render table (keeps notes editing)
        self._set_table_from_df(df)


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

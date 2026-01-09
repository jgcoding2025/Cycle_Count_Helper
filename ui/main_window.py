from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re

import pandas as pd

from PySide6.QtCore import Qt, Signal, QSettings
from PySide6.QtGui import QFont
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
    QCheckBox,
    QMessageBox,
    QSplitter,
    QGroupBox,
    QFormLayout,
    QTextEdit,
    QTabWidget,
    QDialog,
    QHeaderView,
    QSizePolicy,
    QScrollArea,
    QComboBox,
)

from core.io_excel import load_warehouse_locations, load_recount_workbook
from core.review_builder import build_review_lines
from core.recommender import apply_recommendations
from core.notes_db import NotesDB, NoteKey
from core.exporter import export_workbook
from datetime import datetime

@dataclass
class LoadedPaths:
    warehouse_locations_path: Path | None = None
    recount_path: Path | None = None


class FilterHeader(QHeaderView):
    filterChanged = Signal(int, str)

    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(Qt.Horizontal, parent)
        self._filters: list[QComboBox] = []
        self._filter_height = 24
        self._all_label = "(All)"
        self.sectionResized.connect(self._position_filters)
        self.sectionMoved.connect(self._position_filters)
        self.geometriesChanged.connect(self._position_filters)

    def sizeHint(self):
        size = super().sizeHint()
        size.setHeight(size.height() + self._filter_height + 4)
        return size

    def filter_count(self) -> int:
        return len(self._filters)

    def set_filter_count(self, count: int, placeholders: list[str]) -> None:
        for editor in self._filters:
            editor.deleteLater()
        self._filters = []
        for i in range(count):
            editor = QComboBox(self)
            editor.setEditable(True)
            editor.setInsertPolicy(QComboBox.NoInsert)
            editor.setSizeAdjustPolicy(QComboBox.AdjustToContents)
            editor.lineEdit().setPlaceholderText(placeholders[i])
            editor.addItem(self._all_label)
            editor.lineEdit().textEdited.connect(lambda text, idx=i: self._emit_filter_changed(idx))
            editor.currentIndexChanged.connect(lambda _, idx=i: self._emit_filter_changed(idx))
            self._filters.append(editor)
        self._position_filters()

    def set_filter_placeholders(self, placeholders: list[str]) -> None:
        for editor, placeholder in zip(self._filters, placeholders):
            editor.lineEdit().setPlaceholderText(placeholder)

    def set_filter_options(self, options: list[list[str]]) -> None:
        for editor, values in zip(self._filters, options):
            current = editor.currentText()
            editor.blockSignals(True)
            editor.clear()
            editor.addItem(self._all_label)
            for value in values:
                editor.addItem(value)
            if current and current != self._all_label:
                if current in values:
                    editor.setCurrentText(current)
                else:
                    editor.setEditText(current)
            else:
                editor.setCurrentIndex(0)
            editor.blockSignals(False)

    def _emit_filter_changed(self, index: int) -> None:
        if index >= len(self._filters):
            return
        text = self._filters[index].currentText().strip()
        if text == self._all_label:
            text = ""
        self.filterChanged.emit(index, text)

    def _position_filters(self) -> None:
        base_height = super().sizeHint().height()
        for i, editor in enumerate(self._filters):
            if self.isSectionHidden(i):
                editor.hide()
                continue
            editor.show()
            rect = self.sectionViewportPosition(i)
            editor.setGeometry(
                rect + 2,
                base_height + 2,
                max(0, self.sectionSize(i) - 4),
                self._filter_height,
            )


class MainWindow(QMainWindow):
    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle("Cycle Count Assistant")
        self.resize(1300, 800)
        self._dark_mode_enabled = False

        self.paths = LoadedPaths()
        self.review_df: pd.DataFrame | None = None
        self.transfers_df: pd.DataFrame | None = None
        self.group_df: pd.DataFrame | None = None
        self.test_results_df: pd.DataFrame | None = None
        self.locations_cache_path = Path("data") / "warehouse_locations_saved.xlsx"
        self.locations_loaded_at: datetime | None = None
        self.settings = QSettings("CycleCountAssistant", "CycleCountAssistant")
        self._hidden_columns = self._load_hidden_columns()

        self.notes_db = NotesDB(Path("data") / "cyclecount_notes.db")

        base_font = QFont()
        base_font.setPointSize(12)
        self.setFont(base_font)

        tabs = QTabWidget()
        self.setCentralWidget(tabs)

        main_tab = QWidget()
        tabs.addTab(main_tab, "Main")
        root_layout = QVBoxLayout(main_tab)

        test_tab = QWidget()
        tabs.addTab(test_tab, "Test Scenario")
        test_layout_root = QVBoxLayout(test_tab)

        reference_tab = QWidget()
        tabs.addTab(reference_tab, "Reference")
        reference_layout = QVBoxLayout(reference_tab)
        self.chk_reference_transfer_pref = QCheckBox("Recommend transfers prior to adjustments")
        reference_layout.addWidget(self.chk_reference_transfer_pref)
        self.rules_text = QTextEdit()
        self.rules_text.setReadOnly(True)
        reference_layout.addWidget(self.rules_text)

        settings_tab = QWidget()
        tabs.addTab(settings_tab, "Settings")
        settings_layout = QVBoxLayout(settings_tab)
        settings_group = QGroupBox("Preferences")
        settings_group_layout = QVBoxLayout(settings_group)
        settings_group_layout.setSpacing(12)

        locations_group = QGroupBox("Warehouse Locations")
        locations_layout = QVBoxLayout(locations_group)

        # ---------- TOP CONTROLS ----------
        top = QWidget()
        self.top_layout = QHBoxLayout(top)
        self.top_layout.setContentsMargins(0, 0, 0, 0)

        self.btn_load_locations = QPushButton("Load Warehouse Locations.xlsx")
        self.btn_load_recount = QPushButton("Load Count Sheet.xlsx")
        self.btn_build_review = QPushButton("Build Review Table")
        self.btn_build_review.setEnabled(False)
        self.btn_export = QPushButton("Export XLSX")
        self.btn_export.setEnabled(False)
        self.chk_recommend_transfers = QCheckBox("Recommend transfers prior to adjustments")
        self.chk_dark_mode = QCheckBox("Dark Mode")

        self.session_id = QLineEdit()
        self.session_id.setPlaceholderText("SessionId (e.g. 20260106)")
        self.session_id.setText(datetime.now().strftime("%Y%m%d_%H%M%S"))
        self.session_id.setFixedWidth(170)

        self.btn_load_recount.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
        self.btn_build_review.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
        self.btn_export.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)

        session_container = QWidget()
        session_layout = QHBoxLayout(session_container)
        session_layout.setContentsMargins(0, 0, 0, 0)
        session_layout.setSpacing(6)
        session_layout.addWidget(QLabel("Session ID:"))
        session_layout.addWidget(self.session_id, 1)

        self.top_layout.addWidget(session_container, 1)
        self.top_layout.addWidget(self.btn_load_recount)
        self.top_layout.addWidget(self.btn_build_review)
        self.top_layout.addWidget(self.btn_export)

        root_layout.addWidget(top)

        self.btn_view_locations = QPushButton("View Loaded Warehouse Locations")
        self.btn_view_locations.setEnabled(False)

        self.status_label = QLabel("Load both files to begin.")
        self.status_label.setStyleSheet("font-weight: 600; color: #1f2933; padding: 4px;")

        locations_layout.addWidget(self.btn_load_locations)
        locations_layout.addWidget(self.btn_view_locations)
        locations_layout.addWidget(self.status_label)

        settings_group_layout.addWidget(locations_group)
        settings_group_layout.addWidget(self.chk_recommend_transfers)
        settings_group_layout.addWidget(self.chk_dark_mode)

        settings_layout.addWidget(settings_group)
        settings_layout.addStretch(1)

        filters = QWidget()
        filters_layout = QHBoxLayout(filters)

        self.filter_search = QLineEdit()
        self.filter_search.setPlaceholderText("Search Item / Location / Tag / Description...")

        filters_layout.addWidget(QLabel("Filter:"))
        filters_layout.addWidget(self.filter_search, 1)

        root_layout.addWidget(filters)

        # ---------- TABLE ----------
        splitter = QSplitter(Qt.Horizontal)
        splitter.setHandleWidth(1)

        self.table = QTableWidget()
        self.table.setSortingEnabled(True)
        self.table.setAlternatingRowColors(True)
        self.table_header = FilterHeader(self.table)
        self.table.setHorizontalHeader(self.table_header)
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeToContents)
        self.table_header.filterChanged.connect(self._on_header_filter_changed)
        splitter.addWidget(self.table)

        self.details = QLabel("Click a row to see group details.")
        self.details.setWordWrap(True)
        self.details.setStyleSheet("color: #3e4c59;")

        self.column_selector_group = QGroupBox("Columns")
        column_selector_layout = QVBoxLayout(self.column_selector_group)
        column_selector_layout.setContentsMargins(6, 6, 6, 6)
        column_selector_layout.setSpacing(6)
        self.column_selector_scroll = QScrollArea()
        self.column_selector_scroll.setWidgetResizable(True)
        self.column_selector_content = QWidget()
        self.column_selector_content_layout = QVBoxLayout(self.column_selector_content)
        self.column_selector_content_layout.setSpacing(4)
        self.column_selector_scroll.setWidget(self.column_selector_content)
        column_selector_layout.addWidget(self.column_selector_scroll)

        right_panel = QWidget()
        right_layout = QVBoxLayout(right_panel)
        right_layout.setContentsMargins(0, 0, 0, 0)
        right_layout.setSpacing(12)
        right_layout.addWidget(self.details)
        right_layout.addWidget(self.column_selector_group, 1)

        splitter.addWidget(right_panel)

        splitter.setStretchFactor(0, 3)
        splitter.setStretchFactor(1, 2)
        root_layout.addWidget(splitter, 1)

        # ---------- TEST SCENARIO ----------
        test_group = QGroupBox("Test Scenario")
        test_layout = QVBoxLayout(test_group)

        test_content_layout = QHBoxLayout()
        default_form = QFormLayout()
        self.chk_test_transfer_pref = QCheckBox("Recommend transfers prior to adjustments")
        self.test_default_whs = QLineEdit()
        self.test_default_loc = QLineEdit()
        self.test_default_system = QLineEdit()
        self.test_default_count = QLineEdit()
        self.test_st01_system = QLineEdit()

        for field in (
            self.test_default_whs,
            self.test_default_loc,
            self.test_default_system,
            self.test_default_count,
            self.test_st01_system,
        ):
            field.setMaximumWidth(160)
            field.setMinimumWidth(100)

        self.test_default_whs.setPlaceholderText("Warehouse")
        self.test_default_loc.setPlaceholderText("Default")
        self.test_default_system.setPlaceholderText("System Qty")
        self.test_default_count.setPlaceholderText("Counted Qty")
        self.test_st01_system.setPlaceholderText("ST01 Qty")

        default_form.addRow("Warehouse:", self.test_default_whs)
        default_form.addRow("Default Location (A):", self.test_default_loc)
        default_form.addRow("System Qty for ST01:", self.test_st01_system)
        default_form.addRow(QLabel("System Qty:"), self.test_default_system)
        default_form.addRow(QLabel("Counted Qty:"), self.test_default_count)
        default_form.addRow("", self.chk_test_transfer_pref)

        default_container = QWidget()
        default_container.setLayout(default_form)

        secondary_container = QWidget()
        secondary_layout = QVBoxLayout(secondary_container)
        secondary_layout.setContentsMargins(0, 0, 0, 0)
        secondary_label = QLabel("Secondary Locations (up to 5)")
        secondary_layout.addWidget(secondary_label)

        self.test_secondary_table = QTableWidget(5, 4)
        self.test_secondary_table.setHorizontalHeaderLabels([
            "Warehouse",
            "Secondary Location (B+)",
            "System Qty",
            "Counted Qty",
        ])
        self.test_secondary_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeToContents)
        self.test_secondary_table.verticalHeader().setVisible(False)
        self.test_secondary_table.setCornerButtonEnabled(False)
        secondary_layout.addWidget(self.test_secondary_table)

        test_content_layout.addWidget(default_container, 1)
        test_content_layout.addWidget(secondary_container, 2)
        test_layout.addLayout(test_content_layout)

        controls_layout = QHBoxLayout()
        self.btn_run_test = QPushButton("Run Test Scenario")
        controls_layout.addWidget(self.btn_run_test)
        controls_layout.addStretch(1)
        test_layout.addLayout(controls_layout)

        self.test_results_table = QTableWidget()
        self.test_results_table.setSortingEnabled(True)
        self.test_results_table.setAlternatingRowColors(True)
        self.test_results_table.setWordWrap(True)
        self.test_results_table.setTextElideMode(Qt.ElideNone)
        self.test_results_table.verticalHeader().setVisible(False)
        self.test_results_table.setCornerButtonEnabled(False)
        test_layout.addWidget(self.test_results_table)

        test_layout_root.addWidget(test_group)

        # ---------- SIGNALS ----------
        self.btn_load_locations.clicked.connect(self._pick_locations)
        self.btn_load_recount.clicked.connect(self._pick_recount)
        self.btn_build_review.clicked.connect(self._build_review)
        self.btn_export.clicked.connect(self._export_xlsx)
        self.btn_run_test.clicked.connect(self._run_test_scenario)
        self.table.itemSelectionChanged.connect(self._on_selection_changed)
        self.table.itemChanged.connect(self._on_item_changed)
        self.chk_dark_mode.stateChanged.connect(self._toggle_theme)
        self.btn_view_locations.clicked.connect(self._show_loaded_locations)

        self.filter_search.textChanged.connect(self._apply_filters)
        self._column_filters: dict[int, str] = {}
        self._table_headers: list[str] = []
        self._updating_table = False
        self._apply_ui_theme()
        self._load_saved_locations_if_available()
        self._update_ready_state()
        self._syncing_transfer_pref = False
        self._bind_transfer_preference_controls()
        self._update_rules_text()
        self._update_column_selector([])

    # ---------- FILE PICKERS ----------
    def _pick_locations(self) -> None:
        path, _ = QFileDialog.getOpenFileName(self, "Select Warehouse Locations", "", "Excel (*.xlsx)")
        if path:
            self.paths.warehouse_locations_path = Path(path)
            self._save_locations_to_cache(self.paths.warehouse_locations_path)
            self.paths.warehouse_locations_path = self.locations_cache_path
            self._update_ready_state()

    def _pick_recount(self) -> None:
        path, _ = QFileDialog.getOpenFileName(self, "Select Count Sheet", "", "Excel (*.xlsx)")
        if path:
            self.paths.recount_path = Path(path)
            if not self.session_id.text().strip():
                self.session_id.setText(self.paths.recount_path.stem[:8])
            self._update_ready_state()

    def _update_ready_state(self) -> None:
        ready = self.paths.warehouse_locations_path and self.paths.recount_path
        self.btn_build_review.setEnabled(bool(ready))
        self.btn_view_locations.setEnabled(self.paths.warehouse_locations_path is not None)
        locations_state = "Loaded" if self.paths.warehouse_locations_path else "Missing"
        recount_state = "Loaded" if self.paths.recount_path else "Missing"
        self.status_label.setText(f"Warehouse Locations: {locations_state} | Count Sheet: {recount_state}")

    def _load_saved_locations_if_available(self) -> None:
        if self.locations_cache_path.exists():
            self.paths.warehouse_locations_path = self.locations_cache_path
            self.btn_load_locations.setText("Replace Warehouse Locations.xlsx")
            self.locations_loaded_at = datetime.fromtimestamp(self.locations_cache_path.stat().st_mtime)

    def _save_locations_to_cache(self, source_path: Path) -> None:
        self.locations_cache_path.parent.mkdir(parents=True, exist_ok=True)
        if source_path.resolve() == self.locations_cache_path.resolve():
            return
        self.locations_cache_path.write_bytes(source_path.read_bytes())
        self.btn_load_locations.setText("Replace Warehouse Locations.xlsx")
        self.locations_loaded_at = datetime.now()

    def _show_loaded_locations(self) -> None:
        if not self.paths.warehouse_locations_path:
            QMessageBox.warning(self, "No Locations Loaded", "Load Warehouse Locations before viewing.")
            return

        try:
            loc_df = load_warehouse_locations(self.paths.warehouse_locations_path)
        except Exception as e:
            QMessageBox.critical(self, "Load Error", str(e))
            return

        dialog = QDialog(self)
        dialog.setWindowTitle("Loaded Warehouse Locations")
        dialog.resize(900, 600)
        dialog_layout = QVBoxLayout(dialog)

        loaded_at = self.locations_loaded_at
        if loaded_at is None and self.locations_cache_path.exists():
            loaded_at = datetime.fromtimestamp(self.locations_cache_path.stat().st_mtime)
        loaded_label = QLabel(
            f"Last loaded: {loaded_at.strftime('%Y-%m-%d %H:%M:%S') if loaded_at else 'Unknown'}"
        )
        dialog_layout.addWidget(loaded_label)

        table = QTableWidget()
        table.setAlternatingRowColors(True)
        table.verticalHeader().setVisible(False)
        table.setCornerButtonEnabled(False)
        headers = [str(c) for c in loc_df.columns]
        table.setColumnCount(len(headers))
        table.setRowCount(len(loc_df))
        table.setHorizontalHeaderLabels(headers)
        table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)

        for r in range(len(loc_df)):
            row = loc_df.iloc[r]
            for c, col in enumerate(headers):
                val = row[col]
                item = QTableWidgetItem("" if pd.isna(val) else str(val))
                item.setFlags(item.flags() & ~Qt.ItemIsEditable)
                table.setItem(r, c, item)

        dialog_layout.addWidget(table, 1)
        dialog.exec()

    def _bind_transfer_preference_controls(self) -> None:
        self.chk_recommend_transfers.stateChanged.connect(
            lambda: self._sync_transfer_preference(self.chk_recommend_transfers.isChecked(), "main")
        )
        self.chk_test_transfer_pref.stateChanged.connect(
            lambda: self._sync_transfer_preference(self.chk_test_transfer_pref.isChecked(), "test")
        )
        self.chk_reference_transfer_pref.stateChanged.connect(
            lambda: self._sync_transfer_preference(self.chk_reference_transfer_pref.isChecked(), "reference")
        )
        self._sync_transfer_preference(self.chk_recommend_transfers.isChecked(), "main")

    def _sync_transfer_preference(self, checked: bool, source: str) -> None:
        if self._syncing_transfer_pref:
            return
        self._syncing_transfer_pref = True
        try:
            if source != "main":
                self.chk_recommend_transfers.setChecked(checked)
            if source != "test":
                self.chk_test_transfer_pref.setChecked(checked)
            if source != "reference":
                self.chk_reference_transfer_pref.setChecked(checked)
            self._update_rules_text()
        finally:
            self._syncing_transfer_pref = False

    def _update_rules_text(self) -> None:
        mode_line = (
            "<li><strong>Transfer mode:</strong> explicit From/To moves are recommended to reconcile secondary variances.</li>"
            if self.chk_recommend_transfers.isChecked()
            else "<li><strong>Adjustment mode:</strong> transfer transactions are suppressed, but transfer math is preserved and MOVE + RECOUNT may still be recommended.</li>"
        )
        self.rules_text.setHtml(
            "<h3>Recommendation logic summary</h3>"
            "<ol>"
            "<li>Grouped by Warehouse + Item + Batch/lot; for WHS 50, secondaries are evaluated first, then default.</li>"
            "<li>Warehouses other than 50 are still processed: every non-ST01 location is treated like a secondary and adjusted to match physical counts (no default updates or transfers).</li>"
            "<li>ST01 is system-only and never counted; it is only used as a tolerance buffer for WHS 50 unsecured+available defaults.</li>"
            "<li>Secured locations with any variance are always direct ADJUST to match the physical count (no transfers, no ST01 tolerance).</li>"
            "<li>If a secondary is over system and the default is unverified/empty or ST01 exists, recommend MOVE + RECOUNT (avoid net adjustments).</li>"
            "<li><strong>Default rules (WHS 50 only):</strong>"
            "<ol type=\"a\">"
            "<li>If Default Count = 0 and default is unsecured+available with ST01 system qty &gt; 0, no default-empty issue; note that default may be physically full.</li>"
            "<li>If default is unsecured+available, compute MIN = default-system-after-moves and MAX = MIN + SysST01; "
            "within [MIN, MAX], snap the target entry to MIN.</li>"
            "<li>If Default Count &lt; MIN, recommend adding only enough to reach MIN; if Default Count &gt; MAX, recommend removing only enough to reach MAX.</li>"
            "<li>Non-eligible defaults compare directly to system-after-moves.</li>"
            "</ol>"
            "</li>"
            f"{mode_line}"
            "</ol>"
        )

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
            transfer_mode = "TRANSFER" if self.chk_recommend_transfers.isChecked() else "ADJUST"
            review_df, transfers_df, group_df = apply_recommendations(review_df, transfer_mode=transfer_mode)

            original_columns = list(rec_df.columns) + [c for c in loc_df.columns if c not in rec_df.columns]
            missing_columns = [c for c in original_columns if c not in review_df.columns]
            if missing_columns:
                raise ValueError(f"Columns lost while building review table: {missing_columns}")

            # Merge in persisted notes
            notes = self.notes_db.read_notes_for_session(sid)

            review_df["UserNotes"] = ""
            review_df["NoteUpdatedAt"] = ""

            for i in range(len(review_df)):
                whs = str(review_df.at[i, "Whs"])
                item_code = str(review_df.at[i, "Item"])
                lot = str(review_df.at[i, "Batch/lot"])
                loc = str(review_df.at[i, "Location"])
                key = NoteKey(sid, whs, item_code, lot, loc)
                if key in notes:
                    note_text, updated = notes[key]
                    review_df.at[i, "UserNotes"] = note_text
                    review_df.at[i, "NoteUpdatedAt"] = updated


            self.review_df = review_df
            self.transfers_df = transfers_df
            self.group_df = group_df

        except Exception as e:
            self.btn_export.setEnabled(False)
            QMessageBox.critical(self, "Load/Build Error", str(e))
            return

        self._apply_filters()
        self.btn_export.setEnabled(True)

    def _build_test_recount_df(self) -> pd.DataFrame:
        rows = []
        whs = self.test_default_whs.text().strip()
        default_loc = self.test_default_loc.text().strip().upper()
        if not whs or not default_loc:
            raise ValueError("Test Scenario requires Warehouse and Default Location (A).")

        default_system = float(self.test_default_system.text().strip() or 0)
        default_count = float(self.test_default_count.text().strip() or 0)

        def _add_row(row_whs: str, loc: str, sys_qty: float, count_qty: float) -> None:
            rows.append({
                "Whs": row_whs,
                "Item": "TEST_ITEM",
                "Location": loc,
                "Batch/lot": "",
                "Item Rev Default Location": default_loc,
                "Count 1 cutoff on-hand qty": sys_qty,
                "Count 1 qty": count_qty,
                "Count 1 variance qty": count_qty - sys_qty,
            })

        _add_row(whs, default_loc, default_system, default_count)

        st01_value = self.test_st01_system.text().strip()
        if st01_value:
            st01_system = float(st01_value)
            _add_row(whs, "ST01", st01_system, st01_system)

        for r in range(self.test_secondary_table.rowCount()):
            row_whs_item = self.test_secondary_table.item(r, 0)
            loc_item = self.test_secondary_table.item(r, 1)
            sys_item = self.test_secondary_table.item(r, 2)
            count_item = self.test_secondary_table.item(r, 3)

            row_whs = row_whs_item.text().strip() if row_whs_item else ""
            loc = loc_item.text().strip().upper() if loc_item else ""
            if not loc:
                continue
            if not row_whs:
                row_whs = whs

            sys_qty = float(sys_item.text().strip()) if sys_item and sys_item.text().strip() else 0
            count_qty = float(count_item.text().strip()) if count_item and count_item.text().strip() else 0
            _add_row(row_whs, loc, sys_qty, count_qty)

        if not rows:
            raise ValueError("Enter at least one default or secondary location row.")

        return pd.DataFrame(rows)

    def _run_test_scenario(self) -> None:
        if not self.paths.warehouse_locations_path:
            QMessageBox.warning(self, "Missing Locations", "Load Warehouse Locations before running a test scenario.")
            return

        try:
            loc_df = load_warehouse_locations(self.paths.warehouse_locations_path)
            rec_df = self._build_test_recount_df()
            review_df = build_review_lines("TEST", rec_df, loc_df)
            transfer_mode = "TRANSFER" if self.chk_recommend_transfers.isChecked() else "ADJUST"
            review_df, _, _ = apply_recommendations(review_df, transfer_mode=transfer_mode)
        except Exception as e:
            QMessageBox.critical(self, "Test Scenario Error", str(e))
            return

        result_cols = [
            "Location",
            "SystemQty",
            "CountQty",
            "VarianceQty",
            "RecommendationType",
            "RecommendedQty",
            "RemainingAdjustmentQty",
            "GroupHeadline",
            "Reason",
        ]
        available_cols = [c for c in result_cols if c in review_df.columns]
        results = review_df[available_cols].copy()
        self.test_results_df = results
        self._refresh_test_results()

    def _set_test_table_from_df(self, df: "pd.DataFrame") -> None:
        headers = [str(c) for c in df.columns]
        display_headers = [self._format_header(c) for c in headers]
        self.test_results_table.clear()
        self.test_results_table.setColumnCount(len(headers))
        self.test_results_table.setRowCount(len(df))
        self.test_results_table.setHorizontalHeaderLabels(display_headers)

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
                item.setFlags(item.flags() & ~Qt.ItemIsEditable)
                self.test_results_table.setItem(r, c, item)

        self.test_results_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeToContents)
        self.test_results_table.resizeColumnsToContents()
        self.test_results_table.resizeRowsToContents()

    def _set_table(self, headers: list[str], rows: list[list[str]]) -> None:
        display_headers = [self._format_header(c) for c in headers]
        self.table.clear()
        self.table.setColumnCount(len(headers))
        self.table.setRowCount(len(rows))
        self.table.setHorizontalHeaderLabels(display_headers)

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
            display_headers = [self._format_header(c) for c in headers]
            self._table_headers = headers
            self.table.clear()
            self.table.setColumnCount(len(headers))
            self.table.setRowCount(len(df))
            self.table.setHorizontalHeaderLabels(display_headers)
            if self.table_header.filter_count() != len(headers):
                self._column_filters = {}
                self.table_header.set_filter_count(len(headers), display_headers)
            else:
                self.table_header.set_filter_placeholders(display_headers)

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
            self._apply_column_visibility(headers)
            self._update_column_selector(headers)
            self._update_header_filters(df)
        finally:
            self._updating_table = False

    def _format_header(self, header: str) -> str:
        label = header.replace("_", " ")
        label = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", " ", label)
        words = label.split()
        words = [word.capitalize() if word.islower() else word for word in words]
        return " ".join(words)

    def _toggle_theme(self) -> None:
        self._dark_mode_enabled = self.chk_dark_mode.isChecked()
        self._apply_ui_theme()

    def _apply_ui_theme(self) -> None:
        if self._dark_mode_enabled:
            self.setStyleSheet(
                """
                QMainWindow {
                    background-color: #111827;
                }
                QTabWidget::pane {
                    border: 0;
                }
                QTabBar::tab {
                    background-color: transparent;
                    padding: 8px 4px;
                    margin-right: 16px;
                    border: 0;
                    border-bottom: 2px solid transparent;
                    font-weight: 600;
                }
                QTabBar::tab:selected {
                    color: #93c5fd;
                    border-bottom: 2px solid #60a5fa;
                }
                QSplitter::handle {
                    background: transparent;
                }
                QWidget {
                    color: #f9fafb;
                }
                QGroupBox {
                    font-weight: 600;
                    border: 1px solid #334155;
                    border-radius: 10px;
                    margin-top: 14px;
                    padding: 10px;
                    background: #1f2937;
                }
                QGroupBox::title {
                    subcontrol-origin: margin;
                    left: 12px;
                    padding: 0 6px;
                }
                QLineEdit, QTextEdit, QTableWidget {
                    background: #111827;
                    border: 1px solid #334155;
                    border-radius: 8px;
                    padding: 6px;
                    color: #f9fafb;
                }
                QScrollArea, QScrollArea QWidget {
                    background: #111827;
                    border: 1px solid #334155;
                    border-radius: 8px;
                }
                QTableWidget {
                    gridline-color: #334155;
                    alternate-background-color: #1f2937;
                }
                QHeaderView::section {
                    background-color: #374151;
                    padding: 6px;
                    border: 1px solid #334155;
                    font-weight: 600;
                }
                QTableCornerButton::section {
                    background: #1f2937;
                    border: 1px solid #334155;
                }
                QPushButton {
                    background-color: #60a5fa;
                    color: #0b1220;
                    border-radius: 10px;
                    padding: 8px 16px;
                    font-weight: 600;
                }
                QPushButton:hover {
                    background-color: #3b82f6;
                }
                QPushButton:disabled {
                    background-color: #475569;
                    color: #e2e8f0;
                }
                QCheckBox {
                    spacing: 8px;
                }
                """
            )
            self.status_label.setStyleSheet("font-weight: 600; color: #e2e8f0; padding: 4px;")
            self.details.setStyleSheet("color: #e2e8f0;")
            self.test_secondary_table.setStyleSheet(
                """
                QTableWidget {
                    gridline-color: #334155;
                    border: 1px solid #334155;
                }
                QHeaderView::section {
                    border: 1px solid #334155;
                }
                """
            )
        else:
            self.setStyleSheet(
                """
                QMainWindow {
                    background-color: #f5f7fb;
                }
                QTabWidget::pane {
                    border: 0;
                }
                QTabBar::tab {
                    background-color: transparent;
                    padding: 8px 4px;
                    margin-right: 16px;
                    border: 0;
                    border-bottom: 2px solid transparent;
                    font-weight: 600;
                }
                QTabBar::tab:selected {
                    color: #1d4ed8;
                    border-bottom: 2px solid #2f5bff;
                }
                QSplitter::handle {
                    background: transparent;
                }
                QWidget {
                    color: #1f2933;
                }
                QGroupBox {
                    font-weight: 600;
                    border: 1px solid #d6dbe8;
                    border-radius: 10px;
                    margin-top: 14px;
                    padding: 10px;
                    background: #ffffff;
                }
                QGroupBox::title {
                    subcontrol-origin: margin;
                    left: 12px;
                    padding: 0 6px;
                }
                QLineEdit, QTextEdit, QTableWidget {
                    background: #ffffff;
                    border: 1px solid #d6dbe8;
                    border-radius: 8px;
                    padding: 6px;
                }
                QScrollArea, QScrollArea QWidget {
                    background: #ffffff;
                    border: 1px solid #d6dbe8;
                    border-radius: 8px;
                }
                QTableWidget {
                    gridline-color: #e5e9f2;
                    alternate-background-color: #f8faff;
                }
                QHeaderView::section {
                    background-color: #e9edf7;
                    padding: 6px;
                    border: 1px solid #d6dbe8;
                    font-weight: 600;
                }
                QTableCornerButton::section {
                    background: #e9edf7;
                    border: 1px solid #d6dbe8;
                }
                QPushButton {
                    background-color: #2f5bff;
                    color: #ffffff;
                    border-radius: 10px;
                    padding: 8px 16px;
                    font-weight: 600;
                }
                QPushButton:hover {
                    background-color: #2448cc;
                }
                QPushButton:disabled {
                    background-color: #a5b4ff;
                    color: #f8faff;
                }
                QCheckBox {
                    spacing: 8px;
                }
                """
            )
            self.status_label.setStyleSheet("font-weight: 600; color: #1f2933; padding: 4px;")
            self.details.setStyleSheet("color: #3e4c59;")
            self.test_secondary_table.setStyleSheet(
                """
                QTableWidget {
                    gridline-color: #c2cada;
                    border: 1px solid #c2cada;
                }
                QHeaderView::section {
                    border: 1px solid #c2cada;
                }
                """
            )

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

    def _on_item_changed(self, item: QTableWidgetItem) -> None:
        # prevent recursion when we are programmatically populating the table
        if getattr(self, "_updating_table", False):
            return
        if self.review_df is None:
            return
        if getattr(self, "_col_usernotes", -1) < 0:
            return
        if item.column() != self._col_usernotes:
            return

        r = item.row()
        sid = self.session_id.text().strip()

        whs = str(self.review_df.at[r, "Whs"])
        part = str(self.review_df.at[r, "Item"])
        lot = str(self.review_df.at[r, "Batch/lot"])
        loc = str(self.review_df.at[r, "Location"])

        note_text = item.text()
        # persist
        key = NoteKey(sid, whs, part, lot, loc)
        updated = self.notes_db.upsert_note(key, note_text)

        # update dataframe
        self.review_df.at[r, "UserNotes"] = note_text
        if "NoteUpdatedAt" in self.review_df.columns:
            self.review_df.at[r, "NoteUpdatedAt"] = updated


    def _on_header_filter_changed(self, column: int, text: str) -> None:
        self._column_filters[column] = text
        self._apply_filters()

    def _apply_filters(self) -> None:
        if self.review_df is None:
            return

        df = self.review_df.copy()
        df = self._apply_row_visibility(df)

        q = self.filter_search.text().strip().lower()
        if q:
            cols = [c for c in ["Item", "Location", "Tag", "Description", "GroupHeadline"] if c in df.columns]
            if cols:
                mask = False
                for c in cols:
                    mask = mask | df[c].astype(str).str.lower().str.contains(q, na=False)
                df = df[mask]

        for idx, text in self._column_filters.items():
            if not text:
                continue
            if idx >= len(self._table_headers):
                continue
            col = self._table_headers[idx]
            if col not in df.columns:
                continue
            df = df[df[col].astype(str).str.contains(text, case=False, na=False)]

        # Re-render table (keeps notes editing)
        self._set_table_from_df(df)

    def _apply_row_visibility(self, df: pd.DataFrame) -> pd.DataFrame:
        return df

    def _refresh_test_results(self) -> None:
        if self.test_results_df is None:
            return
        self._set_test_table_from_df(self.test_results_df.copy())

    def _refresh_tables(self) -> None:
        self._apply_filters()
        self._refresh_test_results()

    def _update_header_filters(self, df: pd.DataFrame) -> None:
        if self.table_header.filter_count() == 0:
            return
        source_df = self.review_df if self.review_df is not None else df
        options: list[list[str]] = []
        for col in self._table_headers:
            if col not in source_df.columns:
                options.append([])
                continue
            series = source_df[col].dropna().astype(str)
            unique_values = sorted(set(series.tolist()))
            options.append(unique_values)
        self.table_header.set_filter_options(options)

    def _load_hidden_columns(self) -> set[str]:
        stored = self.settings.value("hidden_columns", [])
        if isinstance(stored, str):
            stored = [stored]
        return {str(name) for name in stored}

    def _save_hidden_columns(self) -> None:
        self.settings.setValue("hidden_columns", sorted(self._hidden_columns))

    def _clear_layout(self, layout: QVBoxLayout) -> None:
        while layout.count():
            item = layout.takeAt(0)
            widget = item.widget()
            if widget is not None:
                widget.deleteLater()

    def _update_column_selector(self, headers: list[str]) -> None:
        self._clear_layout(self.column_selector_content_layout)
        if not headers:
            self.column_selector_content_layout.addWidget(QLabel("Load a count sheet to view columns."))
            self.column_selector_content_layout.addStretch(1)
            return

        for idx, header in enumerate(headers):
            label = self._format_header(header)
            checkbox = QCheckBox(label)
            checkbox.setChecked(header not in self._hidden_columns)
            checkbox.toggled.connect(
                lambda checked, col_name=header, col_index=idx: self._toggle_column_visibility(
                    col_name, col_index, checked
                )
            )
            self.column_selector_content_layout.addWidget(checkbox)

        self.column_selector_content_layout.addStretch(1)

    def _toggle_column_visibility(self, column_name: str, column_index: int, checked: bool) -> None:
        self.table.setColumnHidden(column_index, not checked)
        if checked:
            self._hidden_columns.discard(column_name)
        else:
            self._hidden_columns.add(column_name)
        self._save_hidden_columns()

    def _apply_column_visibility(self, headers: list[str]) -> None:
        for idx, header in enumerate(headers):
            self.table.setColumnHidden(idx, header in self._hidden_columns)


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

#!/usr/bin/env python
import sys
from typing import List, Tuple, Dict

from PyQt5 import QtWidgets
from PyQt5.QtCore import Qt, QModelIndex
from PyQt5.QtGui import QStandardItemModel, QStandardItem, QBrush, QColor, QFont
from PyQt5.QtCore import QSortFilterProxyModel
from PyQt5.QtWidgets import QFileDialog, QApplication, QLineEdit

import design
from store import Store, Table, Workflow, Color
from parsing_tool import parse_workflows_coroutine


def copy_model_to_clipboard(model: QStandardItemModel):
    db_list: str = '\n'.join(
        [model.item(r_i, 0).text() for r_i in range(model.rowCount())])
    QApplication.clipboard().setText(db_list)


def change_item_color(model: QStandardItemModel, item_text: str, color: Color):
    for i in range(model.rowCount()):
        item: QStandardItem = model.item(i, 0)
        if item_text == item.text():
            color: QColor = QColor(Color.to_q_color(color))
            brush: QBrush = QBrush(color)
            item.setForeground(brush)


def less_than_name_color(proxy_model: QSortFilterProxyModel):
    def func(left: QModelIndex, right: QModelIndex) -> bool:
        model: QStandardItemModel = proxy_model.sourceModel()
        left_item: QStandardItem = model.item(left.row())
        right_item: QStandardItem = model.item(right.row())
        left_text: str = left_item.text()
        right_text: str = right_item.text()
        left_color: Color = Color.from_q_color(left_item.foreground().color())
        right_color: Color = Color.from_q_color(right_item.foreground().color())
        if left_color != right_color:
            return left_color < right_color
        return left_text < right_text

    return func


class MainApp(QtWidgets.QMainWindow, design.Ui_MainWindow):
    def bind_copy_actions(self) -> None:
        def copy_list(model: QStandardItemModel):
            def func(_):
                copy_model_to_clipboard(model)

            return func

        self.db_created_at_label.mousePressEvent = copy_list(self.db_created_at_list_model)
        self.db_columns_label.mousePressEvent = copy_list(self.db_columns_list_model)
        self.db_partitions_label.mousePressEvent = copy_list(self.db_partitions_list_model)
        self.db_based_on_label.mousePressEvent = copy_list(self.db_based_on_list_model)
        self.db_updated_at_label.mousePressEvent = copy_list(self.db_updated_at_list_model)
        self.db_used_in_label.mousePressEvent = copy_list(self.db_used_in_list_model)
        self.wf_predecessors_label.mousePressEvent = copy_list(self.wf_predecessors_list_model)
        self.wf_descendants_label.mousePressEvent = copy_list(self.wf_predecessors_list_model)
        self.wf_source_label.mousePressEvent = copy_list(self.wf_source_list_model)
        self.wf_effected_label.mousePressEvent = copy_list(self.wf_effected_list_model)

    def sort_by_text_and_color(self, search_box: QLineEdit, color_filter: List[Color],
                               proxy_model: QSortFilterProxyModel, watch_unplugged: bool = False):
        unplugged_tables: List[str] = []
        if watch_unplugged:
            unplugged_tables: List[str] = self.store.get_tables('', only_unplugged=True, only_names=True)

        def func(row: int, _) -> bool:
            model: QStandardItemModel = proxy_model.sourceModel()
            search_text: str = search_box.text()
            item: QStandardItem = model.item(row)
            text: str = item.text()
            if watch_unplugged and self.only_unplugged['value']:
                if text not in unplugged_tables:
                    return False
            color = Color.from_q_color(item.foreground().color())
            return search_text in text and (not len(color_filter) or color in color_filter)

        return func

    def select_workflows_directory(self) -> None:
        dialog: QFileDialog = QFileDialog(self, caption='Select workflows directory')
        self.directory_path = str(dialog.getExistingDirectory(dialog, 'Select workflows directory'))
        dialog.close()
        if self.directory_path:
            try:
                self.loading_progress.setValue(0)
                self.stackedWidget.setCurrentIndex(1)
                table_id_name_pairs: List[Tuple[int, str]] = self.store.get_tables(id_name_pairs=True)
                gen = parse_workflows_coroutine(self.directory_path, table_id_name_pairs)
                while True:
                    progress: int = next(gen)
                    self.loading_progress.setValue(progress)
            except StopIteration as ret:
                sqooped_tables, workflows, table_based_on, table_created_in, table_partitions, table_updated_in, table_used_in = ret.value
                workflows = [Workflow(*w_n) for w_n in workflows]
                self.store.insert_sqooped_tables(sqooped_tables)
                self.store.insert_workflows(workflows)
                self.store.insert_table_based_on(table_based_on)
                self.store.insert_table_created_in(table_created_in)
                self.store.insert_table_used_in(table_used_in)
                self.store.insert_table_updated_in(table_updated_in)
                self.store.insert_table_partitions(table_partitions)
            finally:
                self.stackedWidget.setCurrentIndex(0)
            self.wf_filter_workflows()
            self.set_menu_state()

    def insert_tables_from_schema_coroutine(self, tables_list: List[str]) -> bool:
        progress: int = 0
        length: int = len(tables_list)
        self.store.delete_tables()
        for table_name in tables_list:
            without_schema: str = table_name.split('.')[1]
            tables: List[Table] = self.store.get_tables_by_names([table_name, without_schema])
            if len(tables) != 0:
                for table in tables:
                    table.name = table_name
                    self.store.update_table(table)
            else:
                self.store.insert_new_table(table_name)
            progress += 1
            yield int((progress / length * 100) + 1)
        return True

    def update_tables_columns_from_schema_coroutine(self, tables_dict: Dict[str, List[Tuple[str, str]]]) -> bool:
        progress: int = 0
        length: int = len(list(tables_dict.keys()))
        tables: List[Table] = self.store.get_tables_by_names(list(tables_dict.keys()))
        for table in tables:
            if table.name in tables_dict:
                self.store.insert_table_columns([(table.index, t[0], t[1]) for t in tables_dict[table.name]])
                # del tables_dict[table.name]
                progress += 1
                yield int((progress / length * 100) + 1)
        # for table_name in tables_dict:
        #     table: Table = self.store.insert_new_table(table_name)
        #     self.store.insert_table_columns([(table.index, t[0], t[1]) for t in tables_dict[table.name]])
        #     progress += 1
        #     yield int((progress / length * 100) + 1)
        return True

    def extract_hive_schema(self) -> None:
        dialog: QFileDialog = QFileDialog(self, caption='Select hive schema file')
        schema_filepath: str = str(dialog.getOpenFileName(dialog, 'Select hive schema file')[0])
        dialog.close()
        if schema_filepath:
            tables_list: List[str] = []
            with open(schema_filepath, 'r') as file:
                for line in file.readlines():
                    if 'schema_name,table_name' in line:
                        continue
                    else:
                        table_name: str = line.replace(' ', '').replace(',', '.').strip()
                        tables_list.append(table_name)
            try:
                self.loading_progress.setValue(0)
                self.stackedWidget.setCurrentIndex(1)
                gen = self.insert_tables_from_schema_coroutine(tables_list)
                while True:
                    progress: int = next(gen)
                    self.loading_progress.setValue(progress)
            except StopIteration as ret:
                pass
            finally:
                self.stackedWidget.setCurrentIndex(0)
            self.db_filter_tables()
            self.set_menu_state()

    def extract_impala_schema(self) -> None:
        schema_filepath: str = str(QFileDialog.getOpenFileName(None, 'Select impala schema file')[0])
        if schema_filepath:
            tables_dict: Dict[str, List[Tuple[str, str]]] = {}
            with open(schema_filepath, 'r') as file:
                for line in file.readlines():
                    if 'schema_name,table_name,field_name,field_type' in line:
                        continue
                    else:
                        table: List[str] = line.split(',')
                        table_name: str = table[0].strip() + '.' + table[1].strip()
                        if table_name not in tables_dict:
                            tables_dict[table_name] = [(table[2].strip(), table[3].strip().strip('"'))]
                        else:
                            tables_dict[table_name].append((table[2].strip(), table[3].strip().strip('"')))
            try:
                self.loading_progress.setValue(0)
                self.stackedWidget.setCurrentIndex(1)
                gen = self.update_tables_columns_from_schema_coroutine(tables_dict)
                while True:
                    progress: int = next(gen)
                    self.loading_progress.setValue(progress)
            except StopIteration as ret:
                pass
            finally:
                self.stackedWidget.setCurrentIndex(0)
            self.db_filter_tables()
            self.set_menu_state()

    def clear_database(self) -> None:
        self.store.create_db_tables(force=True)
        self.set_menu_state()
        self.db_filter_tables()
        self.wf_filter_workflows()

    def wf_fill_workflows(self) -> None:
        for workflow in self.store.get_workflows():
            color: QColor = QColor(Color.to_q_color(workflow.color))
            brush: QBrush = QBrush(color)
            item = QStandardItem(workflow.name)
            item.setForeground(brush)
            item.setEditable(False)
            self.wf_workflow_list_model.appendRow(item)

    def wf_filter_workflows(self) -> None:
        self.wf_workflow_proxy_model.invalidateFilter()
        self.wf_workflow_proxy_model.sort(0)

    def wf_select_workflows(self) -> None:
        try:
            workflow_name: str = self.wf_workflow_list.selectionModel().selectedIndexes()[0].data(Qt.DisplayRole)
            workflow: Workflow = self.store.get_workflows(workflow_name)[0]
            self.store.populate_workflow_data(workflow)
            self.current_workflow = workflow
            self.fill_wf_fields()
        except IndexError:
            pass

    def fill_wf_fields(self) -> None:
        if self.current_workflow:
            self.wf_set_color(self.current_workflow.color)
            self.wf_effected_list_model.clear()
            self.wf_source_list_model.clear()
            self.wf_predecessors_list_model.clear()
            self.wf_descendants_list_model.clear()
            for ef_t in sorted(self.current_workflow.effected_tables):
                item = QStandardItem(ef_t)
                item.setEditable(False)
                self.wf_effected_list_model.appendRow(item)
            for src_t in sorted(self.current_workflow.source_tables):
                item = QStandardItem(src_t)
                item.setEditable(False)
                self.wf_source_list_model.appendRow(item)
            for p_w in sorted(self.current_workflow.predecessors):
                item = QStandardItem(p_w)
                item.setEditable(False)
                self.wf_predecessors_list_model.appendRow(item)
            for d_w in sorted(self.current_workflow.descendants):
                item = QStandardItem(d_w)
                item.setEditable(False)
                self.wf_descendants_list_model.appendRow(item)

    def save_wf_fields(self) -> None:
        if self.current_workflow:
            self.current_workflow.color = self.wf_get_color()
            self.store.update_workflow(self.current_workflow)
            change_item_color(self.wf_workflow_list_model, self.current_workflow.name, self.current_workflow.color)

    def db_fill_tables(self) -> None:
        self.db_table_list_model.clear()
        for table in self.store.get_tables():
            color: QColor = QColor(Color.to_q_color(table.color))
            brush: QBrush = QBrush(color)
            item = QStandardItem(table.name)
            item.setEditable(False)
            item.setForeground(brush)
            self.db_table_list_model.appendRow(item)

    def db_filter_tables(self) -> None:
        self.db_table_proxy_model.invalidateFilter()
        self.db_table_proxy_model.sort(0)

    def fill_db_fields(self) -> None:
        if self.current_table:
            self.store.populate_table_data(self.current_table)
            self.db_description_input.setText(self.current_table.meaning)
            self.db_authors_input.setText(self.current_table.authors)
            self.db_set_color(self.current_table.color)
            self.db_based_on_list_model.clear()
            self.db_created_at_list_model.clear()
            self.db_updated_at_list_model.clear()
            self.db_used_in_list_model.clear()
            self.db_partitions_list_model.clear()
            self.db_columns_list_model.clear()
            for s in sorted(self.current_table.first_based_on_tables):
                item = QStandardItem(s)
                font: QFont = item.font()
                font.setBold(True)
                item.setFont(font)
                item.setEditable(False)
                self.db_based_on_list_model.appendRow(item)
            for s in sorted(self.current_table.based_on_tables):
                item = QStandardItem(s)
                item.setEditable(False)
                self.db_based_on_list_model.appendRow(item)
            for s in sorted(self.current_table.created_in_workflows):
                item = QStandardItem(s)
                item.setEditable(False)
                self.db_created_at_list_model.appendRow(item)
            for s in sorted(self.current_table.updated_in_workflows):
                item = QStandardItem(s)
                item.setEditable(False)
                self.db_updated_at_list_model.appendRow(item)
            for s in sorted(self.current_table.used_in_workflows):
                item = QStandardItem(s)
                item.setEditable(False)
                self.db_used_in_list_model.appendRow(item)
            for s in sorted(self.current_table.partitions):
                item = QStandardItem(s)
                item.setEditable(False)
                self.db_partitions_list_model.appendRow(item)
            for s in self.current_table.columns:
                item = QStandardItem(s)
                item.setEditable(False)
                self.db_columns_list_model.appendRow(item)

    def save_db_fields(self) -> None:
        if self.current_table:
            self.current_table.meaning = self.db_description_input.toPlainText()
            self.current_table.authors = self.db_authors_input.toPlainText()
            self.current_table.color = self.db_get_color()
            self.store.update_table(self.current_table)
            change_item_color(self.db_table_list_model, self.current_table.name, self.current_table.color)

    def db_set_color(self, color: Color):
        if color is Color.RED:
            self.db_red_color_button.setChecked(True)
            self.db_blue_color_button.setChecked(False)
            self.db_yellow_color_button.setChecked(False)
            self.db_green_color_button.setChecked(False)
            self.db_none_color_button.setChecked(False)
        elif color is Color.BLUE:
            self.db_red_color_button.setChecked(False)
            self.db_blue_color_button.setChecked(True)
            self.db_yellow_color_button.setChecked(False)
            self.db_green_color_button.setChecked(False)
            self.db_none_color_button.setChecked(False)
        elif color is Color.YELLOW:
            self.db_red_color_button.setChecked(False)
            self.db_blue_color_button.setChecked(False)
            self.db_yellow_color_button.setChecked(True)
            self.db_green_color_button.setChecked(False)
            self.db_none_color_button.setChecked(False)
        elif color is Color.GREEN:
            self.db_red_color_button.setChecked(False)
            self.db_blue_color_button.setChecked(False)
            self.db_yellow_color_button.setChecked(False)
            self.db_green_color_button.setChecked(True)
            self.db_none_color_button.setChecked(False)
        else:
            self.db_red_color_button.setChecked(False)
            self.db_blue_color_button.setChecked(False)
            self.db_yellow_color_button.setChecked(False)
            self.db_green_color_button.setChecked(False)
            self.db_none_color_button.setChecked(True)

    def wf_set_color(self, color: Color):
        if color is Color.RED:
            self.wf_red_color_button.setChecked(True)
            self.wf_blue_color_button.setChecked(False)
            self.wf_yellow_color_button.setChecked(False)
            self.wf_green_color_button.setChecked(False)
            self.wf_none_color_button.setChecked(False)
        elif color is Color.BLUE:
            self.wf_red_color_button.setChecked(False)
            self.wf_blue_color_button.setChecked(True)
            self.wf_yellow_color_button.setChecked(False)
            self.wf_green_color_button.setChecked(False)
            self.wf_none_color_button.setChecked(False)
        elif color is Color.YELLOW:
            self.wf_red_color_button.setChecked(False)
            self.wf_blue_color_button.setChecked(False)
            self.wf_yellow_color_button.setChecked(True)
            self.wf_green_color_button.setChecked(False)
            self.wf_none_color_button.setChecked(False)
        elif color is Color.GREEN:
            self.wf_red_color_button.setChecked(False)
            self.wf_blue_color_button.setChecked(False)
            self.wf_yellow_color_button.setChecked(False)
            self.wf_green_color_button.setChecked(True)
            self.wf_none_color_button.setChecked(False)
        else:
            self.wf_red_color_button.setChecked(False)
            self.wf_blue_color_button.setChecked(False)
            self.wf_yellow_color_button.setChecked(False)
            self.wf_green_color_button.setChecked(False)
            self.wf_none_color_button.setChecked(True)

    def db_get_color(self) -> Color:
        if self.db_red_color_button.isChecked():
            return Color.RED
        elif self.db_blue_color_button.isChecked():
            return Color.BLUE
        elif self.db_green_color_button.isChecked():
            return Color.GREEN
        elif self.db_yellow_color_button.isChecked():
            return Color.YELLOW
        elif self.db_none_color_button.isChecked():
            return Color.NONE

    def wf_get_color(self) -> Color:
        if self.wf_red_color_button.isChecked():
            return Color.RED
        elif self.wf_blue_color_button.isChecked():
            return Color.BLUE
        elif self.wf_green_color_button.isChecked():
            return Color.GREEN
        elif self.wf_yellow_color_button.isChecked():
            return Color.YELLOW
        elif self.wf_none_color_button.isChecked():
            return Color.NONE

    def db_select_tables(self) -> None:
        try:
            table_name: str = self.db_table_list.selectionModel().selectedIndexes()[0].data(Qt.DisplayRole)
            table: Table = self.store.get_tables(table_name)[0]
            self.current_table = table
            self.fill_db_fields()
        except IndexError:
            pass

    def db_change_tables_filter(self, v: bool):
        self.only_unplugged['value'] = v
        self.db_filter_tables()

    def db_toggle_color_filter(self, color: Color):
        def func(value):
            if value:
                self.db_color_filter.append(color)
            else:
                try:
                    self.db_color_filter.remove(color)
                except ValueError:
                    pass
            self.db_filter_tables()

        return func

    def wf_toggle_color_filter(self, color: Color):
        def func(value):
            if value:
                self.wf_color_filter.append(color)
            else:
                try:
                    self.wf_color_filter.remove(color)
                except ValueError:
                    pass
            self.wf_filter_workflows()

        return func

    def set_menu_state(self):
        status: str = self.store.get_db_status()
        if status == 'db_empty':
            self.action_extract_hive.setEnabled(True)
            self.action_exctract_impala.setEnabled(False)
            self.action_open_workflows.setEnabled(False)
        elif status == 'hive_extracted':
            self.action_extract_hive.setEnabled(False)
            self.action_exctract_impala.setEnabled(True)
            self.action_open_workflows.setEnabled(False)
        elif status == 'impala_extracted':
            self.action_extract_hive.setEnabled(False)
            self.action_exctract_impala.setEnabled(False)
            self.action_open_workflows.setEnabled(True)
        else:
            self.action_extract_hive.setEnabled(False)
            self.action_exctract_impala.setEnabled(False)
            self.action_open_workflows.setEnabled(False)

    def export_list(self):
        tab_id: int = self.tabWidget.currentIndex()
        if tab_id == 0:
            copy_model_to_clipboard(self.db_table_list_model)
        elif tab_id == 1:
            copy_model_to_clipboard(self.wf_workflow_list_model)

    def __init__(self):
        super().__init__()
        self.store: Store = Store('db.sqlite3')
        self.directory_path: str = None
        self.current_table: Table = None
        self.current_workflow: Workflow = None
        self.only_unplugged: Dict[str, bool] = {'value': False}
        self.db_color_filter: List[Color] = []
        self.wf_color_filter: List[Color] = []
        self.setupUi(self)
        self.set_menu_state()

        self.wf_workflow_list_model = QStandardItemModel(self)
        self.wf_workflow_proxy_model = QSortFilterProxyModel(self)
        self.wf_workflow_proxy_model.setSourceModel(self.wf_workflow_list_model)
        self.wf_workflow_list.setModel(self.wf_workflow_proxy_model)
        self.wf_workflow_proxy_model.filterAcceptsRow = self.sort_by_text_and_color(self.wf_workflow_search,
                                                                                    self.wf_color_filter,
                                                                                    self.wf_workflow_proxy_model
                                                                                    )
        self.wf_workflow_proxy_model.lessThan = less_than_name_color(self.wf_workflow_proxy_model)
        self.wf_fill_workflows()
        self.wf_workflow_proxy_model.sort(0)

        self.wf_save_button.clicked.connect(self.save_wf_fields)
        self.wf_source_list_model = QStandardItemModel(self.wf_source_list)
        self.wf_source_list.setModel(self.wf_source_list_model)
        self.wf_effected_list_model = QStandardItemModel(self.wf_effected_list)
        self.wf_effected_list.setModel(self.wf_effected_list_model)
        self.wf_predecessors_list_model = QStandardItemModel(self.wf_predecessors_list)
        self.wf_predecessors_list.setModel(self.wf_predecessors_list_model)
        self.wf_descendants_list_model = QStandardItemModel(self.wf_descendants_list)
        self.wf_descendants_list.setModel(self.wf_descendants_list_model)

        self.action_open_workflows.triggered.connect(self.select_workflows_directory)
        self.action_extract_hive.triggered.connect(self.extract_hive_schema)
        self.action_exctract_impala.triggered.connect(self.extract_impala_schema)
        self.action_clear_database.triggered.connect(self.clear_database)
        self.action_copy_list_to_clipboard.triggered.connect(self.export_list)

        self.wf_workflow_search.textChanged.connect(self.wf_filter_workflows)
        self.wf_workflow_list.selectionModel().selectionChanged.connect(self.wf_select_workflows)
        self.wf_blue_color_filter.stateChanged.connect(self.wf_toggle_color_filter(Color.BLUE))
        self.wf_green_color_filter.stateChanged.connect(self.wf_toggle_color_filter(Color.GREEN))
        self.wf_red_color_filter.stateChanged.connect(self.wf_toggle_color_filter(Color.RED))
        self.wf_yellow_color_filter.stateChanged.connect(self.wf_toggle_color_filter(Color.YELLOW))
        self.wf_none_color_filter.stateChanged.connect(self.wf_toggle_color_filter(Color.NONE))

        self.db_table_list_model = QStandardItemModel(self)
        self.db_table_proxy_model = QSortFilterProxyModel(self)
        self.db_table_proxy_model.setSourceModel(self.db_table_list_model)
        self.db_table_list.setModel(self.db_table_proxy_model)
        self.db_table_proxy_model.filterAcceptsRow = self.sort_by_text_and_color(self.db_table_search,
                                                                                 self.db_color_filter,
                                                                                 self.db_table_proxy_model,
                                                                                 watch_unplugged=True)
        self.db_table_proxy_model.lessThan = less_than_name_color(self.db_table_proxy_model)
        self.db_fill_tables()
        self.db_table_proxy_model.sort(0)

        self.db_created_at_list_model = QStandardItemModel(self.db_created_at_list)
        self.db_created_at_list.setModel(self.db_created_at_list_model)
        self.db_updated_at_list_model = QStandardItemModel(self.db_updated_at_list)
        self.db_updated_at_list.setModel(self.db_updated_at_list_model)
        self.db_based_on_list_model = QStandardItemModel(self.db_based_on_list)
        self.db_based_on_list.setModel(self.db_based_on_list_model)
        self.db_used_in_list_model = QStandardItemModel(self.db_used_in_list)
        self.db_used_in_list.setModel(self.db_used_in_list_model)
        self.db_partitions_list_model = QStandardItemModel(self.db_partitions_list)
        self.db_partitions_list.setModel(self.db_partitions_list_model)
        self.db_columns_list_model = QStandardItemModel(self.db_columns_list)
        self.db_columns_list.setModel(self.db_columns_list_model)

        self.db_table_search.textChanged.connect(self.db_filter_tables)
        self.db_table_list.selectionModel().selectionChanged.connect(self.db_select_tables)
        self.db_save_button.clicked.connect(self.save_db_fields)
        self.db_show_only_unplugged.stateChanged.connect(self.db_change_tables_filter)
        self.db_blue_color_filter.stateChanged.connect(self.db_toggle_color_filter(Color.BLUE))
        self.db_green_color_filter.stateChanged.connect(self.db_toggle_color_filter(Color.GREEN))
        self.db_red_color_filter.stateChanged.connect(self.db_toggle_color_filter(Color.RED))
        self.db_yellow_color_filter.stateChanged.connect(self.db_toggle_color_filter(Color.YELLOW))
        self.db_none_color_filter.stateChanged.connect(self.db_toggle_color_filter(Color.NONE))

        self.bind_copy_actions()


def main():
    app = QtWidgets.QApplication(sys.argv)
    window = MainApp()
    window.show()
    app.exec_()


if __name__ == '__main__':
    main()

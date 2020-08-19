#!/usr/bin/env python
import sys
from typing import List

from PyQt5 import QtWidgets
from PyQt5.QtCore import Qt
from PyQt5.QtGui import QStandardItemModel, QStandardItem, QBrush, QColor
from PyQt5.QtWidgets import QFileDialog, QAbstractItemView

import design
from store import Store, Table, Workflow, Color
from parsing_tool import parse_workflows_coroutine


class MainApp(QtWidgets.QMainWindow, design.Ui_MainWindow):
    def select_directory(self) -> None:
        self.directory_path = str(QFileDialog.getExistingDirectory(self, 'Select Directory'))
        if self.directory_path:
            try:
                self.loading_progress.setValue(0)
                self.stackedWidget.setCurrentIndex(1)
                gen = parse_workflows_coroutine(self.directory_path)
                while True:
                    progress: int = next(gen)
                    self.loading_progress.setValue(progress)
            except StopIteration as ret:
                tables, workflows, table_created_in, table_used_in, table_based_on, table_updated_in, table_partitions = ret.value
                tables = [Table.from_dict(tables[t_n]) for t_n in tables]
                workflows = [Workflow.from_dict(workflows[w_n]) for w_n in workflows]
                self.store.create_db_tables(force=True)
                self.store.insert_tables(tables)
                self.store.insert_workflows(workflows)
                self.store.insert_table_based_on(table_based_on)
                self.store.insert_table_created_in(table_created_in)
                self.store.insert_table_used_in(table_used_in)
                self.store.insert_table_updated_in(table_updated_in)
                self.store.insert_table_partitions(table_partitions)
            finally:
                self.stackedWidget.setCurrentIndex(0)
            self.wf_filter_workflows('')

    def update_from_dump_coroutine(self, tables_list: List[str]) -> bool:
        progress: int = 0
        length: int = len(tables_list)
        self.store.delete_tables(tables_list)
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
        self.store.delete_tables(tables_list)
        return True

    def extract_schema(self) -> None:
        schema_filepath: str = str(QFileDialog.getOpenFileName(self, 'Select schema file')[0])
        if schema_filepath:
            tables_list: List[str] = []
            with open(schema_filepath, 'r') as file:
                schema: str = ''
                for line in file.readlines():
                    if 'schema' in line:
                        schema: str = line.split(':')[1].strip() + '.'
                    elif '+' in line or 'tab_name' in line:
                        continue
                    else:
                        table_name: str = line.split('|')[1].strip()
                        tables_list.append(schema + table_name)
            try:
                self.loading_progress.setValue(0)
                self.stackedWidget.setCurrentIndex(1)
                gen = self.update_from_dump_coroutine(tables_list)
                while True:
                    progress: int = next(gen)
                    self.loading_progress.setValue(progress)
            except StopIteration as ret:
                pass
            finally:
                self.stackedWidget.setCurrentIndex(0)
            self.db_filter_tables()

    def wf_filter_workflows(self, search_text: str) -> None:
        self.wf_workflow_list_model.clear()
        for workflow in self.store.get_workflows(search_text, only_names=True):
            item = QStandardItem(workflow)
            item.setEditable(False)
            self.wf_workflow_list_model.appendRow(item)

    def wf_select_workflows(self) -> None:
        workflow_names: List[str] = []
        for select in self.wf_workflow_list.selectionModel().selectedIndexes():
            workflow_names.append(select.data(Qt.DisplayRole))
        workflows = self.store.get_workflows_by_names(workflow_names)
        self.store.populate_workflows_data(workflows)
        self.wf_effected_list_model.clear()
        self.wf_source_list_model.clear()
        self.wf_predecessors_list_model.clear()
        self.wf_descendants_list_model.clear()
        for workflow in workflows:
            for ef_t in workflow.effected_tables:
                item = QStandardItem(ef_t)
                item.setEditable(False)
                self.wf_effected_list_model.appendRow(item)
            for src_t in workflow.source_tables:
                item = QStandardItem(src_t)
                item.setEditable(False)
                self.wf_source_list_model.appendRow(item)
            for p_w in workflow.predecessors:
                item = QStandardItem(p_w)
                item.setEditable(False)
                self.wf_predecessors_list_model.appendRow(item)
            for d_w in workflow.descendants:
                item = QStandardItem(d_w)
                item.setEditable(False)
                self.wf_descendants_list_model.appendRow(item)

    def db_filter_tables(self) -> None:
        search_text: str = self.db_table_search.text()
        self.db_table_list_model.clear()
        for table in self.store.get_tables(search_text=search_text, color_filter=self.color_filter):
            color: QColor = QColor(Color.to_q_color(table.color))
            brush: QBrush = QBrush(color)
            if len(table.updated_in_workflows) or len(table.created_in_workflows) or len(table.used_in_workflows):
                if not self.unplugged:
                    item = QStandardItem(table.name)
                    item.setForeground(brush)
                    item.setEditable(False)
                    self.db_table_list_model.appendRow(item)
            else:
                if self.unplugged:
                    item = QStandardItem(table.name)
                    item.setEditable(False)
                    item.setForeground(brush)
                    self.db_table_list_model.appendRow(item)

    def fill_db_fields(self) -> None:
        if self.current_table:
            self.db_description_input.setText(self.current_table.meaning)
            self.db_authors_input.setText(self.current_table.authors)
            self.db_set_color(self.current_table.color)
            self.db_sqooped_list_model.clear()
            self.db_created_at_list_model.clear()
            self.db_updated_in_list_model.clear()
            self.db_workflow_list_model.clear()
            self.db_partitions_list_model.clear()
            for s in self.current_table.based_on_tables:
                item = QStandardItem(s)
                item.setEditable(False)
                self.db_sqooped_list_model.appendRow(item)
            for s in self.current_table.created_in_workflows:
                item = QStandardItem(s)
                item.setEditable(False)
                self.db_created_at_list_model.appendRow(item)
            for s in self.current_table.updated_in_workflows:
                item = QStandardItem(s)
                item.setEditable(False)
                self.db_updated_in_list_model.appendRow(item)
            for s in self.current_table.used_in_workflows:
                item = QStandardItem(s)
                item.setEditable(False)
                self.db_workflow_list_model.appendRow(item)
            for s in self.current_table.partitions:
                item = QStandardItem(s)
                item.setEditable(False)
                self.db_partitions_list_model.appendRow(item)

    def save_db_fields(self) -> None:
        if self.current_table:
            self.current_table.meaning = self.db_description_input.toPlainText()
            self.current_table.authors = self.db_authors_input.toPlainText()
            self.current_table.color = self.db_get_color()
            self.store.update_table(self.current_table)

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

    def db_select_tables(self) -> None:
        table_name: str = self.db_table_list.selectionModel().selectedIndexes()[0].data(Qt.DisplayRole)
        try:
            table: Table = self.store.get_tables(table_name)[0]
            self.store.populate_table_data(table)
            self.current_table = table
            self.fill_db_fields()
        except IndexError:
            pass

    def db_change_tables_filter(self, v: bool):
        self.unplugged = v
        self.db_filter_tables()

    def toggle_color_filter(self, color: Color):
        def func(value):
            if value:
                self.color_filter.append(color)
            else:
                try:
                    self.color_filter.remove(color)
                except ValueError:
                    pass
            self.db_filter_tables()
        return func

    def __init__(self):
        super().__init__()
        self.store: Store = Store('db.sqlite3')
        self.directory_path: str = None
        self.current_table: Table = None
        self.unplugged: bool = False
        self.color_filter: List[Color] = []
        self.setupUi(self)

        self.wf_workflow_list_model = QStandardItemModel(self.wf_workflow_list)
        self.wf_workflow_list.setModel(self.wf_workflow_list_model)
        self.wf_workflow_list.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.wf_source_list_model = QStandardItemModel(self.wf_source_list)
        self.wf_source_list.setModel(self.wf_source_list_model)
        self.wf_effected_list_model = QStandardItemModel(self.wf_effected_list)
        self.wf_effected_list.setModel(self.wf_effected_list_model)
        self.wf_predecessors_list_model = QStandardItemModel(self.wf_predecessors_list)
        self.wf_predecessors_list.setModel(self.wf_predecessors_list_model)
        self.wf_descendants_list_model = QStandardItemModel(self.wf_descendants_list)
        self.wf_descendants_list.setModel(self.wf_descendants_list_model)

        self.action_open_workflows.triggered.connect(self.select_directory)
        self.action_extract_hive.triggered.connect(self.extract_schema)
        self.wf_workflow_search.textChanged.connect(self.wf_filter_workflows)
        self.wf_workflow_list.selectionModel().selectionChanged.connect(self.wf_select_workflows)
        self.wf_filter_workflows('')

        self.db_table_list_model = QStandardItemModel(self.db_table_list)
        self.db_table_list.setModel(self.db_table_list_model)
        self.db_created_at_list_model = QStandardItemModel(self.db_created_at_list)
        self.db_created_at_list.setModel(self.db_created_at_list_model)
        self.db_updated_in_list_model = QStandardItemModel(self.db_updated_in_list)
        self.db_updated_in_list.setModel(self.db_updated_in_list_model)
        self.db_sqooped_list_model = QStandardItemModel(self.db_sqooped_list)
        self.db_sqooped_list.setModel(self.db_sqooped_list_model)
        self.db_workflow_list_model = QStandardItemModel(self.db_workflow_list)
        self.db_workflow_list.setModel(self.db_workflow_list_model)
        self.db_partitions_list_model = QStandardItemModel(self.db_partitions_list)
        self.db_partitions_list.setModel(self.db_partitions_list_model)

        self.db_table_search.textChanged.connect(self.db_filter_tables)
        self.db_table_list.selectionModel().selectionChanged.connect(self.db_select_tables)
        self.db_save_button.clicked.connect(self.save_db_fields)
        self.db_show_only_unplugged.stateChanged.connect(self.db_change_tables_filter)
        self.db_blue_color_filter.stateChanged.connect(self.toggle_color_filter(Color.BLUE))
        self.db_green_color_filter.stateChanged.connect(self.toggle_color_filter(Color.GREEN))
        self.db_red_color_filter.stateChanged.connect(self.toggle_color_filter(Color.RED))
        self.db_yellow_color_filter.stateChanged.connect(self.toggle_color_filter(Color.YELLOW))
        self.db_none_color_filter.stateChanged.connect(self.toggle_color_filter(Color.NONE))
        self.db_filter_tables()


def main():
    app = QtWidgets.QApplication(sys.argv)
    window = MainApp()
    window.show()
    app.exec_()


if __name__ == '__main__':
    main()

#!/usr/bin/env python
import sys
from typing import List

from PyQt5 import QtWidgets
from PyQt5.QtCore import Qt
from PyQt5.QtGui import QStandardItemModel, QStandardItem
from PyQt5.QtWidgets import QFileDialog, QAbstractItemView

import design
from store import Store, Table, Workflow
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
            self.db_filter_tables('')

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

    def db_filter_tables(self, search_text: str) -> None:
        self.db_table_list_model.clear()
        for table in self.store.get_tables(search_text, only_names=True):
            item = QStandardItem(table)
            item.setEditable(False)
            self.db_table_list_model.appendRow(item)

    def fill_db_fields(self) -> None:
        if self.current_table:
            self.db_description_input.setText(self.current_table.meaning)
            self.db_authors_input.setText(self.current_table.authors)
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
            self.store.update_table(self.current_table)

    def db_select_tables(self) -> None:
        table_name: str = self.db_table_list.selectionModel().selectedIndexes()[0].data(Qt.DisplayRole)
        try:
            table: Table = self.store.get_tables(table_name)[0]
            self.store.populate_table_data(table)
            self.current_table = table
            self.fill_db_fields()
        except IndexError:
            pass

    def __init__(self):
        super().__init__()
        self.store: Store = Store('db.sqlite3')
        self.directory_path: str = None
        self.current_table: Table = None
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

        self.actionOpen.triggered.connect(self.select_directory)
        self.actionOpen_extract_schema.triggered.connect(self.extract_schema)
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
        self.db_filter_tables('')


def main():
    app = QtWidgets.QApplication(sys.argv)
    window = MainApp()
    window.show()
    app.exec_()


if __name__ == '__main__':
    main()

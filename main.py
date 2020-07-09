#!/usr/bin/env python
import os
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
                tables, workflows, table_created_in, table_used_in, table_based_on = ret.value
                tables = [Table.from_dict(tables[t_n]) for t_n in tables]
                workflows = [Workflow.from_dict(workflows[w_n]) for w_n in workflows]
                self.store.create_db_tables(force=True)
                self.store.insert_tables(tables)
                self.store.insert_workflows(workflows)
                self.store.insert_table_based_on(table_based_on)
                self.store.insert_table_created_in(table_created_in)
                self.store.insert_table_used_in(table_used_in)
            finally:
                self.stackedWidget.setCurrentIndex(0)
            self.wf_filter_workflows('')

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
        print(workflows)

    def __init__(self):
        super().__init__()
        self.store: Store = Store('db.sqlite3')
        self.directory_path: str = None
        self.setupUi(self)

        self.wf_workflow_list_model = QStandardItemModel(self.wf_workflow_list)
        self.wf_workflow_list.setModel(self.wf_workflow_list_model)
        self.wf_workflow_list.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.wf_source_list_model = QStandardItemModel(self.wf_source_list)
        self.wf_source_list.setModel(self.wf_source_list_model)
        self.wf_effected_list_model = QStandardItemModel(self.wf_effected_list)
        self.wf_effected_list.setModel(self.wf_effected_list_model)

        self.actionOpen.triggered.connect(self.select_directory)
        self.wf_workflow_search.textChanged.connect(self.wf_filter_workflows)
        self.wf_workflow_list.selectionModel().selectionChanged.connect(self.wf_select_workflows)
        self.wf_filter_workflows('')


def main():
    app = QtWidgets.QApplication(sys.argv)
    window = MainApp()
    window.show()
    app.exec_()


if __name__ == '__main__':
    main()

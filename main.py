#!/usr/bin/env python
import os
import sys
from typing import List

from PyQt5 import QtWidgets
from PyQt5.QtCore import Qt
from PyQt5.QtGui import QStandardItemModel, QStandardItem
from PyQt5.QtWidgets import QFileDialog, QAbstractItemView

import design
from store import Store


class MainApp(QtWidgets.QMainWindow, design.Ui_MainWindow):
    def select_directory(self) -> None:
        self.directory_path = str(QFileDialog.getExistingDirectory(self, 'Select Directory'))
        if self.directory_path:
            workflow_names: List[str] = []
            for workflow in os.listdir(self.directory_path):
                workflow_dir = os.path.join(self.directory_path, workflow)
                try:
                    list_dir: list = os.listdir(workflow_dir)
                    if 'workflow.xml' in list_dir:
                        workflow_names.append(workflow)
                except NotADirectoryError:
                    pass
            self.store.create_db_tables(force=True)
            self.store.insert_workflows(workflow_names)
            self.filter_workflows('')

    def filter_workflows(self, search_text: str) -> None:
        self.workflow_list_model.clear()
        for workflow in self.store.get_workflows(search_text, only_names=True):
            item = QStandardItem(workflow)
            item.setEditable(False)
            self.workflow_list_model.appendRow(item)

    def select_workflows(self) -> None:
        workflow_names: List[str] = []
        for select in self.workflow_list.selectionModel().selectedIndexes():
            workflow_names.append(select.data(Qt.DisplayRole))
        workflows = self.store.get_workflows_by_names(workflow_names)
        print(workflows)

    def __init__(self):
        super().__init__()
        self.store: Store = Store('db.sqlite3')
        self.directory_path: str = None
        self.setupUi(self)

        self.workflow_list_model = QStandardItemModel(self.workflow_list)
        self.workflow_list.setModel(self.workflow_list_model)
        self.workflow_list.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.source_list_model = QStandardItemModel(self.source_list)
        self.source_list.setModel(self.source_list_model)
        self.effected_list_model = QStandardItemModel(self.effected_list)
        self.effected_list.setModel(self.effected_list_model)

        self.actionOpen.triggered.connect(self.select_directory)
        self.search_box.textChanged.connect(self.filter_workflows)
        self.workflow_list.selectionModel().selectionChanged.connect(self.select_workflows)
        self.filter_workflows('')


def main():
    app = QtWidgets.QApplication(sys.argv)
    window = MainApp()
    window.show()
    app.exec_()


if __name__ == '__main__':
    main()

#!/usr/bin/env python
import os
import sys
from PyQt5 import QtWidgets
from PyQt5.QtCore import Qt
from PyQt5.QtGui import QStandardItemModel, QStandardItem
from PyQt5.QtWidgets import QFileDialog, QAbstractItemView

import design


class MainApp(QtWidgets.QMainWindow, design.Ui_MainWindow):
    def select_directory(self) -> None:
        self.directory_path = str(QFileDialog.getExistingDirectory(self, 'Select Directory'))

        if self.directory_path:
            for workflow in os.listdir(self.directory_path):
                workflow_dir = os.path.join(self.directory_path, workflow)
                try:
                    list_dir: list = os.listdir(workflow_dir)
                    if 'workflow.xml' in list_dir:
                        self.workflows.append(workflow)
                except NotADirectoryError:
                    pass
            self.filter_workflows('')

    def filter_workflows(self, search_text: str) -> None:
        self.workflow_list_model.clear()
        for workflow in [wf for wf in self.workflows if search_text in wf]:
            item = QStandardItem(workflow)
            item.setEditable(False)
            self.workflow_list_model.appendRow(item)

    def select_workflows(self) -> None:
        for select in self.workflow_list.selectionModel().selectedIndexes():
            workflow = select.data(Qt.DisplayRole)
            print(workflow)


    def __init__(self):
        super().__init__()
        self.directory_path: str = None
        self.workflows: list = []
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


def main():
    app = QtWidgets.QApplication(sys.argv)
    window = MainApp()
    window.show()
    app.exec_()


if __name__ == '__main__':
    main()

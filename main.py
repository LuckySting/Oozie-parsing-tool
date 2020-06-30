#!/usr/bin/env python
import sys
from PyQt5 import QtWidgets
from PyQt5.QtWidgets import QFileDialog

import design


class MainApp(QtWidgets.QMainWindow, design.Ui_MainWindow):
    def select_directory(self):
        self.directory_path = str(QFileDialog.getExistingDirectory(self, 'Select Directory'))

    def __init__(self):
        super().__init__()
        self.directory_path: str = None
        self.setupUi(self)
        self.actionOpen.triggered.connect(self.select_directory)


def main():
    app = QtWidgets.QApplication(sys.argv)
    window = MainApp()
    window.show()
    app.exec_()


if __name__ == '__main__':
    main()

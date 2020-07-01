# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file '../../QTProjects/designs/parse_tool.ui'
#
# Created by: PyQt5 UI code generator 5.15.0
#
# WARNING: Any manual changes made to this file will be lost when pyuic5 is
# run again.  Do not edit this file unless you know what you are doing.


from PyQt5 import QtCore, QtGui, QtWidgets


class Ui_MainWindow(object):
    def setupUi(self, MainWindow):
        MainWindow.setObjectName("MainWindow")
        MainWindow.resize(750, 500)
        MainWindow.setMinimumSize(QtCore.QSize(750, 500))
        MainWindow.setMaximumSize(QtCore.QSize(750, 500))
        self.centralwidget = QtWidgets.QWidget(MainWindow)
        self.centralwidget.setMinimumSize(QtCore.QSize(750, 478))
        self.centralwidget.setMaximumSize(QtCore.QSize(750, 478))
        self.centralwidget.setAutoFillBackground(False)
        self.centralwidget.setObjectName("centralwidget")
        self.tabWidget = QtWidgets.QTabWidget(self.centralwidget)
        self.tabWidget.setGeometry(QtCore.QRect(0, 0, 750, 500))
        self.tabWidget.setMinimumSize(QtCore.QSize(750, 500))
        self.tabWidget.setMaximumSize(QtCore.QSize(750, 500))
        self.tabWidget.setCursor(QtGui.QCursor(QtCore.Qt.ArrowCursor))
        self.tabWidget.setAutoFillBackground(False)
        self.tabWidget.setObjectName("tabWidget")
        self.db_tab = QtWidgets.QWidget()
        self.db_tab.setCursor(QtGui.QCursor(QtCore.Qt.ArrowCursor))
        self.db_tab.setObjectName("db_tab")
        self.db_table_list = QtWidgets.QListView(self.db_tab)
        self.db_table_list.setGeometry(QtCore.QRect(10, 50, 191, 391))
        self.db_table_list.setObjectName("db_table_list")
        self.db_table_search = QtWidgets.QLineEdit(self.db_tab)
        self.db_table_search.setGeometry(QtCore.QRect(10, 10, 191, 28))
        self.db_table_search.setAutoFillBackground(True)
        self.db_table_search.setObjectName("db_table_search")
        self.db_frame = QtWidgets.QFrame(self.db_tab)
        self.db_frame.setGeometry(QtCore.QRect(220, 10, 521, 431))
        self.db_frame.setFrameShape(QtWidgets.QFrame.Box)
        self.db_frame.setFrameShadow(QtWidgets.QFrame.Plain)
        self.db_frame.setObjectName("db_frame")
        self.db_sqooped_list = QtWidgets.QListView(self.db_frame)
        self.db_sqooped_list.setGeometry(QtCore.QRect(260, 60, 251, 101))
        self.db_sqooped_list.setObjectName("db_sqooped_list")
        self.db_source_label = QtWidgets.QLabel(self.db_frame)
        self.db_source_label.setGeometry(QtCore.QRect(260, 40, 251, 21))
        self.db_source_label.setAutoFillBackground(False)
        self.db_source_label.setStyleSheet("background-color: white;")
        self.db_source_label.setFrameShape(QtWidgets.QFrame.NoFrame)
        self.db_source_label.setObjectName("db_source_label")
        self.db_workflow_list = QtWidgets.QListView(self.db_frame)
        self.db_workflow_list.setGeometry(QtCore.QRect(260, 190, 251, 101))
        self.db_workflow_list.setObjectName("db_workflow_list")
        self.db_source_label_2 = QtWidgets.QLabel(self.db_frame)
        self.db_source_label_2.setGeometry(QtCore.QRect(260, 170, 251, 21))
        self.db_source_label_2.setAutoFillBackground(False)
        self.db_source_label_2.setStyleSheet("background-color: white;")
        self.db_source_label_2.setFrameShape(QtWidgets.QFrame.NoFrame)
        self.db_source_label_2.setObjectName("db_source_label_2")
        self.db_tableau_list = QtWidgets.QListView(self.db_frame)
        self.db_tableau_list.setGeometry(QtCore.QRect(260, 320, 251, 101))
        self.db_tableau_list.setObjectName("db_tableau_list")
        self.db_source_label_3 = QtWidgets.QLabel(self.db_frame)
        self.db_source_label_3.setGeometry(QtCore.QRect(260, 300, 251, 21))
        self.db_source_label_3.setAutoFillBackground(False)
        self.db_source_label_3.setStyleSheet("background-color: white;")
        self.db_source_label_3.setFrameShape(QtWidgets.QFrame.NoFrame)
        self.db_source_label_3.setObjectName("db_source_label_3")
        self.db_description_input = QtWidgets.QTextEdit(self.db_frame)
        self.db_description_input.setGeometry(QtCore.QRect(10, 40, 241, 61))
        self.db_description_input.setObjectName("db_description_input")
        self.db_header_label = QtWidgets.QLabel(self.db_frame)
        self.db_header_label.setGeometry(QtCore.QRect(10, 10, 501, 21))
        font = QtGui.QFont()
        font.setPointSize(12)
        font.setItalic(False)
        self.db_header_label.setFont(font)
        self.db_header_label.setAutoFillBackground(False)
        self.db_header_label.setStyleSheet("background-color: white;")
        self.db_header_label.setFrameShape(QtWidgets.QFrame.NoFrame)
        self.db_header_label.setObjectName("db_header_label")
        self.db_created_at_label = QtWidgets.QLabel(self.db_frame)
        self.db_created_at_label.setGeometry(QtCore.QRect(10, 200, 241, 21))
        self.db_created_at_label.setAutoFillBackground(False)
        self.db_created_at_label.setStyleSheet("background-color: white;")
        self.db_created_at_label.setFrameShape(QtWidgets.QFrame.NoFrame)
        self.db_created_at_label.setObjectName("db_created_at_label")
        self.db_created_at = QtWidgets.QLabel(self.db_frame)
        self.db_created_at.setGeometry(QtCore.QRect(10, 220, 241, 21))
        self.db_created_at.setAutoFillBackground(False)
        self.db_created_at.setStyleSheet("background-color: white;")
        self.db_created_at.setFrameShape(QtWidgets.QFrame.NoFrame)
        self.db_created_at.setObjectName("db_created_at")
        self.db_partitions_list = QtWidgets.QListView(self.db_frame)
        self.db_partitions_list.setGeometry(QtCore.QRect(10, 360, 241, 61))
        self.db_partitions_list.setObjectName("db_partitions_list")
        self.db_partitions_label = QtWidgets.QLabel(self.db_frame)
        self.db_partitions_label.setGeometry(QtCore.QRect(10, 340, 241, 21))
        self.db_partitions_label.setAutoFillBackground(False)
        self.db_partitions_label.setStyleSheet("background-color: white;")
        self.db_partitions_label.setFrameShape(QtWidgets.QFrame.NoFrame)
        self.db_partitions_label.setObjectName("db_partitions_label")
        self.db_updated_in_list = QtWidgets.QListView(self.db_frame)
        self.db_updated_in_list.setGeometry(QtCore.QRect(10, 270, 241, 61))
        self.db_updated_in_list.setObjectName("db_updated_in_list")
        self.db_updated_in_label = QtWidgets.QLabel(self.db_frame)
        self.db_updated_in_label.setGeometry(QtCore.QRect(10, 250, 241, 21))
        self.db_updated_in_label.setAutoFillBackground(False)
        self.db_updated_in_label.setStyleSheet("background-color: white;")
        self.db_updated_in_label.setFrameShape(QtWidgets.QFrame.NoFrame)
        self.db_updated_in_label.setObjectName("db_updated_in_label")
        self.wf_export_button_2 = QtWidgets.QPushButton(self.db_frame)
        self.wf_export_button_2.setGeometry(QtCore.QRect(10, 160, 241, 31))
        self.wf_export_button_2.setDefault(False)
        self.wf_export_button_2.setFlat(False)
        self.wf_export_button_2.setObjectName("wf_export_button_2")
        self.db_authors_input = QtWidgets.QTextEdit(self.db_frame)
        self.db_authors_input.setGeometry(QtCore.QRect(10, 110, 241, 41))
        self.db_authors_input.setObjectName("db_authors_input")
        self.db_source_label.raise_()
        self.db_sqooped_list.raise_()
        self.db_source_label_2.raise_()
        self.db_source_label_3.raise_()
        self.db_workflow_list.raise_()
        self.db_tableau_list.raise_()
        self.db_description_input.raise_()
        self.db_header_label.raise_()
        self.db_created_at_label.raise_()
        self.db_created_at.raise_()
        self.db_partitions_label.raise_()
        self.db_partitions_list.raise_()
        self.db_updated_in_label.raise_()
        self.db_updated_in_list.raise_()
        self.wf_export_button_2.raise_()
        self.db_authors_input.raise_()
        self.tabWidget.addTab(self.db_tab, "")
        self.wf_tab = QtWidgets.QWidget()
        self.wf_tab.setAutoFillBackground(False)
        self.wf_tab.setObjectName("wf_tab")
        self.wf_export_button = QtWidgets.QPushButton(self.wf_tab)
        self.wf_export_button.setGeometry(QtCore.QRect(330, 410, 411, 31))
        self.wf_export_button.setDefault(False)
        self.wf_export_button.setFlat(False)
        self.wf_export_button.setObjectName("wf_export_button")
        self.wf_workflow_list = QtWidgets.QListView(self.wf_tab)
        self.wf_workflow_list.setGeometry(QtCore.QRect(10, 50, 301, 391))
        self.wf_workflow_list.setObjectName("wf_workflow_list")
        self.wf_workflow_search = QtWidgets.QLineEdit(self.wf_tab)
        self.wf_workflow_search.setGeometry(QtCore.QRect(10, 10, 301, 28))
        self.wf_workflow_search.setAutoFillBackground(True)
        self.wf_workflow_search.setObjectName("wf_workflow_search")
        self.wf_effected_list = QtWidgets.QListView(self.wf_tab)
        self.wf_effected_list.setGeometry(QtCore.QRect(330, 230, 411, 171))
        self.wf_effected_list.setObjectName("wf_effected_list")
        self.wf_effected_label = QtWidgets.QLabel(self.wf_tab)
        self.wf_effected_label.setGeometry(QtCore.QRect(330, 210, 411, 21))
        self.wf_effected_label.setAutoFillBackground(False)
        self.wf_effected_label.setStyleSheet("background-color: white;")
        self.wf_effected_label.setFrameShape(QtWidgets.QFrame.NoFrame)
        self.wf_effected_label.setObjectName("wf_effected_label")
        self.wf_source_list = QtWidgets.QListView(self.wf_tab)
        self.wf_source_list.setGeometry(QtCore.QRect(330, 30, 411, 171))
        self.wf_source_list.setObjectName("wf_source_list")
        self.wf_source_label = QtWidgets.QLabel(self.wf_tab)
        self.wf_source_label.setGeometry(QtCore.QRect(330, 9, 411, 21))
        self.wf_source_label.setAutoFillBackground(False)
        self.wf_source_label.setStyleSheet("background-color: white;")
        self.wf_source_label.setFrameShape(QtWidgets.QFrame.NoFrame)
        self.wf_source_label.setObjectName("wf_source_label")
        self.wf_export_button.raise_()
        self.wf_workflow_list.raise_()
        self.wf_workflow_search.raise_()
        self.wf_effected_label.raise_()
        self.wf_source_list.raise_()
        self.wf_source_label.raise_()
        self.wf_effected_list.raise_()
        self.tabWidget.addTab(self.wf_tab, "")
        self.tw_tab = QtWidgets.QWidget()
        self.tw_tab.setObjectName("tw_tab")
        self.tabWidget.addTab(self.tw_tab, "")
        MainWindow.setCentralWidget(self.centralwidget)
        self.menuBar = QtWidgets.QMenuBar(MainWindow)
        self.menuBar.setGeometry(QtCore.QRect(0, 0, 750, 22))
        self.menuBar.setObjectName("menuBar")
        self.menuFile = QtWidgets.QMenu(self.menuBar)
        self.menuFile.setObjectName("menuFile")
        MainWindow.setMenuBar(self.menuBar)
        self.actionOpen = QtWidgets.QAction(MainWindow)
        self.actionOpen.setObjectName("actionOpen")
        self.menuFile.addAction(self.actionOpen)
        self.menuBar.addAction(self.menuFile.menuAction())

        self.retranslateUi(MainWindow)
        self.tabWidget.setCurrentIndex(0)
        QtCore.QMetaObject.connectSlotsByName(MainWindow)

    def retranslateUi(self, MainWindow):
        _translate = QtCore.QCoreApplication.translate
        MainWindow.setWindowTitle(_translate("MainWindow", "Parsing tool"))
        self.db_table_search.setPlaceholderText(_translate("MainWindow", "Search table... "))
        self.db_source_label.setText(_translate("MainWindow", "Based on sqooped tables"))
        self.db_source_label_2.setText(_translate("MainWindow", "Used in workflows"))
        self.db_source_label_3.setText(_translate("MainWindow", "Used in Tableau workbooks"))
        self.db_description_input.setPlaceholderText(_translate("MainWindow", "Table meaning..."))
        self.db_header_label.setText(_translate("MainWindow", "Table description"))
        self.db_created_at_label.setText(_translate("MainWindow", "Created at workflow"))
        self.db_created_at.setText(_translate("MainWindow", "Unknown"))
        self.db_partitions_label.setText(_translate("MainWindow", "Partitions"))
        self.db_updated_in_label.setText(_translate("MainWindow", "Updated at"))
        self.wf_export_button_2.setText(_translate("MainWindow", "Save"))
        self.db_authors_input.setPlaceholderText(_translate("MainWindow", "Table authors..."))
        self.tabWidget.setTabText(self.tabWidget.indexOf(self.db_tab), _translate("MainWindow", "Databases"))
        self.wf_export_button.setText(_translate("MainWindow", "Export results to CSV"))
        self.wf_workflow_search.setPlaceholderText(_translate("MainWindow", "Search workflow... "))
        self.wf_effected_label.setText(_translate("MainWindow", "Effected tables (insert/update)"))
        self.wf_source_label.setText(_translate("MainWindow", "Source tables"))
        self.tabWidget.setTabText(self.tabWidget.indexOf(self.wf_tab), _translate("MainWindow", "Workflows"))
        self.tabWidget.setTabText(self.tabWidget.indexOf(self.tw_tab), _translate("MainWindow", "Tableua Workbooks"))
        self.menuFile.setTitle(_translate("MainWindow", "File"))
        self.actionOpen.setText(_translate("MainWindow", "Open..."))

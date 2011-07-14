#!/usr/bin/env python
#
# ApachePiwikGUI - graphical user interface for Apache2Piwik
# 
# @link http://clearcode.cc/
# @license http://www.gnu.org/licenses/gpl-3.0.html GPL v3 or later
#

import os
import sys
import MySQLdb
import httpagentparser
from PyQt4 import QtGui
from PyQt4 import QtCore
import settings
from threading import Thread
from StringIO import StringIO

class Apache2PiwikWizard(QtGui.QWizard):
    def __init__(self, parent=None):
        QtGui.QWizard.__init__(self, parent)
        
        self.setWindowTitle('Apache2Piwik Wizard')
        screen = QtGui.QDesktopWidget().screenGeometry()
        size = self.geometry()
        self.move((screen.width()-size.width())/2, (screen.height()-size.height())/2)
        self.piwik_sites = []
        self.log_files = []
        self.cursor = None
        self.finished = False
        self.setupPages()

    def setupPages(self):
        page_apache = ApacheWizardPage(self)
        page_settings = SettingsWizardPage(self)
        page_db = DBWizardPage(self)
        page_piwik = PiwikWizardPage(self)
        page_export = ExportWizardPage(self)
        self.addPage(page_apache)
        self.addPage(page_settings)
        self.addPage(page_db)
        self.addPage(page_piwik)
        self.addPage(page_export)
        

class ApacheWizardPage(QtGui.QWizardPage):
    def __init__(self, parent=None):
        QtGui.QWizardPage.__init__(self, parent)

        self.setTitle("Apache")
        self.setSubTitle("Please choose log files and enter log format.")
        layout = QtGui.QVBoxLayout()
        form = QtGui.QFormLayout()
        self.log_format_field = QtGui.QLineEdit("%h %l %u %t \"%r\" %>s %O \"%{Referer}i\" \"%{User-Agent}i\"")
        form.addRow(QtGui.QLabel("Log format"), self.log_format_field)
        self.log_files_list = QtGui.QListWidget()
        form.addRow(QtGui.QLabel("Log files"), self.log_files_list)
        self.add_button = QtGui.QPushButton("add")
        self.remove_button = QtGui.QPushButton("remove") 
        buttons = QtGui.QHBoxLayout()
        buttons.addWidget(self.add_button)
        buttons.addWidget(self.remove_button)
        form.addRow(QtGui.QLabel(""), buttons)
        layout.addLayout(form)
        self.connect(self.add_button, QtCore.SIGNAL("clicked()"), self.__add)
        self.connect(self.remove_button, QtCore.SIGNAL("clicked()"), self.__remove)
        self.ordered_check = QtGui.QCheckBox("Apache logs are ordered chronologically by server time.")
        self.continue_check = QtGui.QCheckBox("I want to run this script later again on data chronologically after from the same server.")
        layout.addWidget(self.ordered_check)
        layout.addWidget(self.continue_check)
        self.setLayout(layout)
        self.registerField("log_format", self.log_format_field)
        self.registerField("chronological_order", self.ordered_check)
        self.registerField("continue", self.continue_check)
        
    def __add(self):
        file = QtGui.QFileDialog.getOpenFileName()
        if file:
            self.log_files_list.addItem(file)
    
    def __remove(self):
        self.log_files_list.takeItem(self.log_files_list.currentRow())
    
    def validatePage(self):
        if self.log_files_list.count() == 0:
            alert = QtGui.QMessageBox()
            alert.setWindowTitle("Error")
            alert.setText("You have to add log files to continue.")
            alert.exec_()
            return False
        else:
            self.wizard().log_files = [self.log_files_list.item(i).text() 
                                       for i in range(self.log_files_list.count())]
            return True
        
class SettingsWizardPage(QtGui.QWizardPage):
    def __init__(self, parent=None):
        QtGui.QWizardPage.__init__(self, parent)
        self.setTitle("Settings")
        self.setSubTitle("Please separate values with comma.")
        layout = QtGui.QVBoxLayout()
        layout.addWidget(QtGui.QLabel("Ignored logs"))
        self.ignored_logs_field = QtGui.QLineEdit("/admin/")
        layout.addWidget(self.ignored_logs_field)
        layout.addWidget(QtGui.QLabel("Ignored user agents"))
        self.ignored_agents_field = QtGui.QLineEdit("bot, crawl, Bot, spider, Spider")
        layout.addWidget(self.ignored_agents_field)
        layout.addWidget(QtGui.QLabel("Ignored file extensions"))
        self.ignored_exts_field = QtGui.QLineEdit(".jpg, .gif, .jpeg, .css, .js, .png, .ico")
        layout.addWidget(self.ignored_exts_field)
        layout.addWidget(QtGui.QLabel("Downloaded file extensions"))
        self.down_exts_field = QtGui.QLineEdit(".zip, .pdf, .doc, .xls, .ppt, .jad, .jar, .cod, .apk")
        layout.addWidget(self.down_exts_field)
        layout.addWidget(QtGui.QLabel("Regular expression to cut urls, the <url> group is taken only"))
        self.url_expr_field = QtGui.QLineEdit("(?P<url>[^;\?]*).*?")
        layout.addWidget(self.url_expr_field)
        layout.addWidget(QtGui.QLabel("Maximum time of one visit (in seconds)"))
        self.max_visit_field = QtGui.QLineEdit("1800")
        layout.addWidget(self.max_visit_field)
        self.setLayout(layout)
        self.registerField("ignored_logs", self.ignored_logs_field)
        self.registerField("ignored_user_agents", self.ignored_agents_field)
        self.registerField("ignored_extensions", self.ignored_exts_field)
        self.registerField("downloaded_extensions", self.down_exts_field)
        self.registerField("url_regexpr", self.url_expr_field)
        self.registerField("visit_length", self.max_visit_field)
        
class DBWizardPage(QtGui.QWizardPage):
    def __init__(self, parent=None):
        QtGui.QWizardPage.__init__(self, parent)
        
        self.setTitle("MySQL")
        self.setSubTitle("Please enter your MySQL configuration.")
        form = QtGui.QFormLayout()
        form.setHorizontalSpacing(40)
        self.db_host_field = QtGui.QLineEdit()
        form.addRow(QtGui.QLabel("Host"), self.db_host_field)
        self.db_port_field = QtGui.QLineEdit("3306")
        form.addRow(QtGui.QLabel("Port"), self.db_port_field)
        self.db_name_field = QtGui.QLineEdit()
        form.addRow(QtGui.QLabel("Name"), self.db_name_field)
        self.db_user_field = QtGui.QLineEdit()
        form.addRow(QtGui.QLabel("User"), self.db_user_field)
        self.db_password_field = QtGui.QLineEdit()
        self.db_password_field.setEchoMode(QtGui.QLineEdit.Password)
        form.addRow(QtGui.QLabel("Password"), self.db_password_field)
        self.db_prefix_field = QtGui.QLineEdit("piwik_")
        form.addRow(QtGui.QLabel("Table prefix"), self.db_prefix_field)
        self.registerField("db_host", self.db_host_field)
        self.registerField("db_port", self.db_port_field)
        self.registerField("db_name", self.db_name_field)
        self.registerField("db_user", self.db_user_field)
        self.registerField("db_password", self.db_password_field)
        self.registerField("db_prefix", self.db_prefix_field)
        self.setLayout(form)        
   
    def validatePage(self):
        try:
            settings.DB_DATABASE_NAME = str(self.field("db_name").toString())
            settings.DB_HOST = str(self.field("db_host").toString())
            settings.DB_PORT = int(self.field("db_port").toString())
            settings.DB_USER = str(self.field("db_user").toString())
            settings.DB_PASSWORD = str(self.field("db_password").toString())
            settings.DB_PIWIK_PREFIX = str(self.field("db_prefix").toString())
            self.__testDB()
        except Exception as e:
            alert = QtGui.QMessageBox()
            alert.setWindowTitle("Error")
            alert.setText("Unable to connect to the database.\n"+str(e))
            alert.exec_()
            return False
        missing_tables = self.__missingTables()
        if len(missing_tables) == 0:
            return True
        else:
            alert = QtGui.QMessageBox()
            alert.setWindowTitle("Error")
            alert.setText("Tables not found:\n"+", ".join(missing_tables))
            alert.exec_()
            return False
        
    def __testDB(self):
        """ Test MySQL connection """
        conn = MySQLdb.connect (host = settings.DB_HOST, user = settings.DB_USER,
                                passwd = settings.DB_PASSWORD, db = settings.DB_DATABASE_NAME,
                                port = settings.DB_PORT)
        self.wizard().cursor = conn.cursor()
        
    def __missingTables(self):
        """ Find missing tables """
        missing = []
        for table in ["log_action", "log_visit", "log_link_visit_action"]:
            table_name = settings.DB_PIWIK_PREFIX + table
            try:
                self.wizard().cursor.execute("SELECT * FROM {name}".format(name=table_name))
            except MySQLdb.ProgrammingError:
                missing.append(table_name)
        return missing

class PiwikWizardPage(QtGui.QWizardPage):
    def __init__(self, parent=None):
        QtGui.QWizardPage.__init__(self, parent)
        
        self.setTitle("Piwik")
        self.setSubTitle("Please select your Piwik website.")
        form = QtGui.QFormLayout()
        form.setHorizontalSpacing(40)
        self.piwik_sites_combo = QtGui.QComboBox()
        form.addRow(QtGui.QLabel("Website"), self.piwik_sites_combo)
        self.registerField("piwik_sites", self.piwik_sites_combo)
        self.setLayout(form)
    
    def initializePage(self):
        self.piwik_sites_combo.clear()
        self.wizard().piwik_sites = self.__getSites()
        for items in self.wizard().piwik_sites:
            self.piwik_sites_combo.addItem(items["name"])
            
    def __getSites(self):
        select_site_sql = "SELECT idsite, name, main_url from %ssite" % settings.PIWIK_PREFIX
        self.wizard().cursor.execute(select_site_sql)
        return [{"id" : id, "name" : name, "url" : url} for (id, name, url)
                in self.wizard().cursor.fetchall()]

class ExportWizardPage(QtGui.QWizardPage):
    def __init__(self, parent=None):
        QtGui.QWizardPage.__init__(self, parent)
        
        self.setTitle("Apache2Piwik")
        self.setSubTitle("Ready to begin.")
        layout = QtGui.QVBoxLayout()
        warning = QtGui.QLabel("Please make sure you have a backup of your database.\n")
        font = QtGui.QFont()
        font.setBold(True)
        warning.setFont(font)
        layout.addWidget(warning)
        layout.addWidget(QtGui.QLabel("Press Finish to begin exporting data.\n"))
        self.setLayout(layout)
   
    def initializePage(self):
        self.setupConfig()
    
    def setupConfig(self):
        settings.LIFE = False
        settings.APACHE_LOG_FILES = [str(x) for x in self.wizard().log_files]
        settings.LOG_FORMAT = str(self.field("log_format").toString())
        settings.CHRONOLOGICAL_ORDER = self.field("chronological_order").toBool()
        settings.CONTINUE = self.field("continue").toBool()
        settings.IGNORED_LOGS = [str(x).strip() for x in self.field("ignored_logs").toString().split(',')]
        settings.IGNORED_USER_AGENTS = [str(x).strip() for x in self.field("ignored_user_agents").toString().split(',')]
        settings.IGNORED_EXTENSIONS = [str(x).strip() for x in self.field("ignored_extensions").toString().split(',')]
        settings.DOWNLOADED_EXTENSIONS = [str(x).strip() for x in self.field("downloaded_extensions").toString().split(',')]
        settings.URL_REGEXPR = str(self.field("url_regexpr").toString())
        settings.VISIT_LENGTH = self.field("visit_length").toInt()[0]
        selected = self.field("piwik_sites").toInt()[0]
        settings.ID_SITE = self.wizard().piwik_sites[selected]["id"]
        
    def validatePage(self):
        self.wizard().finished = True
        return True
    
class LogsGui(QtGui.QWidget):
    """ Displays export logs from the Apache2Piwik plugin """
    def __init__(self, parent=None):
        QtGui.QWidget.__init__(self, parent)
        self.setGeometry(0, 0, 600, 350)

        self.setWindowTitle('Apache2Piwik')
        screen = QtGui.QDesktopWidget().screenGeometry()
        size = self.geometry()
        self.move((screen.width()-size.width())/2, (screen.height()-size.height())/2)
        
        self.layout = QtGui.QVBoxLayout()
        self.log = QtGui.QTextEdit()
        self.log.setReadOnly(True)
        self.button = QtGui.QPushButton("Close")
        self.button.setEnabled(False)
        self.layout.addWidget(self.log)
        self.layout.addWidget(self.button)
        self.setLayout(self.layout)
        
        self.buffer = StringIO()
        sys.stdout = self.buffer
        self.startWorker()
    
    def startWorker(self):
        """ Starts Apache2Piwik worker and a timer. """
        self.timer = QtCore.QTimer()
        self.connect(self.timer, QtCore.SIGNAL("timeout()"), self.updateLog)
        self.timer.start(1000)
        self.worker = Apache2PiwikWrapper()
        self.worker.start()
        self.connect(self.button, QtCore.SIGNAL("clicked()"), self.cleanUp)
        
    def updateLog(self):
        """ Updates logs on timer signal. """
        self.log.clear()
        if self.worker.isAlive():
            self.log.append(self.buffer.getvalue())
        else:
            self.timer.stop()
            self.button.setEnabled(True)
            self.log.append(self.buffer.getvalue())
            self.log.append("Finished.")
        self.log.ensureCursorVisible()
        
    def closeEvent(self, arg):
        if self.worker.isAlive():
            arg.ignore()
        else:
            arg.accept()
        
    def cleanUp(self):
        self.worker.join()
        self.close()
        
class Apache2PiwikWrapper(Thread):
    """ Runs Apache2Piwik in a separate thread. """
    def run(self):
        try:
            dir = os.getcwd()
            import apache2piwik
            apache2piwik.apache2piwik(dir)
        except Exception as e:
            print "Error: ", e
            print "Please check the configuration you provided."


if __name__ == '__main__':
    # Run wizard configuration
    wiz_app = QtGui.QApplication(sys.argv)
    wiz = Apache2PiwikWizard()
    wiz.show()
    wiz_app.exec_()
    
    # Run Apache2Piwik
    if wiz.finished:
        logs = LogsGui()
        logs.show()
        wiz_app.exec_()

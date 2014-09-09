from lunchinator.plugin import iface_db_plugin, lunch_db
import sys, sqlite3, threading, Queue, datetime, os
from lunchinator import get_server, get_settings, convert_raw
from db_SQLite.multithreaded_sqlite import MultiThreadSQLite
 
class db_MySQL(iface_db_plugin):
    def __init__(self):
        super(db_MySQL, self).__init__()
        self.options=[((u"host", u"Host"), ""),
                      ((u"user", u"Username"), ""),
                      ((u"pass", u"Password"), ""),
                      ((u"db", u"Database"), "")]
        
    def get_displayed_name(self):
        return u"MySQL Connection"
        
    def create_connection(self, options):
        newconn = None
        try:
            db = _MySQLDB(options)
            db.open(self.logger)
            return db
        except:
            self.logger.exception("Problem while opening MySQL connection")   
            raise
        
        return newconn

class _MySQLDB(lunch_db):
    def __init__(self, options):
        self.options = options
        
    def open(self, _logger):
        import mysql.connector
        self._cnx = mysql.connector.connect(host=self.options[u"host"],
                                            user=self.options[u"user"],
                                            password=self.options[u"pass"],
                                            database=self.options[u"db"])
        self.is_open = True
        
    def close(self, _logger):
        self._cnx.close()
        self.is_open = False
    
    def _execute(self, _logger, query, wildcards, returnResults=True, commit=False, returnHeader=False):
        cursor = self._cnx.cursor()
        try:
            query = convert_raw(query)
            queryArgs = [val.encode('utf-8') if type(val) is unicode else val for val in wildcards]
            cursor.execute(query, queryArgs)
            
            if commit:
                self._cnx.commit()
                
            result = [row for row in cursor]
            if returnHeader:
                return cursor.column_names, result
            if returnResults:
                return result
        finally:
            cursor.close()
    
    def existsTable(self, logger, tableName):
        result = self.query(logger, "show tables like ?", tableName)
        return result != None and len(result) > 0
    
    def insert_values(self, logger, table, *values):
        q = "insert into %s values(%s)" % (table, ','.join('?'*len(values)))
        self.execute(logger, q, *values)
        
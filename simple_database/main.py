from . import config
from . import exceptions
import json
import os
import sys
from pydoc import locate
import datetime
import copy

class connect_database(object):
    
    def __init__(self, db_name):
        self._db_name = db_name
        self._db_path = self._check_db_exists()
        self._load_tables()
    
         
    def _load_tables(self):
        tables = [table.split('.')[0] for tables in os.walk(self._db_path) 
                  for table in tables[2]]
        for table in tables:
            if not hasattr(self, table):
                setattr(self, table, 
                        _table_cls(os.path.join(self._db_path+table+'.json')))
        return tables


    def _check_db_exists(self):
        path = os.path.join(config.BASE_DB_FILE_PATH,self._db_name+'/')
        if not isinstance(self, create_database):
            if not os.path.exists(path):
                raise exceptions.ValidationError('Database with name "{}" '
                                                 'doesn\'t exist.'.
                                                 format(self._db_name))
        if isinstance(self, create_database):
            if os.path.exists(path):
                raise exceptions.ValidationError('Database with name "{}" '
                                                 'already exists.'.
                                                 format(self._db_name))
            os.makedirs(path)
        return path
            
    def show_tables(self):
        return self._load_tables()
    
    def create_table(self, table_name, columns=None):
        if table_name in self._load_tables():
            raise exceptions.ValidationError('Table with name "{}" '
                                             'already exists.'.
                                             format(table_name))
        table_json = os.path.join(self._db_path,table_name+'.json')
        with open(table_json, 'w') as f:
            headers = {"headers":columns}
            json.dump(headers, f)
        setattr(self, table_name, _table_cls(table_json))
        return self        
        
    
class create_database(connect_database):
    pass
    
  

class _table_cls(object):
    
    def __init__(self, table_json):
        self._table_json = table_json
        
    def _load_data(self):
        with open(self._table_json) as f:
            return json.load(f)

    def count(self):
        return len(self._load_data()) -1     
    
    def describe(self):
        return self._load_data()['headers']

    def query(self, **kwargs):
        pass
    
    def all(self):
        pass

    def insert(self, *args):
        table_data = self._load_data()
        table_headers = copy.deepcopy(self._load_data()['headers'])
        if len(args) != len(table_headers):
            raise exceptions.ValidationError('Invalid amount of field')
        row_id = args[0]
        row_data = {}
        for index, arg in enumerate(args):
            
            tbl_hdr_type = table_headers[index]['type']
            if table_headers[index]['type'] == 'date':
                tbl_hdr_type = 'datetime.date'
            if not isinstance(arg, locate(tbl_hdr_type)):
            #if not type(arg).__name__ == tbl_hdr_type:
                raise exceptions.ValidationError(
                                'Invalid type of field "{0}": Given "{1}", expected "{2}"'.
                                 format(table_headers[index]['name'], type(arg).__name__, table_headers[index]['type']))
            if isinstance(arg, datetime.date):
                arg = arg.isoformat()
            row_data[table_headers[index]['name']] = arg
        table_data[row_id] = row_data
        with open(self._table_json, 'w') as f:
            json_data = json.dumps(table_data)
            f.write(json_data)
            
        pass
    
class QuerySet(object):
    
    def __init__(self, query):
        self.counter = 0
        self.query = query
    
    def __iter__(self):
        return self
        
    def __next__(self):
        pass
    
    next = __next__
    
    

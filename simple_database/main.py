from . import config
from . import exceptions
import json
import os
import sys
from pydoc import locate
import datetime
import copy



class connect_database(object):
    """Class for providing a connection to a Database object and provide
       methods to update and query tables within the Database object"""
    
    def __init__(self, db_name):
        self._db_name = db_name
        self._db_path = self._check_db_exists()
        self._load_tables()
    
         
    
    def _load_tables(self):
        """Return table attributes equal to table objects for each json file
           in database directory"""
        tables = [table.split('.')[0] for tables in os.walk(self._db_path) 
                  for table in tables[2]]
        for table in tables:
            if not hasattr(self, table):
                setattr(self, table, 
                        _table_cls(os.path.join(self._db_path+table+'.json')))
        return tables


    
    def _check_db_exists(self):
        path = os.path.join(config.BASE_DB_FILE_PATH,self._db_name+'/')
        
        # If method called by connect_database class and database doesn't exist raise exception.
        if not isinstance(self, create_database):
            if not os.path.exists(path):
                raise exceptions.ValidationError('Database with name "{}" '
                                                 'doesn\'t exist.'.
                                                 format(self._db_name))
        
        # if method called by create_database class and database exists raise exception.
        if isinstance(self, create_database):
            if os.path.exists(path):
                raise exceptions.ValidationError('Database with name "{}" '
                                                 'already exists.'.
                                                 format(self._db_name))
            os.makedirs(path)
        return path
            
    
    
    def show_tables(self):
        """Returns a list of tables within the Database"""
        return self._load_tables()
    
    
    
    def create_table(self, table_name, columns=None):
        """Create a new table within the database
            
            Args:
                table_name(str): String to define the table name.
                
                columns(list): A list of dictonaries containing key / value
                               pairs defining the column name and data type.
        """
        
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
    """Empty subclass of connect_database that provides ability to create a new
       database and return a database object."""
    pass
    
  

class _table_cls(object):
    """Private class to provide a table object with methods for querying and 
       updating records within the table"""
       
    def __init__(self, table_json):
        self._table_json = table_json
        
    
    
    def _load_data(self):
        with open(self._table_json) as f:
            return json.load(f)

    
    
    def count(self):
        """Returns total count of records within the table"""
        
        return len(self._load_data()) -1     
    
    
    
    def describe(self):
        """Returns a list of dictonaries with key / value pairs describing 
           column names and data types"""
           
        return self._load_data()['headers']

    
    
    def query(self, **kwargs):
        '''Returns a generator with records matching the kwargs'''
        #Query all and yield only the objects that match
        check = self.all()
        for row in check:
            #Since all cannot be used any is used
            if not any([True for kw in kwargs if getattr(row, kw) != kwargs[kw]]):
                yield row
                
    
    
    def all(self):
        """Returns a generator of all records within the table"""
        table_data = [data for data in self._load_data().items()
                      if 'headers' not in data]
        for row in sorted(table_data):
            yield _QuerySet(**row[1])
            

    
    def _validate(self, arg, header):
        """Private method to validate argument data type is valid for it's
           listed column"""
        
        if header['type'] == 'date':
            required_type = locate('datetime.date')
        
        else:
            required_type = locate(header['type'])
        
        if not isinstance(arg, required_type):
            raise exceptions.ValidationError(
                                'Invalid type of field "{0}": Given "{1}",'
                                ' expected "{2}"'.
                                 format(header['name'], 
                                        type(arg).__name__, 
                                        header['type']))
        
        # If argument is a datetime object convert to isoformat so it can be encoded into json
        if isinstance(arg, datetime.date):
            return arg.isoformat()
        
        return arg
        
    
    
    def insert(self, *args):
        """Provides method to insert a record into the table
           
           Must provide a list of arguments equal to number of columns in the
           table and they must meet the data type required for the column
        """
        table_data = self._load_data()
        table_headers = copy.deepcopy(self._load_data()['headers'])
        
        # if not enough arguments raise exception
        if len(args) != len(table_headers):
            raise exceptions.ValidationError('Invalid amount of field')
        
        # First agument becomes the row identifier
        row_id = args[0]
        row_data = {}
        for index, arg in enumerate(args):
            # Validate arugment data type
            validated_arg = self._validate(arg, table_headers[index])
            # Add validated argument to row data
            row_data[table_headers[index]['name']] = validated_arg
        
        # Insert the row data into the table
        table_data[row_id] = row_data
        
        # Write the updated table data into a table json file
        with open(self._table_json, 'w') as f:
            json_data = json.dumps(table_data)
            f.write(json_data)
    


class _QuerySet(object):
    '''Provides a query object with attributes for each column within a row'''
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)
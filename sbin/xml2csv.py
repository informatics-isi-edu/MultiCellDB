#!/usr/bin/python

"""
This script generates the CSV files from the XML files.
Each CSV file corresponds to a SQL table name.

Parameters:

    - input: the directory with the XML files
    - output: the directory with the CSV files
    - file: the file with the order in which the CSV files will be loaded
    
"""

import sys
import os
#import glob
from optparse import OptionParser
import json
import xml.etree.ElementTree as ET
from httplib import HTTPSConnection, OK
import json

parser = OptionParser()
parser.header = {}
parser.add_option('-c', '--config', action='store', dest='config', type='string', help='Configuration file')

(options, args) = parser.parse_args()

if not options.config:
    print 'ERROR: Missing configuration file'
    sys.exit(1)
    
tablesNames = []
tablesDefinitions = {}
tablesReferences = {}
tablesSortedNames = []
tablesData = {}
columnTypes = {}

class ConfigClient (object):

    def __init__(self, **kwargs):
        self.options = kwargs.get("options")
        self.cfg = None
        
    def load(self):
        self.cfg = {}
        if os.path.exists(self.options.config):
            f = open(self.options.config, 'r')
            try:
                self.cfg = json.load(f)
            except ValueError as e:
                sys.stderr.write('Malformed configuration file: %s\n' % e)
                sys.exit(1)
            else:
                f.close()
        else:
            sys.stderr.write('Configuration file: "%s" does not exist.\n' % self.options.config)
            sys.exit(1)
            
    def validate(self):
        self.host = self.cfg.get('host', None)
        if not self.host:
            sys.stderr.write('Ermrest host name must be given.\n')
            sys.exit(1)
        self.catalog = self.cfg.get('catalog', None)
        if not self.catalog:
            sys.stderr.write('Ermrest catalog must be given.\n')
            sys.exit(1)
        self.schema = self.cfg.get('schema', None)
        if not self.schema:
            sys.stderr.write('Ermrest schema must be given.\n')
            sys.exit(1)
        self.input = self.cfg.get('input', None)
        if not self.input:
            sys.stderr.write('Input directory must be given.\n')
            sys.exit(1)
        if not os.path.exists(self.input):
            sys.stderr.write('Input directory must exist.\n')
            sys.exit(1)
        self.output = '%s/csv' % self.input
        if not os.path.exists(self.output):
            os.mkdir(self.output)
        for f in os.listdir(self.output):
            os.remove('%s/%s' % (self.output,f))
        self.file = '%s/csv.txt' % self.output
            
    def get(self, field):
        if field=='host':
            return self.host
        elif field=='catalog':
            return self.catalog
        elif field=='schema':
            return self.schema
        elif field=='input':
            return self.input
        elif field=='output':
            return self.output
        elif field=='file':
            return self.file
        else:
            return None
        
class ErmrestClient (object):

    def __init__(self, **kwargs):
        self.host = kwargs.get("host")
        self.schema = kwargs.get("schema")
        self.catalog = kwargs.get("catalog")
        self.webconn = None
        self.headers = dict(Accept='application/json')
        
    def connect(self):
        self.webconn = HTTPSConnection(self.host)
            
    def get_request(self, url):
        self.webconn.request('GET', url, '', self.headers)
        resp = self.webconn.getresponse()
        if resp.status != OK:
            sys.stderr.write('Unexpected HTTP status: %d' % resp.status)
            sys.exit(1)
        return resp
    
    def load(self, table):
        resp = self.get_request('/ermrest/catalog/%d/schema/%s' % (self.catalog, self.schema))
        res = json.loads(resp.read())
        tables = []
        for key,value in res['tables'].iteritems():
            tables.append(key)
        for db_table in tables:
            resp = self.get_request('/ermrest/catalog/%d/aggregate/%s:%s/max:=max(id)' % (self.catalog, self.schema, db_table))
            res = json.loads(resp.read())
            max_val = res[0]['max']
            if max_val==None:
                max_val = 0
            tablesData[db_table] = {}
            tablesData[db_table]['id'] = max_val
            tablesData[db_table]['data'] = []
            
class XMLClient (object):
    
    def __init__(self, **kwargs):
        self.input = kwargs.get("input")
        self.tablesNames = kwargs.get("tablesNames")
        self.tablesDefinitions = kwargs.get("tablesDefinitions")
        self.tablesReferences = kwargs.get("tablesReferences")
        self.tablesData = kwargs.get("tablesData")
        self.columnTypes = kwargs.get("columnTypes")
        self.files=[f for f in os.listdir(self.input) if f.endswith('.xml')]

        
    def process_input(self):
        for f in self.files:  
            self.process_XML_file(f) 
        
    def process_XML_file(self, f):
        tree = ET.parse('%s/%s' % (self.input, f))
        root = tree.getroot()
        self.process_file(f, root, None)
        
    """
    Populate the data structures for the SQL tables
    """
    def process_file(self, f, elem, parent):
        hasAttributes = len(elem.attrib.keys()) > 0
        
        if hasAttributes:
            if elem.tag not in self.tablesNames:
                self.tablesNames.append(elem.tag)
            if elem.tag not in self.tablesDefinitions.keys():
                self.tablesDefinitions[elem.tag] = []
            if parent!=None and elem.tag not in self.tablesReferences.keys():
                self.tablesReferences[elem.tag] = []
            if parent!=None and parent.tag not in self.tablesReferences[elem.tag]:
                self.tablesReferences[elem.tag].append(parent.tag)
            for attr,value in elem.attrib.items():
                attrib = '@%s' % attr
                if attrib not in self.tablesDefinitions[elem.tag]:
                    self.tablesDefinitions[elem.tag].append(attrib)
                    self.setType(elem.tag, attrib, self.getType(value))
    
        if len(elem)==0:
            if parent==None:
                sys.stderr.write('Unexpected parent: None')
                sys.exit(1)
            if not hasAttributes:
                if elem.tag not in self.tablesDefinitions[parent.tag]:
                    self.tablesDefinitions[parent.tag].append(elem.tag)
            else:
                if '#text' not in self.tablesDefinitions[elem.tag]:
                    self.tablesDefinitions[elem.tag].append('#text')
            if self.isMultiValue(elem, parent):
                self.setType(parent.tag, elem.tag, 'text')
            else:
                self.setType(parent.tag, elem.tag, self.getType(elem.text))
        else:
            if not hasAttributes:
                if elem.tag not in self.tablesNames:
                    self.tablesNames.append(elem.tag)
                if elem.tag not in self.tablesDefinitions.keys():
                    self.tablesDefinitions[elem.tag] = []
                if parent!=None and elem.tag not in self.tablesReferences.keys():
                    self.tablesReferences[elem.tag] = []
                if parent!=None and parent.tag not in self.tablesReferences[elem.tag]:
                    self.tablesReferences[elem.tag].append(parent.tag)
            for child in elem:
                self.process_file(f, child, elem)
        
    """
    Guess the type of a value
    """
    def getType(self, value):
        try:
            v = int(value)
            return 'int4'
        except:
            try:
                v = float(value)
                return 'float8'
            except:
                return 'text'
        
    """
    Check if the tag is an array
    """
    def isMultiValue(self, elem, parent):
        count = 0
        for child in parent:
            if child.tag==elem.tag:
                count += 1
        return count >= 2
        
    """
    Set the SQL type of a value
    """
    def setType(self, table, column, col_type):
        if col_type=='int4':
            if table not in self.columnTypes.keys():
                self.columnTypes[table] = {}
            if column not in self.columnTypes[table].keys():
                self.columnTypes[table][column] = 'int4'
        elif col_type=='float8':
            if table not in self.columnTypes.keys():
                self.columnTypes[table] = {}
            if column not in self.columnTypes[table].keys() or self.columnTypes[table][column]=='int4':
                self.columnTypes[table][column] = 'float8'
        elif col_type=='text':
            if table not in self.columnTypes.keys():
                self.columnTypes[table] = {}
            if column not in self.columnTypes[table].keys():
                self.columnTypes[table][column] = 'text'
        
    def load_data(self):
        for f in self.files:  
            tree = ET.parse('%s/%s' % (self.input, f))
            root = tree.getroot()
            self.load_file_data(f, root, None, None) 
        
    """
    Populate the data structures for the SQL data
    """
    def load_file_data(self, f, elem, parent, parent_obj):
        obj = {}
        hasAttributes = len(elem.attrib.keys()) > 0
        if hasAttributes or len(elem)>0:
            if elem.tag not in self.tablesData.keys():
                self.tablesData[elem.tag] = {}
                self.tablesData[elem.tag]['id'] = 0
                self.tablesData[elem.tag]['data'] = []
            self.tablesData[elem.tag]['id'] = self.tablesData[elem.tag]['id']+1
            self.tablesData[elem.tag]['data'].append(obj)
            obj['id'] = self.tablesData[elem.tag]['id']
            if parent!=None and elem.tag in self.tablesReferences.keys() and parent.tag in self.tablesReferences[elem.tag]:
                col = '%s_id' % parent.tag
                obj[col] = self.tablesData[parent.tag]['id']
        if hasAttributes:
            for attr,value in elem.attrib.items():
                attrib = '@%s' % attr
                obj[attrib] = value
                
        if len(elem)==0:
            value = elem.text
            if value!=None:
                if self.isMultiValue(elem, parent):
                    value = self.getMultiValue(elem, parent)
                if hasAttributes:
                    obj['#text'] = value
                else:
                    parent_obj[elem.tag] = value
        else:
            for child in elem:
                self.load_file_data(f, child, elem, obj)
        
    """
    Get the multi value of a tag
    """
    def getMultiValue(self, elem, parent):
        value = []
        for child in parent:
            if child.tag==elem.tag:
                value.append(child.text)
        return ','.join(value)
        
class CSVClient (object):
    
    def __init__(self, **kwargs):
        self.csvFiles = None
        self.file = kwargs.get("file")
        self.output = kwargs.get("output")
        self.tablesNames = kwargs.get("tablesNames")
        self.tablesSortedNames = kwargs.get("tablesSortedNames")
        self.tablesReferences = kwargs.get("tablesReferences")
        self.tablesData = kwargs.get("tablesData")
        self.tablesDefinitions = kwargs.get("tablesDefinitions")
        
    """
    Sort the tables to be created based on the dependencies (references)
    """
    def sortTablesDefinitions(self):
        sorted = False
        while not sorted:
            sorted = True
            for t in self.tablesNames:
                if t not in self.tablesSortedNames:
                    if t not in self.tablesReferences.keys():
                        self.tablesSortedNames.append(t)
                        sorted = False
                    else:
                        resolved = True
                        for refTable in self.tablesReferences[t]:
                            if refTable not in self.tablesSortedNames:
                                resolved = False
                                break
                        if resolved:
                            self.tablesSortedNames.append(t)
                            sorted = False
        
    def load_data(self):
        self.csvFiles = open('%s' % self.file, 'w')
        for table in self.tablesSortedNames:
            self.insert_csv_data(table)
        self.csvFiles.close()
        
    def csvValue(self, value):
        if isinstance(value,basestring):
            value = value.replace('"','""')
            value = '"%s"' % value.encode('utf8')
        else:
            value = str(value)
        return value
        
    """
    Insert the data for a table
    """
    def insert_csv_data(self, table):
        if table in self.tablesData.keys():
            self.csvFiles.write('%s/%s.csv\n' % (self.output, table))
            out = open('%s/%s.csv' % (self.output, table), 'w')
            colsDefs = ['id']
            colsDefs.extend(self.tablesDefinitions[table])
            if table in self.tablesReferences.keys():
                colsRef = []
                for col in self.tablesReferences[table]:
                    colsRef.append('%s_id' % col)
                colsDefs.extend(colsRef)
            out.write('%s\n' % ','.join(colsDefs))
            for data in self.tablesData[table]['data']:
                row = []
                for col in colsDefs:
                    if col in data.keys():
                        row.append(self.csvValue(data[col]))
                    else:
                        row.append('')
                out.write('%s\n' % ','.join(row))
            out.close()

        
config_client = ConfigClient(options=options)
config_client.load()
config_client.validate()
ermrest_client = ErmrestClient(host=config_client.get('host'), catalog=config_client.get('catalog'), schema=config_client.get('schema'))
ermrest_client.connect()
ermrest_client.load(tablesData)
xml_client = XMLClient(input=config_client.get('input'), tablesNames=tablesNames, tablesDefinitions=tablesDefinitions, tablesReferences=tablesReferences, columnTypes=columnTypes, tablesData=tablesData)
xml_client.process_input()
xml_client.load_data()
csv_client = CSVClient(tablesNames=tablesNames, tablesSortedNames=tablesSortedNames, tablesReferences=tablesReferences, tablesData=tablesData, file=config_client.get('file'), output=config_client.get('output'), tablesDefinitions=tablesDefinitions)
csv_client.sortTablesDefinitions()
csv_client.load_data()

# if all files of the input directory have the .xml extension
#files=os.listdir(options.input)  

# or the below sequence to get only the *.xml files with glob.glob()
#files=[]   
#xml_files=glob.glob('%s/*.xml' % options.input)   
#for f in xml_files:
#    files.append(f.split('/')[-1])

""" DEBUG Traces

print 'tablesNames'
print json.dumps(tablesNames, indent=4) 
print '\n\ntablesDefinitions'
print json.dumps(tablesDefinitions, indent=4) 
print '\n\ntablesReferences'
print json.dumps(tablesReferences, indent=4) 
print '\n\ntablesSortedNames'
print json.dumps(tablesSortedNames, indent=4) 
print '\n\ntablesData'
print json.dumps(tablesData, indent=4) 
print 'columnTypes'
print json.dumps(columnTypes, indent=4) 

"""

sys.exit(0)

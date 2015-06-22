#!/usr/bin/python

"""
This script generates the SQL statements for creating and loading the tables.

Parameters:

    - input: the directory with the XML files
    - schema: the schema name
    - thumbnail: the column name to set the thumbnail annotation
    - title: the column name to set the title annotation
    - root: the top level table to be viewed 
    - output: the SQL file for creating and loading the tables
    
An SQL file with the schema name followed by the suffix _annotation will contain the statements 
to be executed for the model_table_annotation and model_column_annotation tables.
    
"""

import sys
import os
from optparse import OptionParser
import json
import xml.etree.ElementTree as ET

parser = OptionParser()
parser.header = {}
parser.add_option('-i', '--input', action='store', dest='input', type='string', help='Input directory')
parser.add_option('-o', '--output', action='store', dest='output', type='string', help='Output file')
parser.add_option('-s', '--schema', action='store', dest='schema', type='string', help='Schema name')
parser.add_option('-t', '--title', action='store', dest='title', type='string', help='The title column')
parser.add_option('-p', '--thumbnail', action='store', dest='thumbnail', type='string', help='The thumbnail column')
parser.add_option('-r', '--root', action='store', dest='root', type='string', help='The root table')

(options, args) = parser.parse_args()

title = None
thumbnail = None

if not options.input:
    print 'ERROR: Missing input directory'
    sys.exit(1)
    
if not options.output:
    print 'ERROR: Missing output file'
    sys.exit(1)

if not options.schema:
    print 'ERROR: Missing schema name'
    sys.exit(1)

if not options.root:
    print 'ERROR: Missing root table'
    sys.exit(1)

if not options.title:
    print 'WARNING: Missing title column'
else:
    title = options.title

if not options.thumbnail:
    print 'WARNING: Missing thumbnail column'
else:
    thumbnail = options.thumbnail

schema='"%s"' % options.schema

out_annotation = file('%s_annotation.sql' % options.schema, 'w')
out = file(options.output, 'w')


tablesList = []
tablesDict = {}

tablesNames = []
tablesDefinitions = {}
tablesReferences = {}
tablesSortedNames = []

tablesData = {}
columnTypes = {}

"""
Guess the type of a value
"""
def getType(value):
    try:
        v = int(value)
        return 'integer'
    except:
        try:
            v = float(value)
            return 'double precision'
        except:
            return 'text'
        
"""
Set the SQL type of a value
"""
def setType(table, column, col_type):        
    if col_type=='integer':
        if table not in columnTypes.keys():
            columnTypes[table] = {}
        if column not in columnTypes[table].keys():
            columnTypes[table][column] = 'integer'
    elif col_type=='double precision':
        if table not in columnTypes.keys():
            columnTypes[table] = {}
        if column not in columnTypes[table].keys() or columnTypes[table][column]=='integer':
            columnTypes[table][column] = 'double precision'
    elif col_type=='text':
        if table not in columnTypes.keys():
            columnTypes[table] = {}
        if column not in columnTypes[table].keys():
            columnTypes[table][column] = 'text'
            
"""
Sort the tables to be created based on the dependencies (references)
"""
def sortTablesDefinitions():
    sorted = False
    while not sorted:
        sorted = True
        for t in tablesNames:
            if t not in tablesSortedNames:
                if t not in tablesReferences.keys():
                    tablesSortedNames.append(t)
                    sorted = False
                else:
                    resolved = True
                    for refTable in tablesReferences[t]:
                        if refTable not in tablesSortedNames:
                            resolved = False
                            break
                    if resolved:
                        tablesSortedNames.append(t)
                        sorted = False
                        
"""
Check if the tag is an array
"""
def isMultiValue(elem, parent):
    count = 0
    for child in parent:
        if child.tag==elem.tag:
            count += 1
    return count >= 2

"""
Get the multi value of a tag
"""
def getMultiValue(elem, parent):
    value = []
    for child in parent:
        if child.tag==elem.tag:
            value.append(child.text)
    return ','.join(value)

"""
Populate the data structures for the SQL tables
"""
def process_file(f, elem, parent):
    hasAttributes = len(elem.attrib.keys()) > 0
    
    if hasAttributes:
        if elem.tag not in tablesNames:
            tablesNames.append(elem.tag)
        if elem.tag not in tablesDefinitions.keys():
            tablesDefinitions[elem.tag] = []
        if parent!=None and elem.tag not in tablesReferences.keys():
            tablesReferences[elem.tag] = []
        if parent!=None and parent.tag not in tablesReferences[elem.tag]:
            tablesReferences[elem.tag].append(parent.tag)
        for attr,value in elem.attrib.items():
            attrib = '@%s' % attr
            if attrib not in tablesDefinitions[elem.tag]:
                tablesDefinitions[elem.tag].append(attrib)
                setType(elem.tag, attrib, getType(value))

    if len(elem)==0:
        if parent==None:
            print 'Unexpected parent: None'
            sys.exit(1)
        if not hasAttributes:
            if elem.tag not in tablesDefinitions[parent.tag]:
                tablesDefinitions[parent.tag].append(elem.tag)
        else:
            if '#text' not in tablesDefinitions[elem.tag]:
                tablesDefinitions[elem.tag].append('#text')
        if isMultiValue(elem, parent):
            setType(parent.tag, elem.tag, 'text')
        else:
            setType(parent.tag, elem.tag, getType(elem.text))
    else:
        if not hasAttributes:
            if elem.tag not in tablesNames:
                tablesNames.append(elem.tag)
            if elem.tag not in tablesDefinitions.keys():
                tablesDefinitions[elem.tag] = []
            if parent!=None and elem.tag not in tablesReferences.keys():
                tablesReferences[elem.tag] = []
            if parent!=None and parent.tag not in tablesReferences[elem.tag]:
                tablesReferences[elem.tag].append(parent.tag)
        for child in elem:
            process_file(f, child, elem)
    
"""
Generate the SQL statements for a table as well as its annotations
"""
def create_sql_table(table):
    if table!=options.root:
        out_annotation.write('INSERT INTO _ermrest.model_table_annotation VALUES(\'%s\', \'%s\', \'comment\', \'["exclude", "nested"]\');\n' % (options.schema, table))
    out.write('CREATE TABLE %s."%s"\n' % (schema,table))
    out.write('(\n')
    out.write('\t"id" integer PRIMARY KEY')
    out_annotation.write('INSERT INTO _ermrest.model_column_annotation VALUES(\'%s\', \'%s\', \'id\', \'comment\', \'["hidden"]\');\n' % (options.schema, table))
    for col in tablesDefinitions[table]:
        if thumbnail!=None and col==thumbnail:
            out_annotation.write('INSERT INTO _ermrest.model_column_annotation VALUES(\'%s\', \'%s\', \'%s\', \'comment\', \'["thumbnail"]\');\n' % (options.schema, table, col))
        if title!=None and col==title:
            out_annotation.write('INSERT INTO _ermrest.model_column_annotation VALUES(\'%s\', \'%s\', \'%s\', \'comment\', \'["title"]\');\n' % (options.schema, table, col))
        if col not in tablesReferences.keys() or table not in tablesReferences[col]:
            col_type = 'text'
            if table in columnTypes.keys() and col in columnTypes[table].keys():
                col_type = columnTypes[table][col]
            out.write(',\n\t"%s" %s' % (col, col_type))
    if table in tablesReferences.keys():
        for col in tablesReferences[table]:
            out.write(',\n\t"%s_id" integer' % col)
        for col in tablesReferences[table]:
            out_annotation.write('INSERT INTO _ermrest.model_column_annotation VALUES(\'%s\', \'%s\', \'%s_id\', \'comment\', \'["hidden"]\');\n' % (options.schema, table, col))
            out.write(',\n\tFOREIGN KEY ("%s_id") REFERENCES %s."%s" (id)' % (col,schema,col))
    out.write('\n);\n\n')


"""
Get the set of columns and values for a row to be inserted into the table
"""
def getRow(data):
    ret={}
    ret['columns']=[]
    ret['values']=[]
    for column,value in data.items():
        ret['columns'].append('"%s"' % column)
        if isinstance(value,basestring):
            value = value.replace("'","''")
        ret['values'].append("'%s'" % value)
    ret['columns'] = ','.join(ret['columns'])
    ret['values'] = ','.join(ret['values'])
    return ret
    
"""
Insert the data for a table
"""
def insert_sql_data(table):
    if table in tablesData.keys():
        for data in tablesData[table]['data']:
            columns=getRow(data)['columns']
            values=getRow(data)['values']
            out.write(('INSERT INTO %s."%s" (%s) VALUES(%s);\n' % (schema,table,columns,values)).encode('utf8'))
    out.write('\n\n')


"""
Populate the data structures for the SQL data
"""
def load_file_data(f, elem, parent, parent_obj):
    obj = {}
    hasAttributes = len(elem.attrib.keys()) > 0
    if hasAttributes or len(elem)>0:
        if elem.tag not in tablesData.keys():
            tablesData[elem.tag] = {}
            tablesData[elem.tag]['id'] = 0
            tablesData[elem.tag]['data'] = []
        tablesData[elem.tag]['id'] = tablesData[elem.tag]['id']+1
        tablesData[elem.tag]['data'].append(obj)
        obj['id'] = tablesData[elem.tag]['id']
        if parent!=None and elem.tag in tablesReferences.keys() and parent.tag in tablesReferences[elem.tag]:
            col = '%s_id' % parent.tag
            obj[col] = tablesData[parent.tag]['id']
    if hasAttributes:
        for attr,value in elem.attrib.items():
            attrib = '@%s' % attr
            obj[attrib] = value
            
    if len(elem)==0:
        value = elem.text
        if value!=None:
            if isMultiValue(elem, parent):
                value = getMultiValue(elem, parent)
            if hasAttributes:
                obj['#text'] = value
            else:
                parent_obj[elem.tag] = value
    else:
        for child in elem:
            load_file_data(f, child, elem, obj)
    
files=os.listdir(options.input)     

def process_XML_file(f):
    tree = ET.parse('%s/%s' % (options.input, f))
    root = tree.getroot()
    process_file(f, root, None)

for f in files:  
    process_XML_file(f) 

for f in files:  
    tree = ET.parse('%s/%s' % (options.input, f))
    root = tree.getroot()
    load_file_data(f, root, None, None) 
    
sortTablesDefinitions()

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

out_annotation.write('BEGIN;\n\n')
out_annotation.write('DELETE FROM _ermrest.model_table_annotation where schema_name=\'%s\';\n' % options.schema)
out_annotation.write('DELETE FROM _ermrest.model_column_annotation where schema_name=\'%s\';\n\n' % options.schema)

out.write('BEGIN;\n\n')
out.write('DROP SCHEMA %s CASCADE;\n\n' % schema)
out.write('CREATE SCHEMA %s;\n\n' % schema)

for table in tablesSortedNames:
    create_sql_table(table)
    
for table in tablesSortedNames:
    insert_sql_data(table)
    
out.write('COMMIT;\n\n')
out.close()
out_annotation.write('COMMIT;\n\n')
out_annotation.close()
sys.exit(0)


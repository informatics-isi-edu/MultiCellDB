#!/usr/bin/python

"""
This script generates the CSV files from the JSON files resulted as a conversion from XML files.
Each CSV file corresponds to a SQL table name.

Parameters:

    - input: the directory with the JSON files
    - output: the directory with the CSV files
    
"""

import sys
import os
from optparse import OptionParser
import json

parser = OptionParser()
parser.header = {}
parser.add_option('-i', '--input', action='store', dest='input', type='string', help='Input directory')
parser.add_option('-o', '--output', action='store', dest='output', type='string', help='Output directory')

(options, args) = parser.parse_args()

if not options.input:
    print 'ERROR: Missing input directory'
    sys.exit(1)
    
if not options.output:
    print 'ERROR: Missing output directory'
    sys.exit(1)

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
Check if all the values of an array are strings
"""
def isStringArray(data):
    if isinstance(data,list):
        for value in data:
            if not isinstance(value, basestring):
                return False
        return True
    return False

"""
Populate the data structures for the SQL tables
"""
def process_file(f, data, table_name):
    if isinstance(data,dict):
        for key,value in data.items():
            if value!=None:
                if isinstance(value, basestring):
                    setType(table_name, key, getType(value))
                elif isStringArray(value):
                    setType(table_name, key, 'text')
            if value==None or isinstance(value, basestring) or isStringArray(value):
                if table_name==None:
                    print 'Unexpected table name: %s' % table_name
                    sys.exit(1)
                if key not in tablesDefinitions[table_name]:
                    tablesDefinitions[table_name].append(key)
            elif isinstance(value,dict) or isinstance(value,list):
                if key not in tablesNames:
                    tablesNames.append(key)
                if key not in tablesDefinitions.keys():
                    tablesDefinitions[key] = []
                if table_name!=None and key not in tablesReferences.keys():
                    tablesReferences[key] = []
                if table_name!=None and table_name not in tablesReferences[key]:
                     tablesReferences[key].append(table_name)
                process_file(f, value, key)
    elif isinstance(data,list):
        for value in data:
            process_file(f, value, table_name)
    elif not isinstance(data, basestring):
        print 'Unknown type: %s' % data
        sys.exit(1)
    
def csvValue(value):
    if isinstance(value,basestring):
        value = value.replace('"','""')
        value = '"%s"' % value.encode('utf8')
    else:
        value = str(value)
    return value
        
"""
Insert the data for a table
"""
def insert_csv_data(table):
    if table in tablesData.keys():
        out = open('%s/%s.csv' % (options.output, table), 'w')
        colsDefs = ['id']
        colsDefs.extend(tablesDefinitions[table])
        if table in tablesReferences.keys():
            colsRef = []
            for col in tablesReferences[table]:
                colsRef.append('%s_id' % col)
            colsDefs.extend(colsRef)
        out.write('%s\n' % ','.join(colsDefs))
        for data in tablesData[table]['data']:
            row = []
            for col in colsDefs:
                if col in data.keys():
                    row.append(csvValue(data[col]))
                else:
                    row.append('')
            out.write('%s\n' % ','.join(row))
        out.close()


"""
Populate the data structures for the SQL data
"""
def load_file_data(f, data, table_name, parent_table):
    if table_name!=None and table_name not in tablesData.keys():
        tablesData[table_name] = {}
        tablesData[table_name]['id'] = 0
        tablesData[table_name]['data'] = []
    obj = {}
    if table_name!=None:
        tablesData[table_name]['id'] = tablesData[table_name]['id']+1
        obj['id'] = tablesData[table_name]['id']
    if isinstance(data,dict):
        for key,value in data.items():
            if value!=None:
                if isinstance(value, basestring):
                    obj[key] = value
                elif isStringArray(value):
                    obj[key] = ','.join(value)
                elif isinstance(value, dict):
                    load_file_data(f, value, key, table_name)
                elif isinstance(value, list):
                    for val in value:
                        load_file_data(f, val, key, table_name)
        if table_name!=None and parent_table!=None and table_name in tablesReferences.keys() and parent_table in tablesReferences[table_name]:
            col = '%s_id' % parent_table
            obj[col] = tablesData[parent_table]['id']
        if table_name!=None:
            tablesData[table_name]['data'].append(obj)
    elif isinstance(data,list):
        sys.stderr.write('array data\n')
        for value in data:
            load_file_data(f, value, table_name, parent_table)
    
files=os.listdir(options.input)     

for f in files:  
    input = open('%s/%s' % (options.input, f))
    json_data = json.load(input)
    process_file(f, json_data, None) 
    input.close()
    
for f in files:  
    input = open('%s/%s' % (options.input, f))
    json_data = json.load(input)
    load_file_data(f, json_data, None, None) 
    input.close()
    
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

for table in tablesSortedNames:
    insert_csv_data(table)
    
sys.exit(0)


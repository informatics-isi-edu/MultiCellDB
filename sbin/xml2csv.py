#!/usr/bin/python

"""
This script generates the CSV files from the XML files.
Each CSV file corresponds to a SQL table name.

Parameters:

    - input: the directory with the XML files
    - output: the directory with the CSV files
    
"""

import sys
import os
from optparse import OptionParser
import json
import xml.etree.ElementTree as ET

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

for table in tablesSortedNames:
    insert_csv_data(table)
    
sys.exit(0)


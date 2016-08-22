#!/usr/bin/python

"""
This script adds a thumbnail attribute to an XML file,
if an image file with the same name as the XML file exists.

Parameters:

    - input: the directory with the XML files
    - thumbnail: the directory with the image files
    - url: the prefix to be added to the thumbnail file
    - tag: the node tag where the attribute will be inserted
    - attribute: the name of the thumbnail attribute
    - output: the directory where the resulted XML files will be placed
    
"""

import sys
import os
from optparse import OptionParser
import xml.etree.ElementTree as ET
import json

parser = OptionParser()
parser.header = {}
parser.add_option('-c', '--config', action='store', dest='config', type='string', help='Configuration file')

(options, args) = parser.parse_args()

if not options.config:
    print 'ERROR: Missing configuration file'
    sys.exit(1)
    
"""
parser.add_option('-i', '--input', action='store', dest='input', type='string', help='Input directory')
parser.add_option('-f', '--file', action='store', dest='file', type='string', help='Root file')
parser.add_option('-o', '--output', action='store', dest='output', type='string', help='Output file')

if not options.input:
    print 'ERROR: Missing input directory'
    sys.exit()
    
if not options.output:
    print 'ERROR: Missing output directory'
    sys.exit()
    
if not options.file:
    print 'ERROR: Missing root file'
    sys.exit()
"""
    
cfg = None

if os.path.exists(options.config):
    f = open(options.config, 'r')
    try:
        cfg = json.load(f)
    except ValueError as e:
        sys.stderr.write('Malformed configuration file: %s\n' % e)
        sys.exit(1)
    else:
        f.close()
else:
    sys.stderr.write('Configuration file: "%s" does not exist.\n' % options.config)
    sys.exit(1)
    
xsd_dir = cfg.get('input', None)
if not xsd_dir:
    sys.stderr.write('Schemas directory name must be given.\n')
    sys.exit(1)
xsd_json = cfg.get('json', None)
if not xsd_json:
    sys.stderr.write('output JSON file must be given.\n')
    sys.exit(1)
schema = cfg.get('schema', None)
if not schema:
    sys.stderr.write('Ermrest schema must be given.\n')
    sys.exit(1)
xsd_root = cfg.get('root', None)
if not xsd_root:
    sys.stderr.write('Schema root file must be given.\n')
    sys.exit(1)
tablesDefinitions_file = cfg.get('tablesDefinitions', None)
if not tablesDefinitions_file:
    sys.stderr.write('tablesDefinitions file must be given.\n')
    sys.exit(1)
tables_file = cfg.get('tables', None)
if not tables_file:
    sys.stderr.write('tables file must be given.\n')
    sys.exit(1)
sql_file = cfg.get('sql', None)
if not sql_file:
    sys.stderr.write('SQL file must be given.\n')
    sys.exit(1)
annotation_file = cfg.get('annotation', None)
if not annotation_file:
    sys.stderr.write('Annotation file must be given.\n')
    sys.exit(1)
top_table = cfg.get('topTable', None)
if not top_table:
    sys.stderr.write('Top table must be given.\n')
    sys.exit(1)
thumbnail = cfg.get('thumbnail', None)
title = cfg.get('title', None)

result = []

xsd_files = []
xsd_processed_files = []

tables = []
tablesDefinitions = {}
tablesSortedNames = []

baseTypes = {
    'boolean': 'boolean', 
    'dateTime': 'timestamptz', 
    'double': 'float8', 
    'string': 'text', 
    'unsignedInt': 'int4', 
    'unsignedLong': 'int8', 
    'unsignedShort': 'int2'
}

"""
Process recursively the XML elements,
adding the thumbnail attribute
"""
def process_element(elem, parent, res):
    if len(elem.attrib.keys()) > 0 or len(elem) > 0:
        tag = elem.tag
        if '}' in tag:
            tag = tag.split('}',1)[1]
        item = {tag: {}}
        res.append(item)
        item = item[tag]
        if len(elem.attrib.keys()) > 0:
            item['attributes'] = {}
        if len(elem) > 0:
            item['children'] = []
        for attr,value in elem.attrib.items(): 
            attrib = '@%s' % attr
            item['attributes'][attrib] = value
            if tag=='import' and attr=='schemaLocation' and value not in xsd_files:
                xsd_files.append(value)
        for child in elem:
            process_element(child, elem, item['children'])
        
"""
Process each XML file
"""
def process_file(f):
    tree = ET.parse('%s' % f)
    root = tree.getroot()
    process_element(root, None, result)
    
def getTables():
    for schema in result:
        for child in schema['schema']['children']:
            for tag,value in child.items():
                if tag!='import':
                    tables.append(value['attributes']['@name'])
                    tablesDefinitions[value['attributes']['@name']] = {}
    tables.sort()
    
def getTableAttributes(elem, root):
    if root==thumbnail:
        tablesDefinitions[root]['thumbnail'] = 'string'
    for tag,value in elem.items():
        if tag=='attributes':
            if '@name' in value.keys() and '@type' in value.keys():
                type = value['@type'].split(':')[-1]
                if type in tables:
                    tablesDefinitions[type]['%s_id' % root] = root
                else:
                    tablesDefinitions[root][value['@name']] = type
            else:
                type = None
                if '@ref' in value.keys():
                    type = value['@ref'].split(':')[-1]
                elif '@base' in value.keys():
                    type = value['@base'].split(':')[-1]
                if type!=None and type in tables:
                    tablesDefinitions[type]['%s_id' % root] = root
        elif tag=='children':
            for child in value:
                getTableAttributes(child, root)
        else:
            getTableAttributes(value, root)
    
def getFields():
    for schema in result:
        for child in schema['schema']['children']:
            for tag,value in child.items():
                if tag!='import':
                    getTableAttributes(child, value['attributes']['@name'])
        
"""
Generate the SQL statements for a table as well as its annotations
"""
def create_sql_table(table, out, out_annotation):
    if table!=top_table:
        out_annotation.write('INSERT INTO _ermrest.model_table_annotation VALUES(\'%s\', \'%s\', \'comment\', \'["exclude", "nested"]\');\n' % (schema, table))
    out.write('CREATE TABLE %s."%s"\n' % (schema,table))
    out.write('(\n')
    out.write('\t"id" int4 PRIMARY KEY')
    out_annotation.write('INSERT INTO _ermrest.model_column_annotation VALUES(\'%s\', \'%s\', \'id\', \'comment\', \'["hidden"]\');\n' % (schema, table))
    tablesReferences = []
    for col,value in tablesDefinitions[table].iteritems():
        if thumbnail!=None and col==thumbnail:
            out_annotation.write('INSERT INTO _ermrest.model_column_annotation VALUES(\'%s\', \'%s\', \'%s\', \'comment\', \'["thumbnail"]\');\n' % (schema, table, col))
        if title!=None and col==title:
            out_annotation.write('INSERT INTO _ermrest.model_column_annotation VALUES(\'%s\', \'%s\', \'%s\', \'comment\', \'["title"]\');\n' % (schema, table, col))
        if value in tables:
            tablesReferences.append(value)
            out.write(',\n\t"%s" int4' % (col))
            out_annotation.write('INSERT INTO _ermrest.model_column_annotation VALUES(\'%s\', \'%s\', \'%s_id\', \'comment\', \'["hidden"]\');\n' % (schema, table, col))
        else:
            if value not in baseTypes.keys():
                sys.stderr.write('Unknown base type: %s.\n' % value)
                sys.exit(1)
            out.write(',\n\t"%s" %s' % (col, baseTypes[value]))
    for col in tablesReferences:
        out.write(',\n\tFOREIGN KEY ("%s_id") REFERENCES %s."%s" (id)' % (col,schema,col))
    out.write('\n);\n\n')


def sortTablesDefinitions():
    sorted = False
    while not sorted:
        sorted = True
        for table in tables:
            if table not in tablesSortedNames:
                resolved = True
                for col,value in tablesDefinitions[table].iteritems():
                    if value in tables and value not in tablesSortedNames and value!=table:
                        resolved = False
                        break
                if resolved:
                    tablesSortedNames.append(table)
                else:
                    sorted = False
                        
xsd_files.append(xsd_root)
ready=False
while ready==False:
    ready=True
    for f in xsd_files:
        if f not in xsd_processed_files:
            process_file('%s/%s' % (xsd_dir, f)) 
            xsd_processed_files.append(f)
            ready=False

out = open('%s' % (xsd_json), 'w')
out.write('%s\n' % json.dumps(result, indent=4))
out.close()

getTables()
out = open('%s' % (tables_file), 'w')
out.write('%s\n' % json.dumps(tables, indent=4))
out.write('%d\n' % len(tables))
out.close()

#print json.dumps(tables, indent=4)
#print len(tables)

getFields()
out = open('%s' % (tablesDefinitions_file), 'w')
out.write('%s\n' % json.dumps(tablesDefinitions, indent=4))
out.close()

out_annotation = open('%s' % (annotation_file), 'w')
out_annotation.write('BEGIN;\n\n')
out_annotation.write('DELETE FROM _ermrest.model_table_annotation where schema_name=\'%s\';\n' % schema)
out_annotation.write('DELETE FROM _ermrest.model_column_annotation where schema_name=\'%s\';\n\n' % schema)

out = open('%s' % (sql_file), 'w')
out.write('BEGIN;\n\n')
out.write('DROP SCHEMA IF EXISTS %s CASCADE;\n\n' % schema)
out.write('CREATE SCHEMA %s;\n\n' % schema)

sortTablesDefinitions()

for table in tablesSortedNames:
    create_sql_table(table, out, out_annotation)
out.write('COMMIT;\n\n')
out.close()
out_annotation.write('COMMIT;\n\n')
out_annotation.close()

#baseTypes.sort()
#print baseTypes


"""
out = open('%s' % (sql_file), 'w')
for table in tables:
    create_sql_table(table, out)
out.close()
"""    

#print json.dumps(tablesDefinitions, indent=4)
#print json.dumps(tablesReferences, indent=4)
sys.exit(0)
    


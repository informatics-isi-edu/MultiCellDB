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

parser = OptionParser()
parser.header = {}
parser.add_option('-i', '--input', action='store', dest='input', type='string', help='Input directory')
parser.add_option('-o', '--output', action='store', dest='output', type='string', help='Output directory')
parser.add_option('-u', '--url', action='store', dest='url', type='string', help='URL root for thumbnails')
parser.add_option('-p', '--thumbnail', action='store', dest='thumbnail', type='string', help='Directory for thumbnails')
parser.add_option('-t', '--tag', action='store', dest='tag', type='string', help='XML tag for thumbnails')
parser.add_option('-a', '--attribute', action='store', dest='attribute', type='string', help='XML attribute for thumbnails')

(options, args) = parser.parse_args()

if not options.input:
    print 'ERROR: Missing input directory'
    sys.exit()
    
if not options.output:
    print 'ERROR: Missing output directory'
    sys.exit()
    
thumbnails = {}
thumbnail_url = ''
thumbnail_tag = ''
thumbnail_attribute = ''

if options.url:
    thumbnail_url = options.url

if options.tag:
    thumbnail_tag = options.tag

if options.attribute:
    thumbnail_attribute = options.attribute

"""
Get the file name w/o its extension
"""
def get_file_name(file):
    return '.'.join(file.split('.')[0:-1])

files=os.listdir(options.input)     

"""
Process recursively the XML elements,
adding the thumbnail attribute
"""
def process_element(elem, file):
    for child in elem:
        process_element(child, file)
    if elem.tag==thumbnail_tag:
        filename = get_file_name(file)
        if filename in thumbnails.keys():
            elem.set(thumbnail_attribute, '%s/%s' % (thumbnail_url, thumbnails[filename]))
        else:
            elem.set(thumbnail_attribute, '%s/%s' % (thumbnail_url, 'blank.png'))
    
"""
Process each XML file
"""
def process_file(f):
    tree = ET.parse('%s/%s' % (options.input, f))
    root = tree.getroot()
    process_element(root, f)
    tree.write('%s/%s' % (options.output, f), encoding='UTF-8')
    
if options.thumbnail:
    for f in os.listdir(options.thumbnail):
        thumbnails[get_file_name(f)] = f

for f in files:  
    process_file(f) 

sys.exit(0)
    


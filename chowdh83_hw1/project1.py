"""
Name: SHOIMYA CHOWDHURY
Netid: chowdh83
PID; 159958342
External Resources Used:
1 https://www.pythontutorial.net/python-basics/python-write-csv-file/
2 https://www.geeksforgeeks.org/json-dump-in-python/
3 https://www.studytonight.com/python-howtos/how-to-read-xml-file-in-python
4 https://www.datacamp.com/tutorial/python-xml-elementtree
5 https://www.geeksforgeeks.org/create-xml-documents-using-python/
6 https://www.tutorialspoint.com/How-to-convert-Python-Dictionary-to-a-list
...
"""
import csv
import json
import xml.etree.ElementTree as ET
from collections import OrderedDict

class File(object):
    def __init__(self) -> None:
        self.container = []
        self.headers = []
        self.list_of_dict = []

def read_csv_file(filename):
    """
    Takes a filename denoting a CSV formatted file.
    Returns an object containing the data from the file.
    The specific representation of the object is up to you.
    The data object will be passed to the write_*_files functions.
    """
    new_file = File() #the file object to store the data

    file=open( filename , "r")
    count = 0
    reader = csv.reader(file)

    for line in reader:
        if count == 0:
            new_file.headers = line
            count += 1
        else:
            new_file.container.append(line)
    file.close()
    
    l_o_d = []
    for i in range(len(new_file.container)):
        d = {}
        
        for j in range(len(new_file.container[i])): #0,9
            d[new_file.headers[j]] = new_file.container[i][j]
        
        l_o_d.append(d)

    new_file.list_of_dict = l_o_d #set

    return new_file

def write_csv_file(filename, data):
    """
    Takes a filename (to be writen to) and a data object 
    (created by one of the read_*_file functions). 
    Writes the data in the CSV format.
    """
    #for sorting the DS
    new_d = []
    for item in data.list_of_dict :
        sorted_item = OrderedDict(sorted(item.items()))
        new_d.append(sorted_item)
    data.list_of_dict = new_d

    head = list(data.list_of_dict[0].keys())
    with open(filename, 'w',newline='') as f:
        writer = csv.writer(f)
        writer.writerow(head)

        for item in data.list_of_dict:
            row = item.values()
            writer.writerow(row)

def read_json_file(filename):
    """
    Similar to read_csv_file, except works for JSON files.
    """
    # raise NotImplementedError("To Be Done")
    new_file = File()

    with open(filename) as f_in:
        as_dict =  json.load(f_in)
        new_file.list_of_dict = as_dict

    return new_file

def write_json_file(filename, data):
    """
    Writes JSON files. Similar to write_csv_file.
    """
    #for sorting the DS
    new_d = []
    for item in data.list_of_dict :
        sorted_item = OrderedDict(sorted(item.items()))
        new_d.append(sorted_item)
    data.list_of_dict = new_d

    out_file = open(filename, "w")
  
    json.dump(data.list_of_dict, out_file)
    
    out_file.close()

def read_xml_file(filename):
    """
    You should know the drill by now...
    """
    tree = ET.parse(filename) 
    new_file = File()

    root = tree.getroot() 
    store = [] #list of dict.

    for child in root:
        d = {}
        for item in child:
            d[item.tag] = item.text
        store.append(d)

    new_file.list_of_dict = store
    return new_file

def write_xml_file(filename, data):
    """
    Feel free to write what you want here.
    """
    #for sorting the DS
    new_d = []
    for item in data.list_of_dict :
        sorted_item = OrderedDict(sorted(item.items()))
        new_d.append(sorted_item)
    data.list_of_dict = new_d


    root = ET.Element("data")
    for item in data.list_of_dict:
        rec = ET.Element("record")
        root.append(rec)

        for k in item:
            temp = ET.SubElement(rec, k)
            temp.text = item[k]

    tree = ET.ElementTree(root)
    with open (filename, "wb") as files:
        tree.write(files)
    

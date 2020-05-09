import sys 
import re
import os
from xml.dom import minidom
import xml.etree.ElementTree as ET

#CREATES A NEW NODE WITHIN THE XML TREE STRUCTURE
def createNode(parent, newNode, attrib={}):
	if parent is None:
		return ET.Element(newNode, attrib=attrib)
	else:
		newNode = ET.Element(newNode, attrib=attrib)
		parent.append(newNode)
		return newNode		

#PUPULATES THE NEW XML TREE STRUCTURE
def populateXML(outRoot, postings):
	for post in postings:
		newNode = createNode(outRoot, 'postings')
		name = createNode(newNode, 'name')
		name.text = post
		for inum in postings[post]:
			idNode = createNode(newNode, 'inumber')
			idNode.text = inum
	return outRoot

#CONTRUCTS THE INVERTED INDEX
def createInvertedIndex(inRoot, outRoot):
	postings = dict()
	n = 0
	for node in inRoot.find('INodeSection'):
		if node.tag == "inode" and node.find('name').text is not None:	
			name = node.find('name').text
			name = name.split('.')[0]
			name = re.split('[- ]', name)
			inum = node.find('id').text
			for n in name:
				if n in postings:
					n = n.lower()
					postings[n].append(inum)
				else:
					postings[n] = [inum]
	outRoot = populateXML(outRoot, postings)
	return outRoot

def getRootNode(filePath):
	inTree = ET.parse(filePath)
	return inTree.getroot()

def formatPretty(root):
	rawString = ET.tostring(root, 'utf-8')
	parsedString = minidom.parseString(rawString)
	return parsedString.toprettyxml(indent="\t")

def exportXML(filePath, root):
	xml = formatPretty(root)
	Ofile = open(filePath, "w")
	Ofile.write(xml)

if __name__ == "__main__":
	inputFile = sys.argv[1]
	outFile = sys.argv[2]

	inRootNode = getRootNode(inputFile)
	outRootNode = createNode(None, "index")
	outRootNode = createInvertedIndex(inRootNode, outRootNode)

	exportXML(outFile, outRootNode)
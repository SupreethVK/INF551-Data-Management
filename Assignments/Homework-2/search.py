import sys 
import re
import json
import xml.etree.ElementTree as ET

def getRootNode(filePath):
	inTree = ET.parse(filePath)
	return inTree.getroot()


#RECURSIVELY CONSTRUCTS THE PATH OF INUMBER
def getPath(inode, dirStruct, info):
	result = [inode]
	path = ""
	curr = inode
	while curr in dirStruct:
		curr = dirStruct[curr]
		result.insert(0, curr)
	for inum in result:
		if inum in info:
			path+="/"+info[inum][0]
	return path

#RETRIEVES THE PATH AND METADATA QUERIED
def retrieveQuery(keyword, index, dirStruct, info):
	inodes = index[keyword]
	result = {}
	for inode in inodes:
		path = getPath(inode, dirStruct, info)
		meta = {"id":int(inode), "type":info[inode][1], "mtime":int(info[inode][2]), "blocks":info[inode][3]}
		result[path] = meta
	return result
		
#TO STORE THE INDEX IN A DICTIONARY DATASTRUCTURE
def parseIndex(indexXML):
	indexRoot = getRootNode(indexXML)
	result = {}
	for node in indexRoot.findall('postings'):
		name = node.find('name').text
		inum = []
		for num in node.findall('inumber'):
			inum.append(num.text)
		result[name] = inum
	return result


#TO STORE THE FILE-DIRECTORY STRUCTURE IN A DICTIONARY DATASTRUCTURE
def buildDirStructure(filePath):
	root = getRootNode(filePath)
	result = {}
	for node in root.find('INodeDirectorySection'):
		parent = node.find('parent')
		children = node.findall('child')
		for child in children:
			result[child.text] = parent.text
	return result


#TO STORE THE METADATA IN A DICTIONARY DATASTRUCTURE
def extractDetails(filePath):
	root = getRootNode(filePath)
	result = {}
	for node in root.find('INodeSection'):
		if node.tag == "inode" and node.find('name').text is not None:	
			inode = node.find('id').text
			name = node.find('name').text
			ftype = node.find('type').text
			mtime = node.find('mtime').text
			blockIDs = []
			blocks = node.find('blocks')
			if blocks is not None:
				for b in blocks.findall('block'):
					blockIDs.append(int(b.find('id').text))
			result[inode] = [name, ftype, mtime, blockIDs]
	return result

if __name__ == "__main__":
	inputXML = sys.argv[1]
	indexXML = sys.argv[2]

	index = parseIndex(indexXML)
	directoryStructure = buildDirStructure(inputXML)
	infoindex = extractDetails(inputXML)

	keywords = sys.argv[3].split(" ")
	result = dict()
	for keyword in keywords:
		keyword = keyword.lower()
		temp = retrieveQuery(keyword, index, directoryStructure, infoindex)
		for k, v in temp.items():
			if k not in result:
				result[k] = v

	for k in result:
		print(k)
		print(json.dumps(result[k]))
		





#!/usr/bin/env python
# encoding=utf-8

import sys
reload(sys)
sys.setdefaultencoding( "utf-8" )

def parseRow(rowdefine,data,topic,date,delimiter='\t'):
    numClumn = len(rowdefine)
    data = '{0}{1}{2}{1}{3}' .format(topic,'\t',date, data)
    column = data.split('\t')
    oneRow = {}
    if len(column) >= numClumn:
        for i in range(0,numClumn):
            rowStr = column[i]
            oneRow[rowdefine[i][0]]= castValue(rowStr,rowdefine[i][1])
        return oneRow
    

def castValue(value,valueType):
    if valueType == 'int':
	if value=='' or value=='\N':
	    return 0
	return int(value)
    elif valueType == 'string':
        if value=='' or value=='\N':
            return 'NULL'
        return value
    elif valueType == 'long':
        if value=='' or value=='\N':
            return 0
        return long(value)
    elif valueType == 'float':
	if value=='' or value=='\N':
	    return 0.0
	return float(value)
    else:
	return NotImplemented



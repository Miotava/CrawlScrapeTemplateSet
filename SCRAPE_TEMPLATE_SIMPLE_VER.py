# coding: utf-8
#!/usr/bin/python

import time
import re
import urllib2
import cookielib
import pymysql
from bs4 import BeautifulSoup
import urllib
import subprocess
import sys
import shutil
import json
import os
import datetime
import copy
import glob

#######################################################
### optional: if it's needed to print dict object,  ###
### <easy_install prettyprint> and                  ###
### uncomment below and use <pp(dict)>              ###
#######################################################
# from prettyprint import pp


# sysモジュールをリロードする
reload(sys)
# デフォルトの文字コードを変更する．
sys.setdefaultencoding('utf-8')


############################## DB open ##############################
try:
    conn = pymysql.connect(host='yourhostname', user='username', passwd='password', charset='utf8')
    cur = conn.cursor(pymysql.cursors.DictCursor)
except:
    print "Unexpected DB connect Error:" + sys.exc_info()
cur.execute("USE YOURDBNAME")

### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ###
def printListFunc(printTitle, list):
    print "\n$$$$$$$$ " + printTitle + " param info $$$$$$$$"
    for v in list:
        for i in range(0, len(v)):
            if v[i] is not None:
                if isinstance(v[i],datetime.date):
                    print v[i]
                else:
                    print v[i].encode('utf-8').decode('utf-8')

    print "$$$$$$$$ " + printTitle + " param info end $$$$\n"

### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ###
def getHtmlFromPath(htmlFilePath):
    f = open(htmlFilePath)
    html = f.read()
    f.close()
    return html

### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ###
def collectTargetHtmlPaths(tgtPath):
    resultPaths = []
    res = os.listdir(tgtPath)
    for line in res:
        if (line != "") and (".html" in line):
            resultPaths.append(line)
    return resultPaths

### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ###
def collectTargetFolderPaths(tgtPath):
    resultPaths = []
    res = os.listdir(tgtPath)
    for line in res:
        if (line != "") and (line != "scraped") and ("." not in line):
            resultPaths.append(line)
    return resultPaths

### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ###
def makeFileNameFromPath(tgtFileName,delimiterChar,paramNum):
    resultFileName = tgtFileName.split(delimiterChar)[0]
    for i in range(1,paramNum+1):
        resultFileName = resultFileName + delimiterChar + tgtFileName.split(delimiterChar)[i]
    return resultFileName

### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ###
def removeNonHtmlFolder(tgtPath):
    resultPaths = []
    res = os.listdir(tgtPath)
    for line in res:
        if line != "":
            resultPaths.append(line)
    if len(resultPaths) == 0:
        shutil.rmtree(tgtPath)
        print "removed: " + tgtPath
    else:
        print "COULDN'T REMOVE FOLDER: " + tgtPath

### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ###
def scrapeHtml(htmlFilePath):

    html = getHtmlFromPath(htmlFilePath)
    bsObj = BeautifulSoup(html, "html.parser")

    resultDict = {}
    itemCounter = 1

    #######################################
    ####   GET INFO FROM bsObj BELOW   ####
    #######################################

    itemsObj = bsObj.findAll("tag")
    for itemObj in itemsObj:
        item = itemObj.find("tag"})

        value1 = item.<SOME PROCESS WITH FIND>
        value2 = item.<SOME PROCESS WITH FIND>
        value3 = item.<SOME PROCESS WITH FIND>

        resultDict.update({"ITEM" + str(itemCounter): {
            "key1"  :   value1,
            "key2"  :   value2,
            "key3"  :   value3,
        }})
        itemCounter += 1

    #######################################
    ####   GET INFO FROM bsObj ABOVE   ####
    #######################################

    ###### FOR DEBUGGING (UNCOMMENT THEM IF IT'S NEEDED) ######
    # printList = []
    # printList.append((param1,param2,param3))
    # printListFunc("PRINT TITLE", printList)

    print "SCRAPED HTML: " + htmlFilePath

    return resultDict

### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ###
def analyzeInsertDataToDB(tgtDict):
    while tgtDict:
        popedItem = tgtDict.popitem()
        popedItemVal = popedItem[1]
        # pp(popedItemValue)

        ##############################################################
        ####   DO SOME ANALYSIS AND INSERT DATA FROM DICT TO DB   ####
        ##############################################################


        cur.execute("INSERT IGNORE INTO tableName (field1,field2,field3) VALUES (%s, %s, %s)", (popedItemVal["key1"],popedItemVal["key2"],popedItemVal["key3"]))
        conn.commit()

############################################################################################
######                                 main function                                  ######
############################################################################################

origPath = "path/to/crawledHtmlFileFolder" # os.path.joinなどを使ってOK
if not os.path.exists(os.path.join(origPath,'scraped')):
    os.mkdir(origPath + 'scraped/')

targetFolderPaths = collectTargetFolderPaths(origPath)

for folderPath in targetFolderPaths:

    scrapedFolderPath = os.path.join(origPath,'scraped',folderPath)
    print "scrapedFolderPath: " + scrapedFolderPath

    if not os.path.exists(scrapedFolderPath):
        os.mkdir(scrapedFolderPath)

    htmlFilePaths = collectTargetHtmlPaths(os.path.join(origPath,folderPath))

    while htmlFilePaths:

        htmlFilePath = htmlFilePaths.pop()
        print "htmlFilePath-ORIGINAL: " + htmlFilePath

        htmlFilePath = os.path.join(origPath,folderPath,htmlFilePath)
        print "htmlFilePath-CONNECTED: " + htmlFilePath

        itemDict = {}

        itemDict = scrapeHtml(htmlFilePath)
        shutil.move(htmlFilePath,scrapedFolderPath)
        # pp(itemDict)

        print "LEN DICT: " + str(len(itemDict))
        analyzeInsertDataToDB(itemDict)
        print "### INSERTED itemDict TO DB ###"

    removeNonHtmlFolder(os.path.join(origPath,folderPath))

############################## DB close ##############################
cur.close()
conn.close()


############################################################################################
######                                 main function end                              ######
############################################################################################

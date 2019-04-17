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
def collectSimilarHtmlFilePaths(tgtFilePathStr):
    resultPaths = []
    res = glob.glob(tgtFilePathStr)
    for line in res:
        if line != "":
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
def getTargetFilePathStr(tgtPath):
    delimiterChar = "_"
    tgtFileName = tgtPath.split(os.sep)[-1]
    originPath = tgtPath.replace(tgtFileName,"")
    resultPathStr = originPath + makeFileNameFromPath(tgtFileName.split(".")[0],delimiterChar,3) + delimiterChar + "*.html"
    return resultPathStr

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
def getPageNUm(tgtFilePath):
    pageNum = tgtFilePath.split(os.sep)[-1].split(".")[0].split("_")[-1]
    return int(pageNum)

### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ###
def getDigitStrFromStr(tgtStr):
    pattern = r'([0-9]+)'
    resultDigitList = re.findall(pattern,tgtStr)
    resultDigitStr = ""
    for i in range(0,len(resultDigitList)):
        resultDigitStr += resultDigitList[i]

    return resultDigitStr

### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ###
def scrapeHtml(htmlFilePath, itemCounter):

    html = getHtmlFromPath(htmlFilePath)
    bsObj = BeautifulSoup(html, "html.parser")

    resultList = []
    #######################################
    ####   GET INFO FROM bsObj BELOW   ####
    #######################################


    resultListObj = bsObj.findAll("tag")
    print "resultListObj LEN: " + str(len(resultListObj))

    for resObj in resultListObj:

        param1 = resObj.find("tag")
        param2 = resObj.find("tag")
        param3 = resObj.find("tag")

        resultList.append({
            "key1"  :   param1,
            "key2"  :   param2,
            "key3"  :   param3
        })
        itemCounter += 1


    #######################################
    ####   GET INFO FROM bsObj ABOVE   ####
    #######################################

    ###### FOR DEBUGGING (UNCOMMENT THEM IF IT'S NEEDED) ######
    # printList = []
    # printList.append((param1,param2,param3))
    # printListFunc("PRINT TITLE", printList)


    print "SCRAPED HTML: " + htmlFilePath

    return resultList

### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ###
def analyzeInsertDataToDB(tgtList):
    ######################## tips ########################
    ### To convert tgtDict to dict,
    ### 1. use atom
    ### 2. replace ":\s*" with ":\tpopedItem[\""
    ### 3. replace "," with ""], "
    ### 4. delete "," in the last row
    #####################################################
    ### To convert dict to values row,
    ### 1. use atom
    ### 2. replace """ with ""
    ### 3. replace ":\s*popedItem\[" with "= VALUES("
    ### 4. replace "\]" with ")"
    ### 5. delete rows including primary key


    ##############################################################
    ####   DO SOME ANALYSIS AND INSERT DATA FROM DICT TO DB   ####
    ##############################################################

    itemList = []
    while tgtList:
        popedItem = tgtList.pop()

        # print "\n################# POPED ITEM #################"
        # pp(popedItem)

        itemList.append({
            "key1"    :	popedItem["key1"],
            "key2"   :	popedItem["key2"],
            "key3"  :	popedItem["key3"]
        })
        # pp(itemList)

    cur.executemany("""
    INSERT INTO
    tableName (field1,field2,field3)
    VALUES (%(key1)s,%(key2)s,%(key3)s)
    ON DUPLICATE KEY UPDATE
    field2 = VALUES(field2),
    field3 = VALUES(field3)""",
    itemList
    )
    conn.commit()

############################################################################################
######                                 main function                                  ######
############################################################################################

origPath = "path/to/crawledHtmlFileFolder" # os.path.joinなどを使ってOK

if not os.path.exists(os.path.join(origPath,'scraped')):
    os.mkdir(os.path.join(origPath,'scraped'))

targetFolderPaths = collectTargetFolderPaths(origPath)
ITEMNUM_PER_PAGE = 120

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

        tgtFilePathStr = getTargetFilePathStr(htmlFilePath)
        print "tgtFilePathStr: " + tgtFilePathStr

        similarHtmlFilePaths = collectSimilarHtmlFilePaths(tgtFilePathStr)
        similarHtmlFilePathsForMove = copy.deepcopy(similarHtmlFilePaths)

        itemList = []
        itemListCounter = 1

        while similarHtmlFilePaths:
            tempHtmlFilePath = similarHtmlFilePaths.pop()
            print "tempHtmlFilePath: " + tempHtmlFilePath

            itemListCounter = ITEMNUM_PER_PAGE * (getPageNUm(tempHtmlFilePath) - 1) + 1
            tempItemList = scrapeHtml(tempHtmlFilePath, itemListCounter)
            itemList.extend(tempItemList)

            if htmlFilePath != tempHtmlFilePath:
                htmlFilePaths.remove(tempHtmlFilePath.split(os.sep)[5])
        # pp(itemList)

        print "LEN DICT: " + str(len(itemList))
        analyzeInsertDataToDB(itemList)
        print "### INSERTED itemList TO DB ###"

        while similarHtmlFilePathsForMove:
            tempHtmlFilePath = similarHtmlFilePathsForMove.pop()
            shutil.move(tempHtmlFilePath,scrapedFolderPath)



    removeNonHtmlFolder(os.path.join(origPath,folderPath))

############################## DB close ##############################
cur.close()
conn.close()


############################################################################################
######                                 main function end                              ######
############################################################################################

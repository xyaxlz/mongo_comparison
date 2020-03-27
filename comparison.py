#!/usr/bin/env python
# -*- coding:utf-8 -*-  

import pymongo
import time
import random
import sys
import getopt
import os, time, random
from multiprocessing import Pool
import os
import json
import threading
from bson.objectid import ObjectId

# constant
EXCLUDE_DBS = "excludeDbs"
EXCLUDE_COLLS = "excludeColls"
PARALLEL = "parallel"
CHECK_NUM="checkNum"
# we don't check collections and index here because sharding's collection(`db.stats`) is splitted.
CheckList = {"objects": 1, "numExtents": 1, "ok": 1}
configure = {}

def log_info(message):
    print "INFO  [%s] %s " % (time.strftime('%Y-%m-%d %H:%M:%S'), message)

def log_error(message):
    print "ERROR [%s] %s " % (time.strftime('%Y-%m-%d %H:%M:%S'), message)

class MongoCluster:

    # pymongo connection
    conn = None

    # connection string
    url = ""

    def __init__(self, url):
        self.url = url

    def connect(self):
        self.conn = pymongo.MongoClient(self.url)

    def close(self):
        self.conn.close()


def filter_check(m):
    new_m = {}
    for k in CheckList:
        new_m[k] = m[k]
    return new_m

"""
    check meta data. include db.collection names and stats()
"""
def check(src, dst):

    #
    # check metadata 
    #
    totalRecordCount = 0
    srcDbNames = src.conn.database_names()
    dstDbNames = dst.conn.database_names()
    srcDbNames = [db for db in srcDbNames if db not in configure[EXCLUDE_DBS]]
    dstDbNames = [db for db in dstDbNames if db not in configure[EXCLUDE_DBS]]
    if len(srcDbNames) != len(dstDbNames):
        log_error("DIFF => database count not equals. src[%s], dst[%s]" % (srcDbNames, dstDbNames))
        return False
    else:
        log_info("EQUL => database count equals")

    # check database names and collections
    for db in srcDbNames:
        if db in configure[EXCLUDE_DBS]:
            log_info("IGNR => ignore database [%s]" % db)
            continue

        if dstDbNames.count(db) == 0:
            log_error("DIFF => database [%s] only in srcDb" % (db))
            return False

        # db.stats() comparison
        srcDb = src.conn[db] 
        dstDb = dst.conn[db] 
        srcStats = srcDb.command("dbstats") 
        dstStats = dstDb.command("dbstats")

        srcStats = filter_check(srcStats)
        dstStats = filter_check(dstStats)

        if srcStats != dstStats:
            log_error("DIFF => database [%s] stats not equals src[%s], dst[%s]" % (db, srcStats, dstStats))
            #return False
        else:
            log_info("EQUL => database [%s] stats equals" % db)

        # for collections in db
        srcColls = srcDb.collection_names()
        dstColls = dstDb.collection_names()
        srcColls = [coll for coll in srcColls if coll not in configure[EXCLUDE_COLLS]]
        dstColls = [coll for coll in dstColls if coll not in configure[EXCLUDE_COLLS]]
        if len(srcColls) != len(dstColls):
            log_error("DIFF => database [%s] collections count not equals, src[%s], dst[%s]" % (db, srcColls, dstColls))
            return False
        else:
            log_info("EQUL => database [%s] collections count equals" % (db))
        
        p = Pool(configure[PARALLEL])

        for coll in srcColls:
            if coll in configure[EXCLUDE_COLLS]:
                log_info("IGNR => db:[%s] ignore collection [%s]" % (db, coll))
                continue

            if dstColls.count(coll) == 0:
                log_error("DIFF => db:[%s] collection only in source [%s]" % (db, coll))
                return False

            srcColl = srcDb[coll]
            dstColl = dstDb[coll]
            # comparison collection records number
            totalRecordCount = totalRecordCount + srcColl.count()
            dstCollCount = dstColl.count()
            srcCollCount = srcColl.count()
            if dstCollCount != srcCollCount:
                log_error("DIFF => db:[%s] collection [%s] record count not equals src[%d] dst[%d]" % (db, coll,srcCollCount,dstCollCount))
                #return False
            else:
                log_info("EQUL => db:[%s] collection [%s] record count equals recs[%d]" % (db, coll,srcCollCount))

            # comparison collection index number
            src_index_length = len(srcColl.index_information())
            dst_index_length = len(dstColl.index_information())
            if src_index_length != dst_index_length:
                log_error("DIFF => db:[%s] collection [%s] index number not equals: src[%r], dst[%r]" % (db, coll, src_index_length, dst_index_length))
                return False
            else:
                log_info("EQUL => db:[%s] collection [%s] index number equals" % (db, coll))

            # check sample data
            p.apply_async(data_comparison_process, args=(db, coll,))
            
            #p = threading.Thread(target=data_comparison_process, args=(db, coll,)) 
            #p.start()
            #data_comparison_process(db, coll,)
        p.close()
        p.join()

    print("totalRecordCount: " + str(totalRecordCount))
    return True


"""
    check sample data. comparison every entry
"""

#def data_comparison_process(db, coll, mode):
def data_comparison_process(db, coll):

    src, dst = MongoCluster(srcUrl), MongoCluster(dstUrl)
    src.connect()
    dst.connect()
    srcDb = src.conn[db] 
    dstDb = dst.conn[db] 
    srcColl = srcDb[coll]
    dstColl = dstDb[coll]
    diffIds = []
    '''
    先查询dst count 后查询 src count,一般情况下线删除操作比较
    少，所以正常情况下src 的count 大于或者等于 dst count。

    '''
    dstRecCount = dstColl.find().count()
    srcRecCount = srcColl.find().count()
    
    #对比src 和dst count 取最大值,为分页总行数 
    if srcRecCount >= dstRecCount:
        totalRecord = srcRecCount
    else:
        totalRecord = dstRecCount
        
    pageSize=10000
    totalPageNum = (totalRecord + pageSize - 1) / pageSize
    lastId = "None"
    where = {}
    while totalPageNum >0:
        
        srcDocs = []
        dstDocs = []
        diffDocIds = []
        if lastId != "None":
            where["_id"]={'$gt':lastId}
        
        ##dst src 分片后把每行记录分表放到各自的list里。
        for doc in dstColl.find(where).sort('_id', 1).limit(pageSize):
            dstDocs.append(doc)
            #因为src 分页数量可能小于dst 的分页数量，所以这个把dst 的lastId先配置上，如果src 的lastId不为空
            #再覆盖掉。
            lastId = doc['_id']

        for doc in srcColl.find(where).sort('_id', 1).limit(pageSize):
            srcDocs.append(doc)
            #print(doc)
            #以src 的lastId 为最终值
            lastId = doc['_id']
        
        '''
        直接对比src 分片和dst 分片后的list数据是否相同，如果不相同选取两个list 
        长度最大值作为总行数，一行一行对比数据，求出相互不在对方的数据。并且把
        不匹配的数据id 放到diff
        '''
        if srcDocs != dstDocs:
            log_error("db [%s] collection [%s] ObjectId [%s] limit: %d record not equals " % (db, coll, str(lastId), pageSize))
            srcTotal = len(srcDocs)
            dstTotal = len(dstDocs)

            #查询src 的每行记录是否在dst 里
            i = 0
            while i < srcTotal:
                if srcDocs[i] not in dstDocs:
                    if srcColl.find_one(srcDocs[i]["_id"]) != dstColl.find_one(srcDocs[i]["_id"]):
                        diffDocIds.append(srcDocs[i]["_id"])
                        #return False
                i += 1
            #查询dst 每行记录是否在src 里
            j = 0
            while j < dstTotal:
                if dstDocs[j] not in srcDocs:
                    if srcColl.find_one(dstDocs[j]["_id"]) != dstColl.find_one(dstDocs[j]["_id"]):
                        diffDocIds.append(dstDocs[j]["_id"])
                        #return False
                j += 1


            checkNums = configure[CHECK_NUM]
            h = 0
            while h < checkNums:
                checkDiffDocIds = []
                for id in diffDocIds:
                    diffSrcDoc = srcColl.find_one(id)
                    diffDstDoc = dstColl.find_one(id)
                    if diffSrcDoc != diffDstDoc:
                        checkDiffDocIds.append(id)
                h +=1

            for id in checkDiffDocIds:
                diffSrcDoc = srcColl.find_one(id)
                diffDstDoc = dstColl.find_one(id)
                if diffSrcDoc != diffDstDoc:
                    log_error("db [%s] collection [%s]   DIFF => src_record[%s], dst_record[%s]" % (db, coll, diffSrcDoc, diffDstDoc))



        totalPageNum -= 1
    
    return True


def usage():
    print '|------------------------------------------------------------------------------------------------------------------------------------------------------------------------|'
    print "| Usage: ./comparison.py --src=localhost:27017/db? --dest=localhost:27018/db?  --excludeDbs=admin,local --excludeCollections=system.profile  |"
    print '|------------------------------------------------------------------------------------------------------------------------------------------------------------------------|'
    print '| Like : ./comparison.py --src="localhost:3001" --dest=localhost:3100  --excludeDbs=admin,local,mongoshake --excludeCollections=system.profile   |'
    print '|------------------------------------------------------------------------------------------------------------------------------------------------------------------------|'
    exit(0)

if __name__ == "__main__":
    opts, args = getopt.getopt(sys.argv[1:], "hs:d:n:e:x:", ["help", "src=", "dest=", "count=", "excludeDbs=", "excludeCollections=","parallel=","checkNum="])

    #configure[SAMPLE] = True
    configure[EXCLUDE_DBS] = []
    configure[EXCLUDE_COLLS] = []
    srcUrl, dstUrl = "", ""

    for key, value in opts:
        if key in ("-h", "--help"):
            usage()
        if key in ("-s", "--src"):
            srcUrl = value
        if key in ("-d", "--dest"):
            dstUrl = value
        if key in ("-e", "--excludeDbs"):
            configure[EXCLUDE_DBS] = value.split(",")
        if key in ("-x", "--excludeCollections"):
            configure[EXCLUDE_COLLS] = value.split(",")
        if key in ("--parallel"):
            if int(value) >0:
                configure[PARALLEL] = int(value)
            else:
                configure[PARALLEL] = 16
        if key in ("--checkNum"):
            if int(value) >0:
                configure[CHECK_NUM] = int(value)
            else:
                configure[CHECK_NUM] = 1
    
    if CHECK_NUM not in configure:
        configure[CHECK_NUM] = 1
                
    if PARALLEL not in configure:
        configure[PARALLEL] = 16



    # params verify
    if len(srcUrl) == 0 or len(dstUrl) == 0:
        usage()


    # ignore databases
    configure[EXCLUDE_DBS] += ["admin", "local","_mongodrc_"]
    configure[EXCLUDE_COLLS] += ["system.profile"]

    # dump configuration
    log_info("Configuration [excludeDbs=%s, excludeColls=%s]" % ( configure[EXCLUDE_DBS], configure[EXCLUDE_COLLS]))

    try :
        src, dst = MongoCluster(srcUrl), MongoCluster(dstUrl)
        print "[src = %s]" % srcUrl
        print "[dst = %s]" % dstUrl
        src.connect()
        dst.connect()
    except Exception, e:
        print e
        log_error("create mongo connection failed %s|%s" % (srcUrl, dstUrl))
        exit()
    ''' 
    db = "bonus_log"
    coll = "packet_log_202003"
    data_comparison_process(db, coll)   
    '''
   
    if check(src, dst):
        print "SUCCESS"
        exit(0)
    else:
        print "FAIL"
        exit(-1)

    src.close()
    dst.close()

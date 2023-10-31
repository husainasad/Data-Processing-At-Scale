#!/usr/bin/python2.7
#
# Assignment2 Interface
#

import psycopg2
import os
import sys
# Donot close the connection inside this file i.e. do not perform openconnection.close()
def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):
    cur = openconnection.cursor()

    result_list = []    
    cur.execute('SELECT * FROM RoundRobinRatingsMetadata')
    rr_partitions = cur.fetchone()[0]

    for pnum in range(rr_partitions):
        rr_tableName = 'RoundRobinRatingsPart{0}'.format(pnum)
        cur.execute('SELECT * FROM {0} WHERE Rating>={1} AND Rating<={2}'.format(rr_tableName, ratingMinValue, ratingMaxValue))
        rr_data_list = cur.fetchall()
        if len(rr_data_list)>0:
            for i in range(len(rr_data_list)):
                rr_data_list[i] = (rr_tableName,) + rr_data_list[i]
                result_list.append(rr_data_list[i])

    writeToFile('RangeQuery.txt', result_list)

def PointQuery(ratingsTableName, ratingValue, openconnection):
    cur = openconnection.cursor()

    result_list = []
    cur.execute('SELECT * FROM RoundRobinRatingsMetadata')
    rr_partitions = cur.fetchone()[0]
    
    for pnum in range(rr_partitions):
        rr_tableName = 'RoundRobinRatingsPart{0}'.format(pnum)
        cur.execute('SELECT * FROM {0} WHERE Rating={1}'.format(rr_tableName, ratingValue))
        rr_data_list = cur.fetchall()
        if len(rr_data_list)>0:
            for i in range(len(rr_data_list)):
                rr_data_list[i] = (rr_tableName,) + rr_data_list[i]
                result_list.append(rr_data_list[i])

    writeToFile('PointQuery.txt', result_list)

def writeToFile(filename, rows):
    f = open(filename, 'w')
    for line in rows:
        f.write(','.join(str(s) for s in line))
        f.write('\n')
    f.close()

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

    cur.execute('SELECT * FROM RangeRatingsMetadata')
    range_partitions = cur.fetchall()
    good_partitions = []
    
    if ratingMinValue==0:
        good_partitions.append(0)
    else:
        for i in range(len(range_partitions)):
            if ratingMinValue>range_partitions[i][1] and ratingMinValue<=range_partitions[i][2]:
                good_partitions.append(i)
                break

    for i in range(len(range_partitions))[::-1]:
        if ratingMaxValue>range_partitions[i][1] and ratingMaxValue<=range_partitions[i][2]:
            good_partitions.append(i)
            break

    for i in range(good_partitions[0], good_partitions[1]+1):
        range_tableName = 'RangeRatingsPart{0}'.format(i)
        cur.execute('SELECT * FROM {0} WHERE Rating>={1} AND rating<={2}'.format(range_tableName, ratingMinValue, ratingMaxValue))
        range_data_list = cur.fetchall()
        if len(range_data_list)>0:
            for i in range(len(range_data_list)):
                range_data_list[i] = (range_tableName,) + range_data_list[i]
                result_list.append(range_data_list[i])

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

    cur.execute('SELECT * FROM RangeRatingsMetadata')
    range_partitions = cur.fetchall()
    good_partition = 0

    if(ratingValue!=0):
        for rp in range_partitions:
            if(ratingValue>rp[1] and ratingValue<=rp[2]):
                good_partition=rp[0]
                break

    range_tableName = 'RangeRatingsPart{0}'.format(good_partition)
    cur.execute('SELECT * FROM {0} WHERE Rating={1}'.format(range_tableName, ratingValue))
    range_data_list = cur.fetchall()
    if len(range_data_list)>0:
        for i in range(len(range_data_list)):
            range_data_list[i] = (range_tableName,) + range_data_list[i]
            result_list.append(range_data_list[i])

    writeToFile('PointQuery.txt', result_list)

def writeToFile(filename, rows):
    f = open(filename, 'w')
    for line in rows:
        f.write(','.join(str(s) for s in line))
        f.write('\n')
    f.close()

from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader

def formatResult(row):
  
  formatted = '%s,%s,%s' % ( row[0], row[1][0], row[1][1] )  

  return formatted

def formatCensusTuple(row):

  key = '%s' % row[1]
  value = '%s' % ( row[0] + ',' + row[2] + ',' + row[3] + ',' + row[4] )

  return (key, value)

def formatComplaintsTuple(row):

  key = '%s' % row[3]
  value = '%s' % ( row[0] + ',' + row[1] + ',' + row[2] )

  return (key, value)


if __name__ == '__main__':

  sc = SparkContext()
  
  ## reading census
  census = sc.textFile(sys.argv[1], 1)
  census = census.mapPartitions(lambda x: reader(x)) 
  
  ## census formatting
  census = census.map( lambda row: formatCensusTuple(row) )

  
  ## reading complaints
  complaints = sc.textFile(sys.argv[2], 1)
  complaints = complaints.mapPartitions(lambda x: reader(x)) 

  ## complaints formatting
  complaints = complaints.map( lambda row: formatComplaintsTuple(row))

  ## joining
  result = complaints.join(census)
  ##result = result.sortBy(lambda x:  '_'.join( x[0].split('_')[0:3] + x[0].split('_')[3:4]  ), ascending=True) 
  result = result.map( lambda row: formatResult(row) )

  result.saveAsTextFile('/user/jr4964/final-project/complaint_census_join.out')
  sc.stop()
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

import re
def normalizeWords(text):
  return re.compile(r'\W+', re.UNICODE).split(text.lower())


rddlocal = sc.textFile('file:///home/cloudera/sparktutorials/frost.txt')

words = rddlocal.flatMap( normalizeWords )
wordcounts = words.map(lambda x: (x,1)).reduceByKey(lambda x,y: x+y)


wordcountsSorted = wordcounts.map(lambda (x,y):(y,x)).sortByKey()
# python 3 version:
# wordcountsSorted = wordcounts.map(lambda xy:(xy[1],xy[0])).sortByKey()


for word, count in wordcountsSorted.collect():
  print("{0} {1}".format(word,count))

from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

import re
def normalizeWords(text):
  return re.compile(r'\W+', re.UNICODE).split(text.lower())


rddlocal = sc.textFile('file:///home/cloudera/sparktutorials/frost.txt')

words = rddlocal.flatMap( normalizeWords )
wordcounts = words.map(lambda x: (x,1)).reduceByKey(lambda x,y: x+y)



for word, count in wordcounts.items():
  print("{0} {1}".format(word,count))

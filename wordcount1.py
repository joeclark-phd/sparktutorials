from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

rddlocal = sc.textFile('file:///home/cloudera/sparktutorials/frost.txt')
wordcounts = rddlocal.flatMap( lambda x: x.split() ).countByValue()

for word, count in wordcounts.items():
  print("{0} {1}".format(word,count))

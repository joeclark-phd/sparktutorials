from pyspark import SparkConf, SparkContext
import sys

conf = SparkConf().setMaster("local").setAppName("MovieSearch")
sc = SparkContext(conf = conf)

source_file = sc.textFile('file:///home/cloudera/sparktutorials/movies.txt')
titles = source_file.map(lambda line: line.split("|")[1])


if len(sys.argv) == 2:
  search_term = sys.argv[1]
else
  search_term = "gold"
  
requested_movie = sc.broadcast(search_term)
matches = titles.filter( lambda x: requested_movie.value in x.lower() )

results = matches.distinct().collect()
print '{0} Matching titles found:'.format(len(results))
for title in results:
  print title
  
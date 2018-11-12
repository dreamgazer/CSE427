import sys
from pyspark import SparkContext
from pyspark import SparkConf

if __name__ == "__main__":
	if len(sys.argv) < 2:
		print >> sys.stderr, "Usage: JPGCount.py <file>"
		exit(-1)

	sconf = SparkConf().setAppName("CountJPGS").set("spark.ui.port","4141")
        sc = SparkContext(conf=sconf)

	data = sc.textFile(sys.argv[1],1) \
		.filter(lambda line: ".jpg" in line) \
		.map(lambda line:("jpg",1)) \
		.reduceByKey(lambda v1,v2: v1+v2)


	print(data.take(1))

	sc.stop()
  


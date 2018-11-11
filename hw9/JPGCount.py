import sys
from pyspark import SparkContext
from pyspark import SparkConf

if __name__ == "__main__":
	if len(sys.argv) < 2:
		print >> sys.stderr, "Usage: JPGCount.py <file>"
		exit(-1)

	sconf = SparkConf().setAppName("CountJPGS").set("spark.ui.port","4141")
        sc = SparkContext(conf=sconf)

	data = sc.wholeTextFiles(sys.argv[1]) \
		.flatMap(lambda line:line[1].split("\n")) \
		.filter(lambda line: len(line)>0 and line.split(" ")[6].split(".")[1] == "jpg") \
		.map(lambda line:("jpg",1)) \
		.reduceByKey(lambda v1,v2: v1+v2)


	data.saveAsTextFile("out_spark_jpgc");

	sc.stop()
  


package com.learning.rdd


import org.apache.spark.sql.SparkSession


object LearnRddFirst {

	def main(args:Array[String]):Unit=
		{
				val spark=SparkSession.builder().appName("first_demo").master("local[1]").getOrCreate()
						spark.sparkContext.setLogLevel("Error")

						val rdd1=spark.sparkContext.parallelize(List(1,2,3,4,5,6,7,8))
						println("No of partitions"+rdd1.getNumPartitions)
						println("First Element"+rdd1.first())
						rdd1.collect().foreach(println)

						val rdd2=spark.sparkContext.textFile("C:/SparkCourse/Book1.txt")
						println("after reading the book")
						rdd2.foreach(f=>{
							println(f)})
							
						val rdd3=rdd2.map(f=> { f.length() })
						rdd3.foreach(f => { println(f) })
						
							
							
						val rdd4=rdd2.map(a => {a.toUpperCase()})
						rdd4.foreach(println)

						val space1=rdd2.flatMap(f=>f.split(" "))
						val transform =space1.map(f=>(f,1))				
						val count1=transform.reduceByKey((a,b)=>a+b).sortBy(-_._2)
						count1.collect.foreach(println)
						
						
						
		}

}
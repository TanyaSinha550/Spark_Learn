package com.learning.dataframe

import org.apache.spark.sql.SparkSession

object LearnDataFrameFirst {

	def main(args:Array[String]):Unit=
		{

				val spark=SparkSession.builder().appName("dataframe").master("local[1]").getOrCreate()

						val Data=Seq(("java",1000),("python",2000),("C++",3000))
						val rdd=spark.sparkContext.parallelize(Data)
						spark.sparkContext.setLogLevel("Error")
						import spark.implicits._

						val df=rdd.toDF("language","marks")

						df.printSchema()
						df.show(5,false)

						/*  =======Broadcast Variables========    */

						val states = Map(("NY","New York"),("CA","California"),("FL","Florida"))
						val countries = Map(("USA","United States of America"),("IN","India"))


						val data = Seq(("James","Smith","USA","CA"),
								("Michael","Rose","USA","NY"),
								("Robert","Williams","USA","CA"),
								("Maria","Jones","USA","FL")
								)

						val rdd1 = spark.sparkContext.parallelize(data)

						println("after rdd1")
						rdd1.foreach(println)

						val rdd2=rdd1.map(g=>{
							val jamcon=g._1.concat("Sinha")
									(jamcon,g._2,g._3,g._4)
						})
						println("after rdd2")
						rdd2.foreach(println)

						val broadcastStates = spark.sparkContext.broadcast(states)
						val broadcastCountries = spark.sparkContext.broadcast(countries)


						val rdd5 = rdd1.map(f=>{
							val country = f._3
									val state = f._4
									val fullCountry = broadcastCountries.value.get(country).get
									val fullState = broadcastStates.value.get(state).get
									(f._1,f._2,fullCountry,fullState)
						})

						rdd5.collect.foreach(println)



						val repar = rdd1.repartition(4)
						println("Repartition size : "+repar.partitions.size)


						val col = rdd1.coalesce(4)
						println("Repartition size : "+col.partitions.size)












		}

}
package com.learning.dataframe

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField

import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.Row

object LearnDataFrameSecond {

	def main(args:Array[String]):Unit={

			val spark=SparkSession.builder().appName("second").master("local[1]").getOrCreate()

					/* ===================Dataframe from rdd============*/

					val data=Seq(("java",1000),("python",2000))
					val rdd=spark.sparkContext.parallelize(data)
					val rowRDD=rdd.map(a=>Row(a._1,a._2))
					val schema=StructType(Array(StructField("language",StringType),StructField("score",IntegerType)))

					val df=spark.createDataFrame(rowRDD,schema)

					df.show(10,false)
					df.printSchema()

					/* ===================Dataframe from seq============*/

					import spark.implicits._					
					val detail=Seq(("tanya",12),("sinha",13),("aman",14))
					val df1=detail.toDF()
					df1.show(3,false)
					df1.printSchema()

				
					val columns = Seq("name","address")
					val data_split = Seq(("Robert, Smith", "1 Main st, Newark, NJ, 92537"),
							("Maria, Garcia","3456 Walnut st, Newark, NJ, 94732"))
					var dfFromData = spark.createDataFrame(data_split).toDF(columns:_*)
					dfFromData.printSchema()


					val newDF = dfFromData.map(f=>{
						val nameSplit =f.getString(0).split(",")
								val addSplit = f.getString(1).split(",")
								(nameSplit(0),nameSplit(1),addSplit(0),addSplit(1),addSplit(2),addSplit(3))
					})
					val finalDF = newDF.toDF("First Name","Last Name",
							"Address Line1","City","State","zipCode")

					finalDF.printSchema()
					finalDF.show(false)

					val name="India"
					
					val newDf=finalDF.select(concat(finalDF.col("City"),lit(" India")))
					newDf.show(5,false)
					newDf.printSchema()
					







	}
}
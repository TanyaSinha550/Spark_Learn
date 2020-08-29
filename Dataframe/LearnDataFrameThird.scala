package com.learning.dataframe

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object LearnDataFrameThird {

	def main(args:Array[String])
	{

		val spark=SparkSession.builder().appName("third").master("local[1]").getOrCreate()

				val data=Seq(("tanya",1),("sinha",2),("punam",3))

				import spark.implicits._
				val df=data.toDF("name","roll")
				
				
//				df.write.parquet("/A:/Python_Excel/par5.parquet")
				df.write.partitionBy("roll")
        .parquet("/A:/Python_Excel/par6.parquet")
				
				val dfpar=spark.read.parquet("/A:/Python_Excel/par6.parquet")
				println("reading parquet file")
				dfpar.show(5,false)
				dfpar.printSchema()

				//				========Using select ===========
				val check=1
				val df2=df.select($"name",$"roll",lit(check) as "constant1",lit("sinha") as "title",{$"roll"+1} as "increment")
				df2.show(10,false)

				//				========using withColumn ===========

				val df3=df.withColumn("Constant1",lit(1))
				df3.show(3,false)
				val df4=df.withColumn("Increment", $"roll"+1).withColumn("title",lit("sinha"))
				df4.show()

				/* ========casting the data type============*/
				
				val dfDataType=df.withColumn("roll",$"roll".cast("String"))
				dfDataType.printSchema()
				
				val dfDataType2=df.select($"roll".cast("String"))
				dfDataType2.printSchema()
				
				
				
				









	}  

}
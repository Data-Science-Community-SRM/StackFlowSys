//9. The most scored questions with specific tags – Questions having tag hadoop, spark in descending order of score
package com.df.stackoverflow

import scala.xml.XML

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import java.text.SimpleDateFormat
import java.lang.String
import scala.xml.Elem
import org.apache.spark.sql.SparkSession

object TagQuesScore {
	def main(args: Array[String]) = {
			System.setProperty("hadoop.home.dir", "D:\\Anish_Training_Data\\setups\\hadoop_setups\\CDH5\\hadoop-2.5.0-cdh5.3.2")
			System.setProperty("spark.sql.warehouse.dir", "file:/D:/Anish_Training_Data/setups/Spark_Setups/spark-2.0.0-bin-hadoop2.6/spark-warehouse")

			val spark = SparkSession
				.builder
				.appName("AvgAnsTime")
				.master("local")
				.getOrCreate()
				
			//Read some example file to a test RDD
			val data = spark.read.textFile("E:\\test\\Posts1.xml").rdd


			val result = data.filter{line => {line.trim().startsWith("<row")}
			}
			.filter { line => {line.contains("PostTypeId=\"1\"")}
			}
			.map {line => {
			  val xml = XML.loadString(line)
			  (xml.attribute("Tags").get.toString(), Integer.parseInt(xml.attribute("Score").get.toString()), line)
//			  x,y,z
//			  tags, score, line
			  }
			}
			.filter { tag => {tag._1.contains("bigdata")}
			}
			.map { data => {
			  (data._2, data._3)
//			  score, line
			}
			}
			.sortByKey(false)

			result.foreach { println }
//			println(result.count())

			spark.stop
	}
}
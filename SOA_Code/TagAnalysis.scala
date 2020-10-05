//10. List of all the tags along with their counts
package com.df.stackoverflow

import scala.xml.XML

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import java.text.SimpleDateFormat
import java.lang.String
import scala.xml.Elem
import org.apache.spark.sql.SparkSession

object TagAnalysis {
	def main(args: Array[String]) = {
			System.setProperty("hadoop.home.dir", "D:\\Anish_Training_Data\\setups\\hadoop_setups\\CDH5\\hadoop-2.5.0-cdh5.3.2")
			System.setProperty("spark.sql.warehouse.dir", "file:/D:/Anish_Training_Data/setups/Spark_Setups/spark-2.0.0-bin-hadoop2.6/spark-warehouse")

			val format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
      val format2 = new SimpleDateFormat("yyyy-MM");

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
//			  line
			  val xml = XML.loadString(line)
			  xml.attribute("Tags").get.toString()
//			  tagString
			  }
			}
			.flatMap { data => {
//			  tagString
			  data.replaceAll("&lt;", " ").replaceAll("&gt;", " ").split(" ")
//			  individual tag like spark
			}
			}
			.filter { tag => {tag.length() > 0 }
			}
			.map { data => {
			  (data, 1)
			}
			}
			.reduceByKey(_ + _)
			.sortByKey(true)

			result.foreach { println }
//			println(result.count())

			spark.stop
	}
}
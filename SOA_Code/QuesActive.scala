//7. Number of questions which are active for more than 6 months

package com.df.stackoverflow

import scala.xml.XML

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import java.text.SimpleDateFormat
import java.lang.String
import java.util.Date
import org.apache.spark.sql.SparkSession

object QuesActive {
	def main(args: Array[String]) = {
			System.setProperty("hadoop.home.dir", "D:\\Anish_Training_Data\\setups\\hadoop_setups\\CDH5\\hadoop-2.5.0-cdh5.3.2")
			System.setProperty("spark.sql.warehouse.dir", "file:/D:/Anish_Training_Data/setups/Spark_Setups/spark-2.0.0-bin-hadoop2.6/spark-warehouse")
			
			val format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

			val spark = SparkSession
				.builder
				.appName("QuesActive")
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
			  (xml.attribute("CreationDate").get,  xml.attribute("LastActivityDate").get, line)
//			  data._1                              data._2                              data._3
			  }
			}
			.map{ data => {
			  val crDate = format.parse(data._1.text)
			  val crTime = crDate.getTime;
			  
			  val edDate = format.parse(data._2.text)
			  val edTime = edDate.getTime;
			  
			  val timeDiff : Long = edTime - crTime
			  (crDate, edDate, timeDiff, data._3)
//			 data._1 data._2 data._3 data._4
			}
			}
			.filter { data => { data._3 / (1000 * 60 * 60 * 24) > 30*6}
			}
			
			result.foreach { println }
			println(result.count())
//			println(result.count())

			spark.stop
	}
}
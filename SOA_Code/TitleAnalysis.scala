//3. Provide the number of posts which are questions and contains specified words in their title (like data, science, nosql, hadoop, spark)
package com.df.stackoverflow

import scala.xml.XML

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import java.text.SimpleDateFormat
import java.lang.String
import scala.xml.Elem
import org.apache.spark.sql.SparkSession

object TitleAnalysis {
	def main(args: Array[String]) = {
			System.setProperty("hadoop.home.dir", "D:\\Anish_Training_Data\\setups\\hadoop_setups\\CDH5\\hadoop-2.5.0-cdh5.3.2")
			System.setProperty("spark.sql.warehouse.dir", "file:/D:/Anish_Training_Data/setups/Spark_Setups/spark-2.0.0-bin-hadoop2.6/spark-warehouse")

			val format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
      val format2 = new SimpleDateFormat("yyyy-MM");

			val spark = SparkSession
				.builder
				.appName("TitleAnalysis")
				.master("local")
				.getOrCreate()
				
			val data = spark.read.textFile("E:\\test\\Posts1.xml").rdd

			val result = data.filter{line => {line.trim().startsWith("<row")}
			}
			.filter { line => {line.contains("PostTypeId=\"1\"")}
			}
			.flatMap {line => {
			  val xml = XML.loadString(line)
			  xml.attribute("Title")
			  }
			}
			.filter { line => {
			  line.mkString.toLowerCase().contains("hadoop")
			}
			}
			
			result.foreach { println }
			println ("Result Count: " + result.count())
			
			spark.stop
	}
}
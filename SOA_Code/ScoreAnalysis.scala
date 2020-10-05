//4. The trending questions which are scored highly by the user – Top 10 highest Scored questions
package com.df.stackoverflow

import scala.xml.XML

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import java.text.SimpleDateFormat
import java.lang.String
import scala.xml.Elem
import org.apache.spark.sql.SparkSession

object ScoreAnalysis {
	def main(args: Array[String]) = {
			System.setProperty("hadoop.home.dir", "D:\\Anish_Training_Data\\setups\\hadoop_setups\\CDH5\\hadoop-2.5.0-cdh5.3.2")
			System.setProperty("spark.sql.warehouse.dir", "file:/D:/Anish_Training_Data/setups/Spark_Setups/spark-2.0.0-bin-hadoop2.6/spark-warehouse")

			val format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
      val format2 = new SimpleDateFormat("yyyy-MM");

			val spark = SparkSession
				.builder
				.appName("ScoreAnalysis")
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
			  (Integer.parseInt(xml.attribute("Score").getOrElse(0).toString()), line)
			  }
			}
//			(7, <row Id="5" PostTypeId="1" CreationDate="2014-05-13T23:58:30.457" Score="7" ViewCount="286" Body="&lt;p&gt;I've always been interested in machine learning, but I can't figure out one thing about starting out with a simple &quot;Hello World&quot; example - how can I avoid hard-coding behavior?&lt;/p&gt;&#xA;&#xA;&lt;p&gt;For example, if I wanted to &quot;teach&quot; a bot how to avoid randomly placed obstacles, I couldn't just use relative motion, because the obstacles move around, but I don't want to hard code, say, distance, because that ruins the whole point of machine learning.&lt;/p&gt;&#xA;&#xA;&lt;p&gt;Obviously, randomly generating code would be impractical, so how could I do this?&lt;/p&gt;&#xA;" OwnerUserId="5" LastActivityDate="2014-05-14T00:36:31.077" Title="How can I do simple machine learning without hard-coding behavior?" Tags="&lt;machine-learning&gt;" AnswerCount="1" CommentCount="1" FavoriteCount="1" ClosedDate="2014-05-14T14:40:25.950" />)
			.sortByKey(false)
			
			result.take(10).foreach(println)
//			result.foreach { println }

			spark.stop
	}
}
package com.spark.example.spark;

import java.util.Arrays;
import java.util.Scanner;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class WordCount {
	public static void main(String[] args) throws Exception {

		SparkConf sparkConf = new SparkConf().setAppName("SparkTestApp").setMaster("local[*]");
		JavaSparkContext sparkJavaContext = new JavaSparkContext(sparkConf);
		JavaRDD<String> file = sparkJavaContext.textFile("HELP.txt", 10);

		JavaRDD<String> wordsFromFile = file.flatMap(content -> Arrays.asList(content.split(" ")).iterator());
		System.out.println("wordsFromFile ==> " + wordsFromFile.count());

		JavaPairRDD content = wordsFromFile.mapToPair(w -> new Tuple2<>(w, 1))
				.reduceByKey((v1, v2) -> (int) v1 + (int) v2);
		System.out.println("content:: "+content.collect());
		
		Scanner scanner = new Scanner(System.in);
		scanner.nextLine();
		System.out.println("=============== Closing context ==================");
		sparkJavaContext.close();
	}
}

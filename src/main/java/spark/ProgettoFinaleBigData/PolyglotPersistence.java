package spark.ProgettoFinaleBigData;


import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;

import scala.Function1;
import scala.Serializable;
import scala.Tuple2;

public class PolyglotPersistence implements Serializable {

	private static final long serialVersionUID = 1L;
	private String pathToFile;

	public PolyglotPersistence(String path) {
		this.pathToFile = path;

	}

	public String getPathToFile() {
		return pathToFile;
	}

	public void setPathToFile(String pathToFile) {
		this.pathToFile = pathToFile;
	}

	public static void main(String[] args) {

		if (args.length < 2) {
			System.err.println("File path or Output location not found!");
			System.exit(1);
		}
		PolyglotPersistence p = new PolyglotPersistence(args[0]);
		SparkSession spark = SparkSession.builder()			     
				.appName("PolyglotPersistence")
				.config("spark.mongodb.input.uri","mongodb://172.17.0.2:27017/dbTerr.attacks")
				.config("spark.mongodb.output.uri","mongodb://172.17.0.2:27017/dbTerr.attacks")
				.getOrCreate();

		JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
		//attacchi terroristici in mongo
		JavaMongoRDD<Document> dataFromMongo = MongoSpark.load(jsc);
		//indicatori di sviluppo delle nazioni nell'hdfs
		JavaRDD<String> dataFromLake = p.loadDataFromDataLake(p.getPathToFile(), jsc);

		JavaPairRDD<Integer,Iterable<String>> mostAttackedCountry = p.mostAttackedCountries(dataFromMongo);
		JavaPairRDD<Integer,Iterable<Integer>> attacksPerYear = p.attacksperYear(dataFromMongo);
		JavaPairRDD<Integer,Iterable<String>> claimedAttacks = p.claimedAttacks(dataFromMongo);
		System.out.println(mostAttackedCountry.take(2));
		System.out.println(attacksPerYear.take(2));
		System.out.println(claimedAttacks.take(2));
	}

	public  JavaRDD<String> loadDataFromDataLake(String path,JavaSparkContext jsc) {
		JavaRDD<String> fileLines = jsc.textFile(this.pathToFile);
		JavaRDD<String> lines = fileLines.flatMap(line -> Arrays.asList(line.split("\n")).iterator());
		return lines;		
	}

	public JavaPairRDD<Integer,Iterable<String>> mostAttackedCountries(JavaMongoRDD<Document> input) {
		JavaPairRDD<String,Integer> countryOne = input.mapToPair(line -> {
			String country = (String) line.get("country_txt");
			return new Tuple2<String,Integer>(country,1);	
		});
		JavaPairRDD<Integer,Iterable<String>> result = countryOne.aggregateByKey(0, (a,b) -> a+b,(a,b) -> a+b)
				.mapToPair(a ->new Tuple2<Integer,String>(a._2,a._1)).groupByKey().sortByKey(false);
		
		return result;
	}
	
	public JavaPairRDD<Integer,Iterable<Integer>> attacksperYear(JavaMongoRDD<Document> input) {
		JavaPairRDD<Integer,Integer> yearOne = input.mapToPair(line -> {
			Integer year =  (Integer) line.get("iyear");
			return new Tuple2<Integer,Integer>(year,1);	
		});
		JavaPairRDD<Integer,Iterable<Integer>> result = yearOne.aggregateByKey(0, (a,b) -> a+b,(a,b) -> a+b)
				.mapToPair(a ->new Tuple2<Integer,Integer>(a._2,a._1)).groupByKey().sortByKey(false);
		
		return result;
	}
	
	public JavaPairRDD<Integer,Iterable<String>> claimedAttacks(JavaMongoRDD<Document> input) {
		JavaPairRDD<String,Integer> claimedAttacks = input.mapToPair(line -> {
			String claimed = (String) line.get("gname");
			if (claimed!= null && !claimed.isEmpty() && !claimed.equals("Unknown"))
				return new Tuple2<String,Integer>(claimed,1);
			else return new Tuple2<String,Integer>("",0);
		});
		JavaPairRDD<Integer,Iterable<String>> result = claimedAttacks.aggregateByKey(0, (a,b) -> a+b,(a,b) -> a+b)
				.mapToPair(a ->new Tuple2<Integer,String>(a._2,a._1)).groupByKey().sortByKey(false);
		
		return result;
	}
	
	
}

package spark.ProgettoFinaleBigData.PolyglotPersistence;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;

import com.mongodb.Function;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import scala.Serializable;
import scala.Tuple2;

public class MostAttackedCountries implements Serializable {

	private static final long serialVersionUID = 1L;
	private String pathToFile;

	public MostAttackedCountries() {

	}
	public MostAttackedCountries(String path) {
		this.pathToFile = path;

	}

	public String getPathToFile() {
		return pathToFile;
	}

	public void setPathToFile(String pathToFile) {
		this.pathToFile = pathToFile;
	}

	public static void main(String[] args) {

		MostAttackedCountries p = new MostAttackedCountries();
		SparkSession spark = SparkSession.builder()			     
				.appName("MostAttackedCountries")
				.config("spark.mongodb.input.uri","mongodb://172.17.0.2:27017/dbTerr.attacks")
				.config("spark.mongodb.output.uri","mongodb://172.17.0.2:27017/dbTerr.mostAttackedCountries")
				.getOrCreate();

		JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
		JavaMongoRDD<Document> dataFromMongo = MongoSpark.load(jsc);
		//JavaRDD<String> dataFromLake = p.loadDataFromDataLake(p.getPathToFile(), jsc);

		JavaPairRDD<Integer,Iterable<String>> mostAttackedCountries = p.mostAttackedCountries(dataFromMongo);
		System.out.println(mostAttackedCountries.take(10));
		JavaRDD<Document> mongoOutput = p.mapToMongo(mostAttackedCountries);
		System.out.println(mongoOutput.take(5));
		MongoSpark.save(mongoOutput);
	}

	//	public  JavaRDD<String> loadDataFromDataLake(String path,JavaSparkContext jsc) {
	//		JavaRDD<String> fileLines = jsc.textFile(this.pathToFile);
	//		JavaRDD<String> lines = fileLines.flatMap(line -> Arrays.asList(line.split("\n")).iterator());
	//		return lines;		
	//	}

	public JavaPairRDD<Integer,Iterable<String>> mostAttackedCountries(JavaMongoRDD<Document> input) {
		JavaPairRDD<String,Integer> countryOne = input.mapToPair(line -> {
			String country = (String) line.get("country_txt");
			return new Tuple2<String,Integer>(country,1);	
		});
		JavaPairRDD<Integer,Iterable<String>> result = countryOne.aggregateByKey(0, (a,b) -> a+b,(a,b) -> a+b)
				.mapToPair(a ->new Tuple2<Integer,String>(a._2,a._1)).groupByKey().sortByKey(false);

		return result;
	}

	@SuppressWarnings("unchecked")
	public JavaRDD<Document> mapToMongo(JavaPairRDD<Integer,Iterable<String>> input) {
		@SuppressWarnings("rawtypes")
		FlatMapFunction mapToDocument = new FlatMapFunction<Tuple2<Integer,Iterable<String>>,Document>(){
			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<Document> call(Tuple2<Integer, Iterable<String>> t) throws Exception {
				Document doc = new Document();
				doc.append("NumOfAttacks", t._1);
				doc.append("Countries", t._2.toString());
				List<Document> docs = new ArrayList<Document>();
				docs.add(doc);
				return docs.iterator();
			}
			
		};
	
		return input.flatMap(mapToDocument);
	}     
}


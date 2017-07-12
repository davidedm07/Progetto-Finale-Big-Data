package spark.ProgettoFinaleBigData.PolyglotPersistence;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;
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
		JavaPairRDD<Integer,Iterable<String>> mostAttackedCountries = p.mostAttackedCountries(dataFromMongo);
		System.out.println("First 5 lines of the result :");
		System.out.println(p.mostAttackedCountry(dataFromMongo).take(5));
		JavaRDD<Document> mongoOutput = p.mapToMongo(mostAttackedCountries);
		MongoSpark.save(mongoOutput);
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
	
	public JavaPairRDD<Integer,String> mostAttackedCountry(JavaMongoRDD<Document> input) {
		JavaPairRDD<String,Integer> countryOne = input.mapToPair(line -> {
			String country = (String) line.get("country_txt");
			return new Tuple2<String,Integer>(country,1);	
		});
		JavaPairRDD<Integer,String> result = countryOne.aggregateByKey(0, (a,b) -> a+b,(a,b) -> a+b)
				.mapToPair(a ->new Tuple2<Integer,String>(a._2,a._1)).sortByKey(false);

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


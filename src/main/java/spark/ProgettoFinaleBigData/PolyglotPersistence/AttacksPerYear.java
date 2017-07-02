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

public class AttacksPerYear implements Serializable{

	private static final long serialVersionUID = 1L;
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		AttacksPerYear p = new AttacksPerYear();
		SparkSession spark = SparkSession.builder()			     
				.appName("AttacksPerYear")
				.config("spark.mongodb.input.uri","mongodb://172.17.0.2:27017/dbTerr.attacks")
				.config("spark.mongodb.output.uri","mongodb://172.17.0.2:27017/dbTerr.attacksPerYear")
				.getOrCreate();
		
		JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
		JavaMongoRDD<Document> dataFromMongo = MongoSpark.load(jsc);
		JavaPairRDD<Integer,Iterable<Integer>> attacksperYear = p.attacksperYear(dataFromMongo);
		MongoSpark.save(p.mapToMongo(attacksperYear));
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
	@SuppressWarnings("unchecked")
	public JavaRDD<Document> mapToMongo(JavaPairRDD<Integer,Iterable<Integer>> input) {
		@SuppressWarnings("rawtypes")
		FlatMapFunction mapToDocument = new FlatMapFunction<Tuple2<Integer,Iterable<Integer>>,Document>(){
			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<Document> call(Tuple2<Integer, Iterable<Integer>> t) throws Exception {
				Document doc = new Document();
				doc.append("Years", t._2.toString());
				doc.append("NumOfAttacks", t._1);
				List<Document> docs = new ArrayList<Document>();
				docs.add(doc);
				return docs.iterator();
			}
			
		};
	
		return input.flatMap(mapToDocument);
	}     

}

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

public class ClaimedAttacks implements Serializable{

	private static final long serialVersionUID = 1L;

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		ClaimedAttacks p = new ClaimedAttacks();
		SparkSession spark = SparkSession.builder()			     
				.appName("ClaimedAttacks")
				.config("spark.mongodb.input.uri","mongodb://172.17.0.2:27017/dbTerr.attacks")
				.config("spark.mongodb.output.uri","mongodb://172.17.0.2:27017/dbTerr.claimedAttacks")
				.getOrCreate();

		JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
		JavaMongoRDD<Document> dataFromMongo = MongoSpark.load(jsc);
		JavaPairRDD<Integer,Iterable<String>> claimedAttacks = p.claimedAttacks(dataFromMongo);
		MongoSpark.save(p.mapToMongo(claimedAttacks));

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

	@SuppressWarnings("unchecked")
	public JavaRDD<Document> mapToMongo(JavaPairRDD<Integer,Iterable<String>> input) {
		@SuppressWarnings("rawtypes")
		FlatMapFunction mapToDocument = new FlatMapFunction<Tuple2<Integer,Iterable<String>>,Document>(){
			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<Document> call(Tuple2<Integer, Iterable<String>> t) throws Exception {
				Document doc = new Document();
				doc.append("NumOfAttacks", t._1);
				doc.append("GroupName", t._2.toString());
				List<Document> docs = new ArrayList<Document>();
				docs.add(doc);
				return docs.iterator();
			}
			
		};
	
		return input.flatMap(mapToDocument);
	}     

}

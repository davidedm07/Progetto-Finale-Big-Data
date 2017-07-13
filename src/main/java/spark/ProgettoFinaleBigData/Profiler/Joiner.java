package spark.ProgettoFinaleBigData.Profiler;

import java.io.Serializable;
import java.util.Arrays;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;

import scala.Tuple2;
import spark.ProgettoFinaleBigData.PolyglotPersistence.ParametricJoin;

public class Joiner implements Serializable {
	
	private static final long serialVersionUID = 1L;
	private String datasetFromMongo;
	private String dataFromLakePath;

	public Joiner() {}

	public Joiner(String mongoPath, String lakePath) {
		this.datasetFromMongo = mongoPath;
		this.dataFromLakePath = lakePath;
	}

	public String getDatasetFromMongo() {
		return datasetFromMongo;
	}

	public void setDatasetFromMongo(String datasetFromMongo) {
		this.datasetFromMongo = datasetFromMongo;
	}

	public String getDataFromLake() {
		return dataFromLakePath;
	}

	public void setDataFromLake(String dataFromLake) {
		this.dataFromLakePath = dataFromLake;
	}
	
	public SparkSession getSparkSession(String path) {
		return SparkSession.builder()
			.appName("Joiner")
			.config("spark.mongodb.input.uri","mongodb://172.17.0.2:27017/" + path)
			.config("spark.mongodb.output.uri","mongodb://172.17.0.2:27017/")
			.getOrCreate();
	}
	
	public String getCsvPrincipalColumn(String metadataToLoad) {
		String columnToSearch = "Key Column";
		MongoClient mongo = new MongoClient( "172.17.0.2" , 27017 );
		MongoDatabase db = mongo.getDatabase("metadata");
		MongoCollection<Document> metadataTable = db.getCollection(metadataToLoad);
		Bson query = new BasicDBObject(columnToSearch, new BasicDBObject("$exists", true));
		FindIterable<Document> result = metadataTable.find(query);
		String principalColumnIndex = result.iterator().next().get(columnToSearch).toString();
		mongo.close();
		return principalColumnIndex;
	}
	public static void main(String[] args) {

		if (args.length < 2) {
			System.err.println("File path or Output location not found!");
			System.exit(1);
		}
		
		Joiner joiner = new Joiner(args[0], "hdfs://localhost:9000/input/WDIData.csv");

		SparkSession spark = SparkSession.builder()			     
				.appName("Joiner")
				.config("spark.mongodb.input.uri","mongodb://172.17.0.2:27017/dbTerr.attacks")
				.config("spark.mongodb.output.uri","mongodb://172.17.0.2:27017/dbTerr.Joiner")
				.getOrCreate();
		
		JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
		//String columnIndex = joiner.getCsvPrincipalColumn(args[1]);
	    //System.out.println(columnIndex);
		
		JavaMongoRDD<Document> dataFromMongo = MongoSpark.load(jsc);
		ParametricJoin pj = new ParametricJoin();
		pj.setPathToFile("hdfs://localhost:9000/input/WDIData.csv");
		JavaRDD<String> dataFromLake = pj.loadDataFromDataLake(pj.getPathToFile(), jsc);
		pj.setDataFromLake(dataFromLake);
		pj.setDataFromMongo(dataFromMongo);
		pj.setKeyColumn(0);
		JavaPairRDD<String,Tuple2<String,String>> join = pj.join();
		System.out.println(join.take(5));
	}
	
}

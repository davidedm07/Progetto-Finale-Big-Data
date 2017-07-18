package spark.ProgettoFinaleBigData.PolyglotPersistence;

import java.io.Serializable;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;

import scala.Tuple2;

public class ParametricJoin implements Serializable {

	private static final long serialVersionUID = 1L;
	private JavaRDD<Document> dataFromMongo;
	private JavaRDD<String> dataFromLake;
	private int keyColumn;
	private String pathToFile;

	public JavaRDD<Document> getDataFromMongo() {
		return dataFromMongo;
	}


	public ParametricJoin() {

	}

	public ParametricJoin(JavaRDD<Document> mongo, JavaRDD<String> lake, int column) {
		this.dataFromLake = lake;
		this.dataFromMongo = mongo;
		this.keyColumn = column;	
	}

	private JavaPairRDD<String,Tuple2<String,String>> joinWithCommas() {
		JavaPairRDD<String,String> temp1 = this.dataFromMongo
				.mapToPair(doc -> new Tuple2<String,String>((String)doc.get("country_txt"),doc.values().toString()));

		@SuppressWarnings({ "unchecked", "rawtypes" })
		JavaPairRDD<String,String> temp2 = this.dataFromLake
		.mapToPair(line -> {
			String[] splitLine = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
			String country = splitLine[this.keyColumn].replaceAll("\"", "");
			return new Tuple2(country,line);	
		});
		return temp1.join(temp2);		
	}

	
	private JavaPairRDD<String,Tuple2<String,String>> joinWithSemiColon() {
		JavaPairRDD<String,String> temp1 = this.dataFromMongo
				.mapToPair(doc -> new Tuple2<String,String>((String)doc.get("country_txt"),doc.values().toString()));

		@SuppressWarnings({ "unchecked", "rawtypes" })
		JavaPairRDD<String,String> temp2 = this.dataFromLake
		.mapToPair(line -> {
			String[] splitLine = line.split(";");
			String country = splitLine[this.keyColumn];
			return new Tuple2(country,line);	
		});
		return temp1.join(temp2);		
	}
	
	public JavaPairRDD<String,Tuple2<String,String>> join(String delimiter) {
		JavaPairRDD<String,Tuple2<String,String>> join = null;
		if (delimiter.equals(",")) {
			join = joinWithCommas();
		}
		else if (delimiter.equals(";")) {
			join = joinWithSemiColon();
		}
		else {
			System.err.println("Invalid delimiter!");
			System.exit(1);
		}
		return join;
		
	}

	public static void main(String[] args) {

		if (args.length < 3) {
			System.err.println("File path, metadata Table or delimiter not found!");
			System.exit(1);
		}

		SparkSession spark = SparkSession.builder()			     
				.appName("ParametricJoin")
				.config("spark.mongodb.input.uri","mongodb://172.17.0.2:27017/dbTerr.attacks")
				.config("spark.mongodb.output.uri","mongodb://172.17.0.2:27017/dbTerr.ParametricJoin")
				.getOrCreate();

		JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
		JavaMongoRDD<Document> dataFromMongo = MongoSpark.load(jsc);
		ParametricJoin pj = new ParametricJoin();
		int joinColumnIndex = pj.getCsvPrincipalColumn(args[1]);
		String delimiter = args[2];
		pj.setPathToFile(args[0]);
		JavaRDD<String> dataFromLake = pj.loadDataFromDataLake(pj.getPathToFile(), jsc);
		pj.setDataFromLake(dataFromLake);
		pj.setDataFromMongo(dataFromMongo);
		pj.setKeyColumn(joinColumnIndex);
		System.out.println("MONGO: " + dataFromMongo.take(5));
		System.out.println("LAKE: " + dataFromLake.take(5));
		JavaPairRDD<String,Tuple2<String,String>> join = pj.join(delimiter);
		System.out.println("Join: " + join.take(5));
		

	}
	
	public int getCsvPrincipalColumn(String metadataToLoad) {
		String columnToSearch = "Key Column";
		MongoClient mongo = new MongoClient( "172.17.0.2" , 27017 );
		MongoDatabase db = mongo.getDatabase("metadata");
		MongoCollection<Document> metadataTable = db.getCollection(metadataToLoad);
		Bson query = new BasicDBObject(columnToSearch, new BasicDBObject("$exists", true));
		FindIterable<Document> result = metadataTable.find(query);
		String principalColumnIndex = result.iterator().next().get(columnToSearch).toString();
		mongo.close();
		return Integer.parseInt(principalColumnIndex);
	}
	
	public  JavaRDD<String> loadDataFromDataLake(String path,JavaSparkContext jsc) {
		JavaRDD<String> fileLines = jsc.textFile(this.pathToFile);
		String header = fileLines.take(1).get(0);
		JavaRDD<String> lines = fileLines.filter(row -> !(row.equals(header)));
		return lines;
	}
	
	public void setDataFromMongo(JavaRDD<Document> dataFromMongo) {
		this.dataFromMongo = dataFromMongo;
	}

	public JavaRDD<String> getDataFromLake() {
		return dataFromLake;
	}

	public void setDataFromLake(JavaRDD<String> dataFromLake) {
		this.dataFromLake = dataFromLake;
	}

	public int getKeyColumn() {
		return keyColumn;
	}

	public void setKeyColumn(int keyColumn) {
		this.keyColumn = keyColumn;
	}

	public String getPathToFile() {
		return pathToFile;
	}

	public void setPathToFile(String pathToFile) {
		this.pathToFile = pathToFile;
	}

}

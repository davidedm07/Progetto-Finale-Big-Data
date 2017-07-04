package spark.ProgettoFinaleBigData.PolyglotPersistence;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;

import scala.Tuple2;

public class AttacksEducation implements Serializable {

	private static final long serialVersionUID = 1L;
	private String pathToFile;
	private String educationCode = "NY.ADJ.AEDU.CD";

	public AttacksEducation(String path) {
		this.setPathToFile(path);
	}

	public static void main(String[] args) {
		if (args.length < 2) {
			System.err.println("File path or Output location not found!");
			System.exit(1);
		}

		AttacksEducation att = new AttacksEducation(args[0]);
		SparkSession spark = SparkSession.builder()			     
				.appName("AttacksEducation")
				.config("spark.mongodb.input.uri","mongodb://172.17.0.2:27017/dbTerr.attacks")
				.config("spark.mongodb.output.uri","mongodb://172.17.0.2:27017/dbTerr.attacksEducation")
				.getOrCreate();

		JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
		JavaMongoRDD<Document> dataFromMongo = MongoSpark.load(jsc);
		JavaRDD<String> dataFromLake = att.loadDataFromDataLake(att.getPathToFile(), jsc);
		//System.out.println(dataFromLake.take(5).toString());
		//System.out.println(dataFromMongo.take(5).toString());
		JavaPairRDD<String,Tuple2<String,String>> join = att.join(dataFromMongo, dataFromLake);
		System.out.println(join.take(10));
	}

	public  JavaRDD<String> loadDataFromDataLake(String path,JavaSparkContext jsc) {
		JavaRDD<String> fileLines = jsc.textFile(this.pathToFile);
		String header = fileLines.take(1).get(0);
		JavaRDD<String> lines = fileLines.filter(row -> !(row.equals(header)));
		return lines;
	}

	public JavaPairRDD<String,Tuple2<String,String>> join (JavaRDD<Document> dataFromMongo,JavaRDD<String> dataFromLake) {
		JavaPairRDD<String,String> temp1 = dataFromLake
				.mapToPair(line -> new Tuple2<String,String>(line.split(",")[0],line));
		JavaPairRDD<String,String> temp2 = dataFromMongo
				.mapToPair(doc -> new Tuple2<String,String>(doc.get("country_txt").toString(),doc.values().toString()));
		//System.out.println(temp1.take(5));
		System.out.println(temp2.take(5));
		JavaPairRDD<String,Tuple2<String,String>> join = temp1.join(temp2);
		return join;
	}

	public String getPathToFile() {
		return pathToFile;
	}

	public void setPathToFile(String pathToFile) {
		this.pathToFile = pathToFile;
	}

}

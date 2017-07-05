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

public class AttacksDefenseExpenditure implements Serializable {

	private static final long serialVersionUID = 1L;
	private String pathToFile;
	private String defenseCode = "MS.MIL.XPND.GD.ZS";

	public AttacksDefenseExpenditure(String path) {
		this.setPathToFile(path);
	}

	public static void main(String[] args) {
		if (args.length < 2) {
			System.err.println("File path or Output location not found!");
			System.exit(1);
		}

		AttacksDefenseExpenditure att = new AttacksDefenseExpenditure(args[0]);
		SparkSession spark = SparkSession.builder()			     
				.appName("AttacksEducation")
				.config("spark.mongodb.input.uri","mongodb://172.17.0.2:27017/dbTerr.attacks")
				.config("spark.mongodb.output.uri","mongodb://172.17.0.2:27017/dbTerr.attacksEducation")
				.getOrCreate();

		JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
		JavaMongoRDD<Document> dataFromMongo = MongoSpark.load(jsc);
		JavaRDD<String> dataFromLake = att.loadDataFromDataLake(att.getPathToFile(), jsc);
		JavaPairRDD<String,Tuple2<String,String>> join = att.join(dataFromMongo, dataFromLake);
		System.out.println(join.take(1));
	}

	public  JavaRDD<String> loadDataFromDataLake(String path,JavaSparkContext jsc) {
		JavaRDD<String> fileLines = jsc.textFile(this.pathToFile);
		String header = fileLines.take(1).get(0);
		JavaRDD<String> lines = fileLines.filter(row -> !(row.equals(header)));
		return lines;
	}

	public JavaPairRDD<String,Tuple2<String,String>> join (JavaRDD<Document> dataFromMongo,JavaRDD<String> dataFromLake) {
		JavaPairRDD<String,String> temp1 = dataFromLake
				.filter(line-> line.contains(defenseCode))
				.mapToPair(line -> new Tuple2<String,String>(line.split(",")[0].replaceAll("\"",""),line));
		JavaPairRDD<String,String> temp2 = dataFromMongo
				.mapToPair(doc -> new Tuple2<String,String>((String)doc.get("country_txt"),doc.values().toString()));
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

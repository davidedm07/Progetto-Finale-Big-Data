package spark.ProgettoFinaleBigData;


import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;

import scala.Serializable;

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
		JavaMongoRDD<Document> rdd = MongoSpark.load(jsc);
		JavaRDD<String> dataFromLake = p.loadDataFromDataLake(p.getPathToFile(), jsc);
		System.out.println(rdd.take(1));
		System.out.println(dataFromLake.take(1));
		
	}
	
	public  JavaRDD<String> loadDataFromDataLake(String path,JavaSparkContext jsc) {
		JavaRDD<String> fileLines = jsc.textFile(this.pathToFile);
		// aggiungere altre trasformazioni dei dati
		return fileLines;		
	}

}

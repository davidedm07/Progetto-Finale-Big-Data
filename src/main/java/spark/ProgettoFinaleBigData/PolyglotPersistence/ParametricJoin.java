package spark.ProgettoFinaleBigData.PolyglotPersistence;

import java.io.Serializable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;

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

	public JavaPairRDD<String,Tuple2<String,String>> join() {
		JavaPairRDD<String,String> temp1 = this.dataFromMongo
				.mapToPair(doc -> new Tuple2<String,String>((String)doc.get("country_txt"),doc.values().toString()));

		@SuppressWarnings({ "unchecked", "rawtypes" })
		JavaPairRDD<String,String> temp2 = this.dataFromLake
		.mapToPair(line -> {
			Pattern p = Pattern.compile("\"[^\"]*\"");
			Matcher m = p.matcher(line);
			int cont = 0;
			String country = "";
			while(m.find()) {
				if (cont == this.keyColumn) {
					country = m.group().replaceAll("\"", "");
					break;
				}
				cont ++;	
			}
			return new Tuple2(country,line);	
		});
		return temp1.join(temp2);		
	}

	public static void main(String[] args) {

		if (args.length < 2) {
			System.err.println("File path or Output location not found!");
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
		pj.setPathToFile(args[0]);
		JavaRDD<String> dataFromLake = pj.loadDataFromDataLake(pj.getPathToFile(), jsc);
		pj.setDataFromLake(dataFromLake);
		pj.setDataFromMongo(dataFromMongo);
		pj.setKeyColumn(0);
		JavaPairRDD<String,Tuple2<String,String>> join = pj.join();
		System.out.println(join.take(5));

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

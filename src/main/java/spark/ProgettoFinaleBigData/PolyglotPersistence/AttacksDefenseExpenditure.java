package spark.ProgettoFinaleBigData.PolyglotPersistence;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
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
				.appName("AttacksDefenseExpenditure")
				.config("spark.mongodb.input.uri","mongodb://172.17.0.2:27017/dbTerr.attacks")
				.config("spark.mongodb.output.uri","mongodb://172.17.0.2:27017/dbTerr.attacksDefenseExpenditure")
				.getOrCreate();

		JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
		JavaMongoRDD<Document> dataFromMongo = MongoSpark.load(jsc);
		JavaRDD<String> dataFromLake = att.loadDataFromDataLake(att.getPathToFile(), jsc);
		JavaPairRDD<Tuple2<String,String>,Iterable<String>> join = att.join(dataFromMongo, dataFromLake);
		JavaPairRDD<Integer,Tuple2<String,Double>> result = att.defenseExpenditureAttacks(join, dataFromMongo);
		result.coalesce(1).saveAsTextFile(args[1]);
	}

	public  JavaRDD<String> loadDataFromDataLake(String path,JavaSparkContext jsc) {
		JavaRDD<String> fileLines = jsc.textFile(this.pathToFile);
		String header = fileLines.take(1).get(0);
		JavaRDD<String> lines = fileLines.filter(row -> !(row.equals(header)));
		return lines;
	}

	public JavaPairRDD<Tuple2<String,String>,Iterable<String>> join (JavaRDD<Document> dataFromMongo,JavaRDD<String> dataFromLake) {
		JavaPairRDD<String,String> temp1 = dataFromLake
				.filter(line-> line.contains(defenseCode))
				.mapToPair(line -> new Tuple2<String,String>(line.split(",")[0].replaceAll("\"",""),line));
		JavaPairRDD<String,String> temp2 = dataFromMongo
				.mapToPair(doc -> new Tuple2<String,String>((String)doc.get("country_txt"),doc.values().toString()));
		JavaPairRDD<Tuple2<String,String>,String> join = temp1.join(temp2)
				.mapToPair(input -> 
				new Tuple2<Tuple2<String,String>,String>(
						new Tuple2<String,String>(input._1,input._2._1),input._2._2));
		
		return join.groupByKey();
		
		
	}

	public JavaPairRDD<Integer,Tuple2<String,Double>> 
	defenseExpenditureAttacks(JavaPairRDD<Tuple2<String,String>,Iterable<String>> join, JavaMongoRDD<Document> attacks) {
		MostAttackedCountries mac = new MostAttackedCountries();
		@SuppressWarnings({ "unchecked", "rawtypes" })
		JavaPairRDD<String,Integer> attackedCountries = mac.mostAttackedCountry(attacks)
		.mapToPair(tuple -> new Tuple2(tuple._2,tuple._1));
		JavaPairRDD<Integer,Tuple2<String,Double>> result = attackedCountries
				.join(join.mapToPair(tuple -> new Tuple2<String,String>(tuple._1._1,tuple._1._2)))
				.mapToPair(input -> 
				new Tuple2<Integer,Tuple2<String,Double>>(input._2._1,new Tuple2<String,Double>(input._1,getAverage(input._2._2))))
				.sortByKey(false);
		return result;
	}
	
	/**
	 * It needs a JavaSpark context configured for mongoDB 
	 * @param result
	 */
	@SuppressWarnings("unchecked")
	public void saveToMongo(JavaPairRDD<Integer,Tuple2<String,Double>> result) {
		@SuppressWarnings("rawtypes")
		FlatMapFunction mapToDocument = new FlatMapFunction<Tuple2<Integer,Tuple2<String,Double>>,Document>(){
			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<Document> call(Tuple2<Integer,Tuple2<String,Double>> t) throws Exception {
				Document doc = new Document();
				doc.append("NumOfAttacks", t._1);
				doc.append("Country", t._2._1.toString());
				doc.append("Average expenditure for Defense",t._2._2.toString());
				List<Document> docs = new ArrayList<Document>();
				docs.add(doc);
				return docs.iterator();
			}		
		};
		MongoSpark.save(result.flatMap(mapToDocument));
	}

	public String getPathToFile() {
		return pathToFile;
	}

	public Double getAverage(String line) {
		int cont = 0;
		double sum = 0;
		int i= 0;
		Pattern p = Pattern.compile("\"[^\"]*\"");
		Matcher m = p.matcher(line);
		while (m.find()) {
			if (i>60)
				i=0;
			if(i>=4 && i<=60) {
				String x = m.group().replaceAll("\"", "");
				if (x!=null && !x.isEmpty()) {
					sum += Double.parseDouble(x);
					cont++;
				}
			}
			i++;
		}
		if (sum == 0 || cont == 0)
			return 0.0;
		return sum/cont;


	}

	public void setPathToFile(String pathToFile) {
		this.pathToFile = pathToFile;
	}

}

package spark.ProgettoFinaleBigData.Profiler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;

import com.mongodb.spark.MongoSpark;

import scala.Serializable;
import scala.Tuple2;

public class ValuesExtractor implements Serializable {

	private static final long serialVersionUID = 1L;
	private String pathToFile;
	private String separator;

	public ValuesExtractor(String path, String separator) {
		this.pathToFile = path;
		this.setSeparator(separator);
	}

	public String getPathToFile() {
		return pathToFile;
	}

	public void setPathToFile(String pathToFile) {
		this.pathToFile = pathToFile;
	}
	
	public String getSeparator() {
		return separator;
	}

	public void setSeparator(String separator) {
		this.separator = separator;
	}
	
	public SparkSession getSparkSession() {
		return SparkSession.builder()
			.appName("ValuesExtractor")
			.config("spark.mongodb.input.uri","mongodb://172.17.0.2:27017/")
			.config("spark.mongodb.output.uri","mongodb://172.17.0.2:27017/")
			.getOrCreate();
	}
	public Map<Integer, Long> getLikelyPrincipalColumnIndexes(Dataset<Row> df) {
		
		String[] columnsList = df.columns();
		//Long totalRows = df.count();
		List<Long> columnsNumRows = new ArrayList<Long>();
		List<Boolean> likelyPrincipalColumns = new ArrayList<Boolean>();
		Map<Integer, Long> likelyPrincipalColumnsRows = new HashMap<Integer, Long>();
		Row nullRow = RowFactory.create("null", new Long(0));
		int i;
		for (i = 0; i < columnsList.length; i++) {
			Column datasetColumn = df.col("`" + columnsList[i] +"`");
			Dataset<Row> column = df.select(datasetColumn).groupBy(datasetColumn).count();
			Dataset<Row> columnNullRow = column.filter(line -> {
				try {
					if (String.valueOf((String)line.getAs(0)).equals(nullRow.getAs(0)) || String.valueOf((String)line.getAs(0)).equals("0")) {
						return true;}
					else {
						return false;
					}
				}
				catch (ClassCastException e ) {};
				return true;
			});
			Long columnNum = column.count();
			if (columnNullRow.count() > 1 || columnNum < 5) {
				likelyPrincipalColumns.add(false);
			}
			else {
				likelyPrincipalColumnsRows.put(i, columnNum);
				likelyPrincipalColumns.add(true);
			}
			columnsNumRows.add(columnNum);
		}
		
		return likelyPrincipalColumnsRows;
		
	}
	
	public Dataset<Row> parseCsv (SparkSession spark, String path, String separator) {
		return spark.read()
		    .format("com.databricks.spark.csv")
		    .option("delimiter", separator)
		    .option("inferSchema", "true")
		    .option("header", "true")
		    .load("hdfs://localhost:9000/input/" + path);
	}
	
	public int choosePrincipalColumn (Map<Integer, Long> likelyPrincipalColumnsRows) {
		int k;
		int principalColumnIndex = 0;
		Long min = new Long(Long.MAX_VALUE);
		for (Integer key : likelyPrincipalColumnsRows.keySet()) {
			if (likelyPrincipalColumnsRows.get(key) < min) {
				principalColumnIndex = key;
				min = likelyPrincipalColumnsRows.get(key);
		}
	}
		return principalColumnIndex;
	}

	public void saveColumnValues (List<JavaRDD> columnsValues) {
		Iterator itr = columnsValues.iterator();
		int j = 0;
		while (itr.hasNext()) {
			((JavaRDD<Row>) itr.next()).coalesce(1).saveAsTextFile("hdfs://localhost:9000/output/DataWorked_" + j);
			j++;
			}
	}
	
	public DatasetMetadata buildDatasetMetadata(Map<Integer, Long> likelyPrincipalColumns, int mostLikelyPrincipalColumn) {
		return new DatasetMetadata(likelyPrincipalColumns, mostLikelyPrincipalColumn);
	}
	
	public void saveToMongo(DatasetMetadata data, JavaSparkContext jsc) {
		List<Document> docs = new ArrayList<>();
		Map<Integer,Long> map = data.getLikelyPrincipalColumns();
		for(int k : map.keySet()) {
			Document doc = new Document();
			doc.append("Column Number", k);
			doc.append("Different Values", map.get(k));
			docs.add(doc);
		}
		Document keyColumn = new Document();
		keyColumn.append("Key Column", data.getMostLikelyPrincipalColumn());
		docs.add(keyColumn);
		JavaRDD<Document> docsRDD =  jsc.parallelize(docs);
		MongoSpark.save(docsRDD);
		
	}
	
	public static void main(String[] args) {
		if (args.length < 2) {
			System.err.println("File path or separator not found!");
			System.exit(1);
		}
		
		/* il seguente codice mostra un utilizzo della classe*/
		ValuesExtractor v = new ValuesExtractor(args[0], args[1]);
		SparkSession spark = v.getSparkSession();
		Dataset<Row> df = v.parseCsv(spark, v.pathToFile, v.getSeparator());
		Map<Integer, Long> likelyPrincipalColumnsRows = v.getLikelyPrincipalColumnIndexes(df);
		System.out.println("Possibili colonne");
		for (Integer key : likelyPrincipalColumnsRows.keySet()) {
		    System.out.println(key + " " + likelyPrincipalColumnsRows.get(key));
		}
		int mostLikelyPrincipalColumn = v.choosePrincipalColumn(likelyPrincipalColumnsRows);
		System.out.println("La colonna principale ha indice: " + String.valueOf(mostLikelyPrincipalColumn));
		System.out.println(String.valueOf(likelyPrincipalColumnsRows.get(mostLikelyPrincipalColumn)) + " valori diversi.");

	}



}

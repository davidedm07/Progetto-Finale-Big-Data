package spark.ProgettoFinaleBigData.Profiler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import scala.Serializable;

public class ValuesExtractor implements Serializable {

	private static final long serialVersionUID = 1L;
	private String pathToFile;

	public ValuesExtractor(String path) {
		this.pathToFile = path;
	}

	public String getPathToFile() {
		return pathToFile;
	}

	public void setPathToFile(String pathToFile) {
		this.pathToFile = pathToFile;
	}
	public SparkSession getSparkSession() {
		return SparkSession.builder()
			.appName("ValuesExtractor")
			.config("spark.mongodb.input.uri","mongodb://172.17.0.2:27017/")
			.config("spark.mongodb.output.uri","mongodb://172.17.0.2:27017/")
			.getOrCreate();
	}
	public Map<Integer, Long> getLikelyPrincipalColumnIndexes(SparkSession spark, ValuesExtractor v, String[] args) {
		
		Dataset<Row> df = v.parseCsv(spark, v.pathToFile);
		
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
					if (String.valueOf((String)line.getAs(0)).equals(nullRow.getAs(0))) {
						return true;}
					else {
						return false;
					}
				}
				catch (ClassCastException e ) {};
				return false;
			});
			Long columnNum = column.count();
			if (columnNullRow.count() == 1) {
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
	
	public Dataset<Row> parseCsv (SparkSession spark, String path) {
		return spark.read()
		    .format("com.databricks.spark.csv")
		    .option("inferSchema", "true")
		    .option("header", "true")
		    .load("hdfs://localhost:9000/input/" + path);
	}
	
	public int choosePrincipalColumn (Map<Integer, Long> likelyPrincipalColumnsRows) {
		int k;
		int principalColumnIndex = 0;
		Long min = new Long(Long.MAX_VALUE);
		for (k = 0; k < likelyPrincipalColumnsRows.size(); k++) {
			if (likelyPrincipalColumnsRows.get(k) < min) {
				principalColumnIndex = k;
				min = likelyPrincipalColumnsRows.get(k);
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
	
	public static void main(String[] args) {
		
		if (args.length < 1) {
			System.err.println("File path not found!");
			System.exit(1);
		}
		
		/* il seguente codice mostra un utilizzo della classe
		ValuesExtractor v = new ValuesExtractor(args[0]);
		SparkSession spark = v.getSparkSession();
		Map<Integer, Long> likelyPrincipalColumnsRows = v.getLikelyPrincipalColumnIndexes(spark, v, args);
		int indice = v.choosePrincipalColumn(likelyPrincipalColumnsRows);
		System.out.println("La colonna principale ha indice: " + String.valueOf(indice));
		System.out.println(String.valueOf(likelyPrincipalColumnsRows.get(indice)) + " valori diversi.");
		*/
	}

}

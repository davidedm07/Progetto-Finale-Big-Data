package spark.ProgettoFinaleBigData.Profiler;

import java.util.Map;

import scala.Serializable;

public class DatasetMetadata implements Serializable {

	private static final long serialVersionUID = 1L;
	private Map<Integer, Long> likelyPrincipalColumns;
	private int mostLikelyPrincipalColumn;
	
	public DatasetMetadata() {}
	
	public DatasetMetadata(Map<Integer, Long> columnMap, int principalColumnIndex) {
		this.likelyPrincipalColumns = columnMap;
		this.mostLikelyPrincipalColumn = principalColumnIndex;
	}
	
	public Map<Integer, Long> getLikelyPrincipalColumns() {
		return likelyPrincipalColumns;
	}
	public void setLikelyPrincipalColumns(Map<Integer, Long> likelyPrincipalColumns) {
		this.likelyPrincipalColumns = likelyPrincipalColumns;
	}
	public int getMostLikelyPrincipalColumn() {
		return mostLikelyPrincipalColumn;
	}
	public void setMostLikelyPrincipalColumn(int mostLikelyPrincipalColumn) {
		this.mostLikelyPrincipalColumn = mostLikelyPrincipalColumn;
	}
	
}

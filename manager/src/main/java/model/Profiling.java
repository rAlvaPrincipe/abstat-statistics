package model;

import org.springframework.data.mongodb.core.mapping.Document;

@Document
public class Profiling {
	
	private String id;
	private String idDataset;
	private String datasetName;
	private String statisticsPosition;
	public boolean isConsolidate;
	
	public String getId() {return id;}
	public void setId(String id) {this.id = id;}
	
	public String getIdDataset() {return idDataset;}
	public void setIdDataset(String idDataset) {this.idDataset = idDataset;}
	
	public String getDatasetName() {return datasetName;}
	public void setDatasetName(String datasetName) {this.datasetName = datasetName;}
	
	public String getStatisticsPosition() {return statisticsPosition;}
	public void setStatisticsPosition(String statisticsPosition) {this.statisticsPosition = statisticsPosition;}
	
	public boolean isConsolidate() {return isConsolidate;}
	public void setConsolidate(boolean isConsolidate) {this.isConsolidate = isConsolidate;}
}
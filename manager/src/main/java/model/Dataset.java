package model;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import com.fasterxml.jackson.annotation.JsonView;

@Document
public class Dataset {

	@Id
	@JsonView(View.Datasets.class)
	private String id;
	
	@JsonView(View.Datasets.class)
	private String datasetName;
	
	private String datasetPosition;
	private boolean isCalculateStatistics;

	
	public String getId() { return id; }
	public void setId(String id) { this.id = id; }
	
	public String getDatasetName() {return datasetName;}
	public void setDatasetName(String datasetName) {this.datasetName = datasetName;}
	
	public String getDatasetPosition() {return datasetPosition;}
	public void setDatasetPosition(String datasetPosition) {this.datasetPosition = datasetPosition;}
	
	public boolean isCalculateStatistics() {return isCalculateStatistics;}
	public void setCalculateStatistics(boolean isCalculateStatistics) {this.isCalculateStatistics = isCalculateStatistics;}
}
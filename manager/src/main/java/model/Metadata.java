package model;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import com.fasterxml.jackson.annotation.JsonView;

@Document
public class Metadata {

	@Id
	@JsonView(View.Datasets.class)
	private String id;
	
	@JsonView(View.Datasets.class)
	private String dataset;
	
	private String timestamp;
	private String positionDataset;
	private String positionStatistics;

	private boolean isCalculateStatistics;
	private boolean isConsolidate;

	
	public String getId() { return id; }
	public void setId(String id) { this.id = id; }
	
	public String getDataset() { return dataset; }
	public void setDataset(String dataset) { this.dataset = dataset; }
	
	public String getTimestamp() { return timestamp; }
	public void setTimestamp(String timestamp) { this.timestamp = timestamp; }

	public boolean isCalculateStatistics() {return isCalculateStatistics;}
	public void setCalculateStatistics(boolean isCalculateStatistics) {this.isCalculateStatistics = isCalculateStatistics;}

	public boolean isConsolidate() {return isConsolidate;}
	public void setConsolidate(boolean isConsolidate) {this.isConsolidate = isConsolidate;}
	
	public String getPositionDataset() {return positionDataset;}
	public void setPositionDataset(String positionDataset) {this.positionDataset = positionDataset;}
	
	public String getPositionStatistics() {return positionStatistics;}
	public void setPositionStatistics(String positionStatistics) {this.positionStatistics = positionStatistics;}
}
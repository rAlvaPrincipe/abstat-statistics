package model;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

@Document
public class Metadata {

	@Id
	private String id;
	private String dataset;
	private String timestamp;
	private String position;

	private boolean isCalculateStatistics;
	private boolean isConsolidate;
	
	public Metadata(String id, String dataset, String timestamp, String position) {
		this.id = id;
		this.dataset = dataset;
		this.timestamp = timestamp;
		this.position = position;
		this.isCalculateStatistics = false;
		this.isConsolidate = false;
	}
	
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
	
	public String getPosition() {return position;}
	public void setPosition(String position) {this.position = position;}
}
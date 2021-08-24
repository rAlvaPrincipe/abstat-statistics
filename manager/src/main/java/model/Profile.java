package model;

import java.util.List;
import java.util.Map;

import org.springframework.data.mongodb.core.mapping.Document;

@Document
public class Profile{

	private String id;
	private String idDataset;
	private String datasetName;
	private String ontologyName;
	private long stat1;
	private Map<String, Double> stat2;
	private Map<String, Long> stat3;
	private List<String> stat4;
	
	
	public String getId() {return id;}
	public void setId(String id) {this.id = id;}
	
	public String getIdDataset() {return idDataset;}
	public void setIdDataset(String idDataset) {this.idDataset = idDataset;}
	
	public String getDatasetName() {return datasetName;}
	public void setDatasetName(String datasetName) {this.datasetName = datasetName;}
	
	public String getOntologyName() {return ontologyName;}
	public void setOntologyName(String ontologyName) {this.ontologyName = ontologyName;}
	
	public long getStat1() {return stat1;}
	public void setStat1(long stat1) {this.stat1 = stat1;}

	public Map<String, Double> getStat2() {return stat2;}
	public void setStat2(Map<String, Double> stat2) {this.stat2 = stat2;}

	public Map<String, Long> getStat3() {return stat3;}
	public void setStat3(Map<String, Long> stat3) {this.stat3 = stat3;}

	public List<String> getStat4() {return stat4;}
	public void setStat4(List<String> stat4) {this.stat4 = stat4;}	
}
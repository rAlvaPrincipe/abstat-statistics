package model;

import java.util.List;
import java.util.Map;

import org.springframework.data.mongodb.core.mapping.Document;

import com.fasterxml.jackson.annotation.JsonView;

@Document
public class Profile{
	
	@JsonView(View.Profiles.class)
	private String id;
	private String idDataset;
	@JsonView(View.Profiles.class)
	private String datasetName;
	private String ontologyName;
	private String statisticsPosition;
	private long bNodesObject;								
	private long bNodesSubject;
	private long outgoingLinks;
	private long incomingLinks;
	private long rdfsLabel;
	private long literalsWithType;
	private long literalsWithoutType;
	private double avgLengthLiterals;
	private long typedSubject;
	private long untypedSubject;
	private long rarePredicate;
	private Map<String, Double> sameAsLink;					
	private Map<String, Double> triplesEntity;
	private Map<String, Double> subjectPredicates;
	private Map<String, Double> subjectObject;
	private Map<String, Double> subjectCount;
	private Map<String, Double> objectCount;
	private Map<String, Double> subjectObjectRatio;
	private Map<String, Double> subjectPredicateRatio;
	private Map<String, Double> predicateObjectRatio;
	private Map<String, Double> countTypedSubject;
	private Map<String, Long> countConceptsPLD;				
	private Map<String, Long> countPropertiesPLD;
	private Map<String, Long> owlSameas;
	private List<Datatype> datatypes;		
	private List<Language> languages;
	private List<Vocabularie> vocabularies;
	private List<PredicateTriples> predicateTriples;
	private List<PredicateSubjects> predicateSubjects;
	private List<PredicateObjects> predicateObjects;
	
	static class Datatype {
		
		private String datatype;
        private long nDatatype;
        
        public String getDatatype() {return datatype;}
		public void setDatatype(String datatype) {this.datatype = datatype;}

		public long getnDatatype() {return nDatatype;}
		public void setnDatatype(long nDatatype) {this.nDatatype = nDatatype;}
	}
	
	static class Language {
		
		private String language;
        private long nLanguage;
        
		public String getLanguage() {return language;}
		public void setLanguage(String language) {this.language = language;}

		public long getnLanguage() {return nLanguage;}
		public void setnLanguage(long nLanguage) {this.nLanguage = nLanguage;}
	}
	
	static class Vocabularie {
		
		private String namespace;
        private long nPredicate;
        
		public String getNamespace() {return namespace;}
		public void setNamespace(String namespace) {this.namespace = namespace;}

		public long getnPredicate() {return nPredicate;}
		public void setnPredicate(long nPredicate) {this.nPredicate = nPredicate;}
	}
	
	static class PredicateTriples {
		
		private String predicate;
        private long nTriples;
        
		public String getPredicate() {return predicate;}
		public void setPredicate(String predicate) {this.predicate = predicate;}

		public long getnTriples() {return nTriples;}
		public void setnTriples(long nTriples) {this.nTriples = nTriples;}
	}
	
	static class PredicateSubjects {
		
		private String predicate;
        private long nSubjects;
        
		public String getPredicate() {return predicate;}
		public void setPredicate(String predicate) {this.predicate = predicate;}

		public long getnSubjects() {return nSubjects;}
		public void setnSubjects(long nSubjects) {this.nSubjects = nSubjects;}
	}
	
	static class PredicateObjects {
		
		private String predicate;
        private long nObjects;
        
		public String getPredicate() {return predicate;}
		public void setPredicate(String predicate) {this.predicate = predicate;}

		public long getnObjects() {return nObjects;}
		public void setnObjects(long nObjects) {this.nObjects = nObjects;}
	}

	public String getId() {return id;}
	public void setId(String id) {this.id = id;}
	
	public String getIdDataset() {return idDataset;}
	public void setIdDataset(String idDataset) {this.idDataset = idDataset;}
	
	public String getDatasetName() {return datasetName;}
	public void setDatasetName(String datasetName) {this.datasetName = datasetName;}
	
	public String getOntologyName() {return ontologyName;}
	public void setOntologyName(String ontologyName) {this.ontologyName = ontologyName;}
	
	public String getStatisticsPosition() {return statisticsPosition;}
	public void setStatisticsPosition(String statisticsPosition) {this.statisticsPosition = statisticsPosition;}
	
	public long getbNodesObject() {return bNodesObject;}
	public void setbNodesObject(long bNodesObject) {this.bNodesObject = bNodesObject;}
	
	public long getbNodesSubject() {return bNodesSubject;}
	public void setbNodesSubject(long bNodesSubject) {this.bNodesSubject = bNodesSubject;}
	
	public long getOutgoingLinks() {return outgoingLinks;}
	public void setOutgoingLinks(long outgoingLinks) {this.outgoingLinks = outgoingLinks;}
	
	public long getIncomingLinks() {return incomingLinks;}
	public void setIncomingLinks(long incomingLinks) {this.incomingLinks = incomingLinks;}
	
	public long getRdfsLabel() {return rdfsLabel;}
	public void setRdfsLabel(long rdfsLabel) {this.rdfsLabel = rdfsLabel;}
	
	public long getLiteralsWithType() {return literalsWithType;}
	public void setLiteralsWithType(long literalsWithType) {this.literalsWithType = literalsWithType;}
	
	public long getLiteralsWithoutType() {return literalsWithoutType;}
	public void setLiteralsWithoutType(long literalsWithoutType) {this.literalsWithoutType = literalsWithoutType;}

	public double getAvgLengthLiterals() {return avgLengthLiterals;}
	public void setAvgLengthLiterals(double avgLengthLiterals) {this.avgLengthLiterals = avgLengthLiterals;}
	
	public long getTypedSubject() {return typedSubject;}
	public void setTypedSubject(long typedSubject) {this.typedSubject = typedSubject;}
	
	public long getUntypedSubject() {return untypedSubject;}
	public void setUntypedSubject(long untypedSubject) {this.untypedSubject = untypedSubject;}
	
	public long getRarePredicate() {return rarePredicate;}
	public void setRarePredicate(long rarePredicate) {this.rarePredicate = rarePredicate;}
	
	public Map<String, Double> getSameAsLink() {return sameAsLink;}
	public void setSameAsLink(Map<String, Double> sameAsLink) {this.sameAsLink = sameAsLink;}
	
	public Map<String, Double> getTriplesEntity() {return triplesEntity;}
	public void setTriplesEntity(Map<String, Double> triplesEntity) {this.triplesEntity = triplesEntity;}
	
	public Map<String, Double> getSubjectPredicates() {return subjectPredicates;}
	public void setSubjectPredicates(Map<String, Double> subjectPredicates) {this.subjectPredicates = subjectPredicates;}
	
	public Map<String, Double> getSubjectObject() {return subjectObject;}
	public void setSubjectObject(Map<String, Double> subjectObject) {this.subjectObject = subjectObject;}
	
	public Map<String, Double> getSubjectCount() {return subjectCount;}
	public void setSubjectCount(Map<String, Double> subjectCount) {this.subjectCount = subjectCount;}
	
	public Map<String, Double> getObjectCount() {return objectCount;}
	public void setObjectCount(Map<String, Double> objectCount) {this.objectCount = objectCount;}
	
	public Map<String, Double> getSubjectObjectRatio() {return subjectObjectRatio;}
	public void setSubjectObjectRatio(Map<String, Double> subjectObjectRatio) {this.subjectObjectRatio = subjectObjectRatio;}
	
	public Map<String, Double> getSubjectPredicateRatio() {return subjectPredicateRatio;}
	public void setSubjectPredicateRatio(Map<String, Double> subjectPredicateRatio) {this.subjectPredicateRatio = subjectPredicateRatio;}
	
	public Map<String, Double> getPredicateObjectRatio() {return predicateObjectRatio;}
	public void setPredicateObjectRatio(Map<String, Double> predicateObjectRatio) {this.predicateObjectRatio = predicateObjectRatio;}
	
	public Map<String, Double> getCountTypedSubject() {return countTypedSubject;}
	public void setCountTypedSubject(Map<String, Double> countTypedSubject) {this.countTypedSubject = countTypedSubject;}
	
	public Map<String, Long> getCountConceptsPLD() {return countConceptsPLD;}
	public void setCountConceptsPLD(Map<String, Long> countConceptsPLD) {this.countConceptsPLD = countConceptsPLD;}
	
	public Map<String, Long> getCountPropertiesPLD() {return countPropertiesPLD;}
	public void setCountPropertiesPLD(Map<String, Long> countPropertiesPLD) {this.countPropertiesPLD = countPropertiesPLD;}
	
	public Map<String, Long> getOwlSameas() {return owlSameas;}
	public void setOwlSameas(Map<String, Long> owlSameas) {this.owlSameas = owlSameas;}
	
	public List<Datatype> getDatatypes() {return datatypes;}
	public void setDatatypes(List<Datatype> datatypes) {this.datatypes = datatypes;}
	
	public List<Language> getLanguages() {return languages;}
	public void setLanguages(List<Language> languages) {this.languages = languages;}
	
	public List<Vocabularie> getVocabularies() {return vocabularies;}
	public void setVocabularies(List<Vocabularie> vocabularies) {this.vocabularies = vocabularies;}
	
	public List<PredicateTriples> getPredicateTriples() {return predicateTriples;}
	public void setPredicateTriples(List<PredicateTriples> predicateTriples) {this.predicateTriples = predicateTriples;}
	
	public List<PredicateSubjects> getPredicateSubjects() {return predicateSubjects;}
	public void setPredicateSubjects(List<PredicateSubjects> predicateSubjects) {this.predicateSubjects = predicateSubjects;}
	
	public List<PredicateObjects> getPredicateObjects() {return predicateObjects;}
	public void setPredicateObjects(List<PredicateObjects> predicateObjects) {this.predicateObjects = predicateObjects;}
}
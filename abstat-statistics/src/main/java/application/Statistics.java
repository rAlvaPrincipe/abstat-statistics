package application;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Statistics {
	
	private SparkSession session;
	private String output_dir;
	private static String[] datasets;
	private String PLD;

	public Statistics(String master, String datasets, String output_dir, String PLD) {
		this.session = SparkSession.builder().appName("Java Spark SQL basic example").master(master).getOrCreate();
		this.session.sparkContext().setLogLevel("ERROR");
		this.datasets = datasets.split(";");
		this.PLD = PLD;
		this.output_dir = output_dir;
	}
	
	public static void main(String[] args) throws Exception {
		Statistics s = new Statistics(args[0], args[1], args[2], args[3]);
		Dataset<Row> peopleDF = s.session.read().format("json").load("/Users/filippocolombo/abstat-statistics/Datasets/prefixes_updated.json");
		peopleDF.createOrReplaceTempView("list");
		peopleDF.show(50, false);
		
		s.preProcessing(datasets);
	/*	s.countConceptsPLD();			
		s.countPropertiesPLD();			
		s.bNodesObject();				
		s.bNodesSubject(); 				
		s.datatype();  					
		s.countLanguage(); 				
		s.outgoingLinks();				
		s.incomingLinks();				
		s.rdfsLabel();					
		s.literalsWithType();			
		s.literalsWithoutType(); */	
		s.vocabularies();	
	/*	s.sameAsLink();					
		s.owlSameas();					
		s.lengthStringAndUntypedLiterals();				 
		s.typedSubjected();				
		s.untypedSubjected(); 			
		s.triplesEntity();		
		s.subjectPredicates(); 			
		s.subjectObject();				
		s.subjectCount(); 				
		s.objectCount();				
		s.predicateTriples();			
		s.predicateSubjects(); 			
		s.predicateObjects(); 			
		s.typedSubject();	*/			
	}
	
	public void preProcessing(String[] datasets) throws Exception {
		JavaRDD<String> input = session.read().textFile(datasets).javaRDD();
		JavaRDD<Triple> rdd = new Splitter().calculate(input);
		Dataset<Row> data = session.createDataFrame(rdd, Triple.class);
		data.createOrReplaceTempView("dataset");
		data.show(50, false);
	}
	
	//stat 4
	public void countConceptsPLD(){		
		session.sql("SELECT object "
				+ "FROM dataset "
				+ "WHERE predicate = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type' "
				+ "GROUP BY object ").createOrReplaceTempView("DistinctObject");
		
		session.sql("SELECT COUNT(object) AS WithPLD, (SELECT COUNT(object) "
														+ "FROM DistinctObject "
														+ "WHERE object NOT LIKE '%" +PLD+ "%') AS WithoutPLD "
					+ "FROM DistinctObject "
					+ "WHERE object LIKE '%" +PLD+ "%' ").write().option("sep", ";").csv(output_dir + "/CountConceptsPLD");
	}
	
	//stat 7
	public void countPropertiesPLD(){		
		session.sql("SELECT predicate "
				+ "FROM dataset "
				+ "GROUP BY predicate ").createOrReplaceTempView("DistinctPredicate");
		
		session.sql("SELECT COUNT(predicate) AS WithPLD, (SELECT COUNT(predicate) "
																	+ "FROM DistinctPredicate "
																	+ "WHERE predicate NOT LIKE '%" +PLD+ "%') AS WithoutPLD "
					+ "FROM DistinctPredicate "
					+ "WHERE predicate LIKE '%" +PLD+ "%' ").write().option("sep", ";").csv(output_dir + "/CountPropertiesPLD");
	}
	
	//stat 10
	public void bNodesObject() {	
		session.sql("SELECT COUNT (object) AS bNodesObject "
					+ "FROM dataset "
					+ "WHERE object LIKE '_:%' ").write().option("sep", ";").csv(output_dir + "/BNodesObject");
	}
	
	//stat 11
	public void bNodesSubject() {
		session.sql("SELECT COUNT (subject) AS bNodesSubject "
					+ "FROM dataset "
					+ "WHERE subject LIKE '_:%' ").write().option("sep", ";").csv(output_dir + "/BNodesSubject");
	}
	
	//stat 12
	public void datatype() {
		session.sql("SELECT datatype, COUNT(datatype) AS nDatatype  "
					+ "FROM dataset "
					+ "WHERE datatype is not null "
					+ "GROUP BY datatype "
					+ "ORDER BY nDatatype DESC").write().option("sep", ";").csv(output_dir + "/Datatype");
	}

	//stat 13
	public void countLanguage() {
		session.sql("SELECT language, COUNT(language) as nLanguage "
					+ "FROM (SELECT substring(object, -3, 3) AS language "
							+ "FROM dataset "
							+ "WHERE object NOT LIKE 'http://dbpedia%' "
							+ "AND object REGEXP '.*@[a-z][a-z]$' ) "
					+ "GROUP BY language "
					+ "ORDER BY nLanguage DESC").write().option("sep", ";").csv(output_dir + "/CountLanguage");
	} 
	
	//stat 14
	public void outgoingLinks() {
		session.sql("SELECT object, COUNT (object) "
					+ "FROM dataset "
					+ "WHERE subject LIKE '%" +PLD+ "%' "
					+ "AND object NOT LIKE '%" +PLD+ "%' "
					+ "GROUP BY object").show();
	}
	
	//stat 15
	public void incomingLinks() {
		session.sql("SELECT subject, COUNT (subject) "
					+ "FROM dataset"
					+ "WHERE object LIKE '%" +PLD+ "%' "
					+ "AND subject NOT LIKE '%" +PLD+ "%' "
					+ "GROUP BY subject ").show();
	}

	//stat 16
	public void rdfsLabel() {
		session.sql("SELECT COUNT(subject) as countLabel "
					+ "FROM dataset "
					+ "WHERE predicate = 'http://www.w3.org/2000/01/rdf-schema#label' ").write().option("sep", ";").csv(output_dir + "/RdfsLabel");
	}
	
	//stat 17
	public void literalsWithType() {	
		session.sql("SELECT COUNT(type) AS LiteralsWithType "
					+ "FROM dataset "
					+ "WHERE type = 'dt_relational' "
					+ "AND datatype is not null ").write().option("sep", ";").csv(output_dir + "/LiteralsWithType"); 
	}
	
	//stat 18
	public void literalsWithoutType() {	
		session.sql("SELECT COUNT(type) AS LiteralsWithoutType "
					+ "FROM dataset "
					+ "WHERE datatype is null "
					+ "AND type = 'dt_relational' ").write().option("sep", ";").csv(output_dir + "/LiteralsWithoutType"); 
	}

	//stat19
	public void vocabularies() {
		session.sql("SELECT namespace, COUNT (namespace) AS NPredicate "
					+ "FROM dataset JOIN list "
					+ "WHERE predicate LIKE CONCAT (namespace,'%') "
					+ "GROUP BY namespace ").show(200, false);
	}
	
	//stat 22
	public void sameAsLink() {
		session.sql("SELECT MIN(number), AVG(number), MAX(number), (SELECT COUNT(subject) "
																	+ "FROM dataset "
																	+ "WHERE predicate = 'http://www.w3.org/2002/07/owl#sameAs') AS NTriples  "
					+ "FROM (SELECT COUNT(subject) AS number "
							+ "FROM dataset "
							+ "WHERE predicate = 'http://www.w3.org/2002/07/owl#sameAs' "
							+ "GROUP BY subject) ").write().option("sep", ";").csv(output_dir + "/SameAsLink");
	}
	
	//stat 23
	public void owlSameas() {
		session.sql("SELECT COUNT(subject) AS WithOwlSemeas, (SELECT COUNT(subject) "
																+ "FROM dataset "
																+ "WHERE predicate != 'http://www.w3.org/2002/07/owl#sameAs') AS WithoutOwlSemeas "
					+ "FROM dataset "
					+ "WHERE predicate = 'http://www.w3.org/2002/07/owl#sameAs' ").write().option("sep", ";").csv(output_dir + "/OwlSameas");
	}
	
	//stat 24
	public void lengthStringAndUntypedLiterals() {
		session.sql("SELECT AVG(LENGTH(object)) AS AVGLengthLiterals "
					+ "FROM dataset "
					+ "WHERE type = 'dt_relational' "
					+ "AND datatype is null "
					+ "OR datatype = 'http://www.w3.org/2001/XMLSchema#string' ").write().option("sep", ";").csv(output_dir + "/LengthStringAndUntypedLiterals");
	}
	
	//stat 25
	public void typedSubjected() {
		session.sql("SELECT COUNT (DISTINCT subject) "
					+ "FROM dataset "
					+ "WHERE type = 'dt_relational' "
					+ "AND datatype is not null ").show();
	}
	
	//stat 26
	public void untypedSubjected() {
		session.sql("SELECT COUNT (DISTINCT subject) "
					+ "FROM dataset "
					+ "WHERE datatype is null "
					+ "AND type = 'dt_relational' ").show();
	}
	
	//stat 29
	public void triplesEntity() {
		session.sql("SELECT MIN(nTriples), AVG(nTriples), MAX(nTriples)"
					+ "FROM (SELECT COUNT (subject) AS nTriples "
							+ "FROM dataset "
							+ "WHERE predicate != 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type'"
							+ "AND type != 'dt_relational' "
							+ "GROUP BY subject) ").show();	
	}
	
	//stat 30
	public void subjectPredicates() {
		session.sql("SELECT MIN(nPredicate), AVG(nPredicate), MAX(nPredicate), STDDEV(nPredicate) "
					+ "FROM (SELECT COUNT (DISTINCT predicate) AS nPredicate "
							+ "FROM dataset " 
							+ "GROUP BY subject) ").write().option("sep", ";").csv(output_dir + "SubjectPredicates");
	}
	
	//stat 32
	public void subjectObject() {
		session.sql("SELECT MIN(nPredicate), AVG(nPredicate), MAX(nPredicate) "
					+ "FROM (SELECT COUNT (DISTINCT predicate) AS nPredicate "
							+ "FROM dataset " 
							+ "GROUP BY subject, object) ").write().option("sep", ";").csv(output_dir + "SubjectPredicates");
	}

	//stat 33
	public void subjectCount() {
		session.sql("SELECT MIN(number), AVG(number), MAX(number) "
					+ "FROM (SELECT COUNT (object) AS number "
							+ "FROM dataset "
							+ "WHERE predicate != 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type' "
							+ "GROUP BY subject) ").write().option("sep", ";").csv(output_dir + "/SubjectObject");
	}
	
	//stat 34
	public void objectCount() {
		session.sql("SELECT MIN(number), AVG(number), MAX(number) "
					+ "FROM (SELECT COUNT (subject) AS number "
							+ "FROM dataset "
							+ "WHERE predicate != 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type' "
							+ "GROUP BY object) ").write().option("sep", ";").csv(output_dir + "/ObjectCount");
	}
	
	//stat 35
	public void predicateTriples() {
		session.sql("SELECT predicate, COUNT (predicate) AS nTriples "
					+ "FROM dataset " 
					+ "GROUP BY predicate "
					+ "ORDER BY nTriples DESC").write().option("sep", ";").csv(output_dir + "/PredicateTriples");
	}
	
	//stat 36
	public void predicateSubjects() {
		session.sql("SELECT predicate, COUNT (DISTINCT subject) AS nSubjects "
					+ "FROM dataset " 
					+ "GROUP BY predicate "
					+ "ORDER BY nSubjects DESC ").write().option("sep", ";").csv(output_dir + "/PredicateSubjects");
	}

	//stat 37
	public void predicateObjects() {
		session.sql("SELECT predicate, COUNT (DISTINCT object) AS nObjects "
					+ "FROM dataset "
					+ "WHERE predicate != 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type' "     
					+ "GROUP BY predicate "
					+ "ORDER BY nObjects DESC ").write().option("sep", ";").csv(output_dir + "/PredicateObjects"); 
	}
	
	//stat 46
	public void typedSubject() {
		session.sql("SELECT MIN(nSubject), AVG(nSubject), MAX(nSubject) "
					+ "FROM (SELECT COUNT(subject) as nSubject "
							+ "FROM dataset "
							+ "WHERE predicate = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type' "
							+ "GROUP BY subject) ").write().option("sep", ";").csv(output_dir + "/TypedSubject"); 
	}
}
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
	private static String prefix;

	public Statistics(String master, String datasets, String output_dir, String PLD, String prefix) {
		this.session = SparkSession.builder().appName("Java Spark SQL basic example").master(master).getOrCreate();
		this.session.sparkContext().setLogLevel("ERROR");
		Statistics.datasets = datasets.split(";");
		this.PLD = PLD;
		this.output_dir = output_dir;
		Statistics.prefix = prefix;
	}
	
	public static void main(String[] args) throws Exception {
		Statistics s = new Statistics(args[0], args[1], args[2], args[3], args[4]);
		Dataset<Row> prefixes = s.session.read().format("json").load(prefix);
		prefixes.createOrReplaceTempView("list");
		prefixes.show(50, false);
		
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
		s.nTypedSubject();				
		s.nUntypedSubject(); 		
		s.triplesEntity();		
		s.subjectPredicates(); 			
		s.subjectObject();				
		s.subjectCount(); 				
		s.objectCount();				
		s.predicateTriples();			
		s.predicateSubjects(); 			
		s.predicateObjects(); 
		s.subjectObjectRatio();
		s.subjectPredicateRatio();
		s.predicateObjectRatio(); 
		s.distinctList();
		s.rarePredicate();
		s.predicateList();
		s.typedSubject();		*/
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
		session.sql("SELECT COUNT (predicate) "
					+ "FROM dataset "
					+ "WHERE subject LIKE '%" +PLD+ "%' "
					+ "AND object NOT LIKE '%" +PLD+ "%' "
					+ "AND type = 'object_relational' ").write().option("sep", ";").csv(output_dir + "/OutgoingLinks");
	}
	
	//stat 15
	public void incomingLinks() {
		session.sql("SELECT COUNT (predicate) "
					+ "FROM dataset "
					+ "WHERE object LIKE '%" +PLD+ "%' "
					+ "AND subject NOT LIKE '%" +PLD+ "%' "
					+ "AND type = 'object_relational' ").write().option("sep", ";").csv(output_dir + "/IncomingLinks");
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

	//stat 19
	public void vocabularies() {
		session.sql("SELECT namespace, COUNT (namespace) AS NPredicate "
					+ "FROM dataset JOIN list "
					+ "WHERE predicate LIKE CONCAT (namespace,'%')"
					+ "OR subject LIKE CONCAT (namespace,'%')  "
					+ "OR object LIKE CONCAT (namespace,'%') "
					+ "GROUP BY namespace ").write().option("sep", ";").csv(output_dir + "/Vocabularies"); 
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
	public void nTypedSubject() {
		session.sql("SELECT COUNT (DISTINCT subject) "
					+ "FROM dataset "
					+ "WHERE predicate = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type' ").write().option("sep", ";").csv(output_dir + "/nTypedSubject");
	}
	
	//stat 26
	public void nUntypedSubject() {
		session.sql("SELECT COUNT (DISTINCT subject) "
					+ "FROM dataset "
					+ "WHERE subject NOT IN (SELECT subject "
											+ "FROM dataset "
											+ "WHERE predicate = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type') ").write().option("sep", ";").csv(output_dir + "/nUntypedSubject");
	}
	
	//stat 29
	public void triplesEntity() {
		session.sql("SELECT MIN(nTriples), AVG(nTriples), MAX(nTriples)"
					+ "FROM (SELECT COUNT (subject) AS nTriples "
							+ "FROM dataset "
							+ "WHERE type = 'dt_relational' "
							+ "OR type = 'object_relational' "
							+ "GROUP BY subject) ").write().option("sep", ";").csv(output_dir + "/TriplesEntity");
	}
	
	//stat 30
	public void subjectPredicates() {
		session.sql("SELECT MIN(nPredicate), AVG(nPredicate), MAX(nPredicate), STDDEV(nPredicate) "
					+ "FROM (SELECT COUNT (DISTINCT predicate) AS nPredicate "
							+ "FROM dataset " 
							+ "GROUP BY subject) ").write().option("sep", ";").csv(output_dir + "/SubjectPredicates");
	}
	
	//stat 32
	public void subjectObject() {
		session.sql("SELECT MIN(nPredicate), AVG(nPredicate), MAX(nPredicate) "
					+ "FROM (SELECT COUNT (DISTINCT predicate) AS nPredicate "
							+ "FROM dataset " 
							+ "GROUP BY subject, object) ").write().option("sep", ";").csv(output_dir + "/SubjectPredicates");
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
	
	//stat 38
	public void subjectObjectRatio() {
		session.sql("SELECT subject, COUNT(subject) AS nSubject "
					+ "FROM dataset "
					+ "GROUP BY subject ").createOrReplaceTempView("Subject");
			
		session.sql("SELECT object, COUNT(object) AS nObject "
					+ "FROM dataset "
					+ "GROUP BY object ").createOrReplaceTempView("Object");
			
		session.sql("SELECT MIN(number), AVG(number), MAX(number) "
					+ "FROM (SELECT nSubject/nObject AS number "
							+ "FROM Subject, Object "
							+ "WHERE subject = object) ").show(100, false);
	}
		
	//stat 39
	public void subjectPredicateRatio() {
		session.sql("SELECT subject, COUNT(subject) AS nSubject "
					+ "FROM dataset "
					+ "GROUP BY subject ").createOrReplaceTempView("Subject");
			
		session.sql("SELECT predicate, COUNT(predicate) AS nPredicate "
					+ "FROM dataset "
					+ "GROUP BY predicate ").createOrReplaceTempView("Predicate");
			
		session.sql("SELECT MIN(number), AVG(number), MAX(number) "
					+ "FROM (SELECT nSubject/nPredicate AS number "
							+ "FROM Subject, Predicate "
							+ "WHERE subject = predicate) ").show(100, false);
	}
		
	//stat 40
	public void predicateObjectRatio() {
		session.sql("SELECT predicate, COUNT(predicate) AS nPredicate "
					+ "FROM dataset "
					+ "GROUP BY predicate ").createOrReplaceTempView("Predicate");
		
		session.sql("SELECT object, COUNT(object) AS nObject "
					+ "FROM dataset "
					+ "GROUP BY object ").createOrReplaceTempView("Object");
			
		session.sql("SELECT MIN(number), AVG(number), MAX(number) "
					+ "FROM (SELECT nPredicate/nObject AS number "
							+ "FROM Predicate, Object "
							+ "WHERE predicate = object) ").show(100, false);
	}
		
	//stat 41
	public void distinctList() {
		session.sql("SELECT predicate, COUNT(predicate) "
					+ "FROM dataset "
					+ "GROUP BY predicate "
					+ "HAVING COUNT(predicate) = 1" ).createOrReplaceTempView("rare");
			
		session.sql("SELECT COUNT (DISTINCT subject) AS nDistinctListPredicate "
					+ "FROM dataset, rare "
					+ "WHERE rare.predicate = dataset.predicate ").show(100, false);
	}

	//stat 43
	public void rarePredicate() {
		session.sql("SELECT COUNT(predicate) AS RarePradicate "
					+ "FROM (SELECT predicate "
							+ "FROM dataset "
							+ "GROUP BY predicate "
							+ "HAVING COUNT(predicate) = 1)" ).show(100, false);
		}
	
	//stat 44
	public void predicateList() {
		session.sql("SELECT MIN(number), AVG(number), MAX(number) "
					+ "FROM (SELECT subject, COUNT(predicate) AS number "
							+ "FROM dataset "
							+ "GROUP BY subject) ").show(100, false);
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
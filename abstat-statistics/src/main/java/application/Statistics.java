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
		s.preProcessing(datasets);
	/*	s.countConceptsPLD();
		s.countPropertiesPLD();
		s.bNodesObject();
		s.bNodesSubject(); 
		s.datatype();  
		s.countLanguage(); 
		s.rdfsLabel(); 
		s.owlSameas(); 
		s.subjectCount(); 
		s.objectCount(); 
		s.predicateTriples(); 
		s.predicateSubjects(); 
		s.predicateObjects(); 
		s.predicateObjects();  */
		s.literalsWithType();
		s.literalsWithoutType();
		s.lengthLiterals(); 
	/*	s.sameAsLink();  
		s.subjectPredicates(); 
		s.subjectObject(); */
	//	s.typedSubject(); 
	}
	
	
	public void preProcessing(String[] datasets) throws Exception {
		JavaRDD<String> input = session.read().textFile(datasets).javaRDD();
		JavaRDD<Triple> rdd = new Splitter().calculate(input);
		Dataset<Row> data = session.createDataFrame(rdd, Triple.class);
		data.createOrReplaceTempView("dataset");
		data.show(30, false);
	}
	

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
	
	
	public void bNodesObject() {	
		session.sql("SELECT COUNT (object) AS bNodesObject "
					+ "FROM dataset "
					+ "WHERE object LIKE '_:%' ").write().option("sep", ";").csv(output_dir + "/BNodesObject");
	}
	
	
	public void bNodesSubject() {
		session.sql("SELECT COUNT (subject) AS bNodesSubject "
					+ "FROM dataset "
					+ "WHERE subject LIKE '_:%' ").write().option("sep", ";").csv(output_dir + "/BNodesSubject");
	}
	

	public void datatype() {
		session.sql("SELECT datatype, COUNT(datatype) AS nDatatype  "
					+ "FROM dataset "
					+ "WHERE datatype is not null "
					+ "GROUP BY datatype "
					+ "ORDER BY nDatatype DESC").write().option("sep", ";").csv(output_dir + "/Datatype");
	}


	public void countLanguage() {
		session.sql("SELECT language, COUNT(language) as nLanguage "
					+ "FROM (SELECT substring(object, -3, 3) AS language "
							+ "FROM dataset "
							+ "WHERE object NOT LIKE 'http://dbpedia%' "
							+ "AND object REGEXP '.*@[a-z][a-z]$' ) "
					+ "GROUP BY language "
					+ "ORDER BY nLanguage DESC").write().option("sep", ";").csv(output_dir + "/CountLanguage");
	} 

	
	public void rdfsLabel() {
		session.sql("SELECT COUNT(subject) as countLabel "
					+ "FROM dataset "
					+ "WHERE predicate = 'http://www.w3.org/2000/01/rdf-schema#label' ").write().option("sep", ";").csv(output_dir + "/RdfsLabel");
	}
	
	
	public void owlSameas() {
		session.sql("SELECT COUNT(subject) AS WithOwlSemeas, (SELECT COUNT(subject) "
															+ "FROM dataset "
															+ "WHERE predicate != 'http://www.w3.org/2002/07/owl#sameAs') AS WithoutOwlSemeas "
					+ "FROM dataset "
					+ "WHERE predicate = 'http://www.w3.org/2002/07/owl#sameAs' ").write().option("sep", ";").csv(output_dir + "/OwlSameas");
	}
	

	public void subjectCount() {
		session.sql("SELECT MIN(number), AVG(number), MAX(number) "
					+ "FROM (SELECT COUNT (object) AS number "
							+ "FROM dataset "
							+ "WHERE predicate != 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type' "
							+ "GROUP BY subject) ").show();		//.write().option("sep", ";").csv(output_dir + "/SubjectCount");
	}
	

	public void objectCount() {
		session.sql("SELECT MIN(number), AVG(number), MAX(number) "
					+ "FROM (SELECT COUNT (subject) AS number "
							+ "FROM dataset "
							+ "WHERE predicate != 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type' "
							+ "GROUP BY object) ").show();				//.write().option("sep", ";").csv(output_dir + "/ObjectCount");
	}
	

	public void predicateTriples() {
		session.sql("SELECT predicate, COUNT (predicate) AS nTriples "
					+ "FROM dataset " 
					+ "GROUP BY predicate "
					+ "ORDER BY nTriples DESC").write().option("sep", ";").csv(output_dir + "/PredicateTriples");
	}
	

	public void predicateSubjects() {
		session.sql("SELECT predicate, COUNT (DISTINCT subject) AS nSubjects "
					+ "FROM dataset " 
					+ "GROUP BY predicate "
					+ "ORDER BY nSubjects DESC ").write().option("sep", ";").csv(output_dir + "/PredicateSubjects");
	}

	
	public void predicateObjects() {
		session.sql("SELECT predicate, COUNT (DISTINCT object) AS nObjects "
					+ "FROM dataset "
					+ "WHERE predicate != 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type' "     
					+ "GROUP BY predicate "
					+ "ORDER BY nObjects DESC ").write().option("sep", ";").csv(output_dir + "/PredicateObjects"); 
	}
	
	//guardare colonna datatype, nulla da matchare
	public void literalsWithType() {	
		session.sql("SELECT COUNT(type) AS LiteralsWithType "
					+ "FROM dataset "
					+ "WHERE type = 'dt_relational' "
					+ "AND datatype is not null ").show();
	}
	
	
	public void literalsWithoutType() {	
		session.sql("SELECT COUNT(type) AS LiteralsWithoutType "
					+ "FROM dataset "
					+ "WHERE datatype is null "
					+ "AND type = 'dt_relational' ").show();
	}
	
	
	public void lengthLiterals() {
		session.sql("SELECT AVG(LENGTH(object)) AS AVGLengthLiterals "
					+ "FROM dataset "
					+ "WHERE type = 'dt_relational' "
					+ "AND datatype is null ").show();
	}
	
	//provare con group by senza query innestata
	public void sameAsLink() {
		session.sql("SELECT MIN(number), AVG(number), MAX(number) "
					+ "FROM (SELECT COUNT(subject) AS number "
							+ "FROM dataset "
							+ "WHERE predicate = 'http://www.w3.org/2002/07/owl#sameAs') ").show();
	}
	
	//devo considerare anche rdf:Type?
	public void subjectPredicates() {
		session.sql("SELECT MIN(nPredicate), AVG(nPredicate), MAX(nPredicate) "
					+ "FROM (SELECT COUNT (DISTINCT predicate) AS nPredicate "
							+ "FROM dataset " 
							+ "GROUP BY subject) ").show(20, false);
	}
	
	//vedere se si può fare con group by
	public void subjectObject() {
		
		//SENZA GROUP BY
		session.sql("SELECT MIN(nPredicate), AVG(nPredicate), MAX(nPredicate) "
					+ "FROM (SELECT COUNT (DISTINCT predicate) AS nPredicate "
							+ "FROM dataset " 
							+ "GROUP BY subject, object) ").show(20, false);
		
		session.sql("SELECT subject, object, COUNT (DISTINCT predicate) AS nPredicate "
					+ "FROM dataset " 
					+ "GROUP BY subject, object "
					+ "ORDER BY nPredicate DESC").show(100, false);
		
		//CON GROUP BY
		session.sql("SELECT MIN(nPredicate), AVG(nPredicate), MAX(nPredicate) "
					+ "FROM (SELECT COUNT (predicate) AS nPredicate "
						+ "FROM dataset " 
						+ "GROUP BY subject, object, predicate) ").show(20, false);
		
		session.sql("SELECT subject, object, COUNT (predicate) AS nPredicate "
					+ "FROM dataset "
					+ "GROUP BY subject, object, predicate "
					+ "ORDER BY nPredicate DESC ").show(100, false);

	}
	
	
	public void typedSubject() {
		session.sql("SELECT MIN(nSubject), AVG(nSubject), MAX(nSubject) "
				+ "FROM (SELECT COUNT(subject) as nSubject "
					+ "FROM dataset "
					+ "WHERE predicate = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type' "
					+ "GROUP BY subject) ").show(20, false);
	}
}
package application;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class Statistics {
	
	private SparkSession session;
	private String output_dir;
	private static String[] datasets;
	private static String PLD;

	public Statistics(String master, String output_dir) {
		this.session = SparkSession.builder().appName("Java Spark SQL basic example").master(master).getOrCreate();
		this.session.sparkContext().setLogLevel("ERROR");
		this.output_dir = output_dir;
	}
	
	
	public static void main(String[] args) throws Exception {
		Statistics s = new Statistics(args[0], args[2]);
		datasets = args[1].split(";");
		PLD = args[3];
		s.preProcessing(datasets);
		s.countConceptsPLD();
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
	}
	
	
	public void preProcessing(String[] datasets) throws Exception {
		JavaRDD<String> input = session.read().textFile(datasets).javaRDD();
		JavaRDD<Triple> rdd = new Splitter().calculate(input);
		Dataset<Row> data = session.createDataFrame(rdd, Triple.class);
		data.createOrReplaceTempView("dataset");
		data.show(40);
	}
	
	//contains invece di =
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
		//dbpedia.org/ontology   
	}
	
	//contains invece di =
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
	
	//okay but parser does not support blank node
	public void bNodesObject() {	
		session.sql("SELECT COUNT (object) AS bNodesObject "
					+ "FROM dataset "
					+ "WHERE object LIKE '_:%' ").write().option("sep", ";").csv(output_dir + "/BNodesObject");
	}
	
	//okay but parser does not support blank node	
	public void bNodesSubject() {
		session.sql("SELECT COUNT (subject) AS bNodesSubject "
					+ "FROM dataset "
					+ "WHERE subject LIKE '_:%' ").write().option("sep", ";").csv(output_dir + "/BNodesSubject");
	}
	
	//non contare NULL
	public void datatype() {
		session.sql("SELECT datatype, COUNT(datatype) AS nDatatype  "
					+ "FROM dataset "
					+ "WHERE datatype != 'null' "
					+ "GROUP BY datatype "
					+ "ORDER BY nDatatype DESC").write().option("sep", ";").csv(output_dir + "/Datatype");
	}

	//split language from name, only language
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
	
	//da aggiungere filtraggio, è diventata una statistica da 1.5	
	public void subjectCount() {
		session.sql("SELECT MIN(number), AVG(number), MAX(Number) "
					+ "FROM (SELECT COUNT (object) AS number "
							+ "FROM dataset "
							+ "WHERE predicate != 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type' "
							+ "GROUP BY subject) ").write().option("sep", ";").csv(output_dir + "/SubjectCount");
	}
	
	//da aggiungere filtraggio, è diventata una statistica da 1.5
	public void objectCount() {
		session.sql("SELECT MIN(number), AVG(number), MAX(Number) "
					+ "FROM (SELECT COUNT (subject) AS number "
							+ "FROM dataset "
							+ "WHERE predicate != 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type' "
							+ "GROUP BY object) ").write().option("sep", ";").csv(output_dir + "/ObjectCount");
	}
	
	//correggere togliendo l'input da tastiera	
	public void predicateTriples() {
		session.sql("SELECT predicate, COUNT (predicate) AS nTriples "
					+ "FROM dataset " 
					+ "GROUP BY predicate "
					+ "ORDER BY nTriples DESC").write().option("sep", ";").csv(output_dir + "/PredicateTriples");
	}
	
	//correggere togliendo l'input da tastiera	
	public void predicateSubjects() {
		session.sql("SELECT predicate, COUNT (DISTINCT subject) AS nSubjects "
					+ "FROM dataset " 
					+ "GROUP BY predicate "
					+ "ORDER BY nSubjects DESC ").write().option("sep", ";").csv(output_dir + "/PredicateSubjects");
	}

	//correggere togliendo l'input da tastiera e aggiungere filtraggio
	public void predicateObjects() {
		session.sql("SELECT predicate, COUNT (DISTINCT object) AS nObjects "
					+ "FROM dataset "
					+ "WHERE predicate != 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type' "     
					+ "GROUP BY predicate "
					+ "ORDER BY nObjects DESC ").write().option("sep", ";").csv(output_dir + "/PredicateObjects"); 
	}
}
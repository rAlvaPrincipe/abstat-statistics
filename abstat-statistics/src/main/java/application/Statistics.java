package application;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class Statistics {
	
	private SparkSession session;
	private String output_dir;
	private static String[] datasets;
	private String PLD;
	private String namespaces = "./abstat-statistics/namespaces.json";
	static HashMap<String,ArrayList<String>> dir=new HashMap<String,ArrayList<String>>();

	public Statistics(String master, String datasets, String output_dir, String PLD) {
		this.session = SparkSession.builder().appName("Java Spark SQL basic example").master(master).getOrCreate();
		this.session.sparkContext().setLogLevel("ERROR");
		Statistics.datasets = datasets.split(";");
		this.PLD = PLD;
		this.output_dir = output_dir;
	}
	
	public static void main(String[] args) throws Exception {
		Statistics s = new Statistics(args[0], args[1], args[2], args[3]);
		Dataset<Row> prefixes = s.session.read().format("json").load(s.namespaces);
		prefixes.createOrReplaceTempView("list");
		
		s.preProcessing(datasets);
		s.countConceptsPLD();			
		s.countPropertiesPLD();
		s.bNodesObject();				
		s.bNodesSubject(); 		
		s.datatype(); 				
		s.countLanguage(); 			
		s.outgoingLinks();				
		s.incomingLinks();				
		s.rdfsLabel();					
		s.literalsWithType();			
		s.literalsWithoutType(); 
		s.vocabularies(); 	
		s.sameAsLink();					
		s.owlSameas();					
		s.lengthStringAndUntypedLiterals();			 
		s.typedSubject();				
		s.untypedSubject(); 		
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
		s.rarePredicate();	
		s.countTypedSubject();
		s.mergedAndDeleteFolder();
	}
	
	public void preProcessing(String[] datasets) throws Exception {
		JavaRDD<String> input = session.read().textFile(datasets).javaRDD();
		JavaRDD<Triple> rdd = new Splitter().calculate(input);
		Dataset<Row> data = session.createDataFrame(rdd, Triple.class);
		data.createOrReplaceTempView("dataset");
		//data.show(50, false);
		new BuildJSON(session).fakeTable();
	}
	
	//stat 4
	public void countConceptsPLD(){	
		
		String like = "";
		String not_like = "";
		String[] PLDs = PLD.split(" ");
		for (int i=0; i< PLDs.length; i++){
			if(i==0){
				not_like += "WHERE object NOT LIKE '%" +PLDs[i]+ "%' ";
				like += "WHERE object NOT LIKE '%" +PLDs[i]+ "%' ";
			}
			else {
				not_like += "OR object  NOT LIKE  '%" +PLDs[i]+ "%' ";
				like += "OR object LIKE  '%" +PLDs[i]+ "%' ";
			}
		}

		session.sql("SELECT object "
					+ "FROM dataset "
					+ "WHERE predicate = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type' "
					+ "GROUP BY object ").createOrReplaceTempView("DistinctObject");
			
		session.sql("SELECT COUNT(object) AS withPLD, (SELECT COUNT(object) "
													+ "FROM DistinctObject "
													+ not_like + ") AS withoutPLD "
					+ "FROM DistinctObject "
					+ like).write().option("sep", ";").csv(output_dir + "/countConceptsPLD");
		
		dir.put("countConceptsPLD", new ArrayList<String>(Arrays.asList("countConceptsPLD", "withPLD", "withoutPLD")));
		new BuildJSON(session).withAndWithout(output_dir, dir.get("countConceptsPLD"));
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
					+ "WHERE predicate LIKE '%" +PLD+ "%' ").write().option("sep", ";").csv(output_dir + "/countPropertiesPLD");
		
		dir.put("countPropertiesPLD", new ArrayList<String>(Arrays.asList("countPropertiesPLD", "withPLD", "withoutPLD")));
		new BuildJSON(session).withAndWithout(output_dir, dir.get("countPropertiesPLD"));
	}
	
	//stat 10
	public void bNodesObject() {	
		session.sql("SELECT COUNT (object) AS nBNodesObject "
					+ "FROM dataset "
					+ "WHERE object LIKE '_:%' ").write().option("sep", ";").csv(output_dir + "/bNodesObject");

		dir.put("bNodesObject", new ArrayList<String>(Arrays.asList("bNodesObject", "long")));
		new BuildJSON(session).oneElement(output_dir, dir.get("bNodesObject"));
	}
	
	//stat 11
	public void bNodesSubject() {
		session.sql("SELECT COUNT (subject) AS nBNodesSubject "
					+ "FROM dataset "
					+ "WHERE subject LIKE '_:%' ").write().option("sep", ";").csv(output_dir + "/bNodesSubject");
		
		dir.put("bNodesSubject", new ArrayList<String>(Arrays.asList("bNodesSubject", "long")));
		new BuildJSON(session).oneElement(output_dir, dir.get("bNodesSubject"));
	}
	
	//stat 12
	public void datatype() {
		session.sql("SELECT datatype, COUNT(datatype) AS nDatatype  "
					+ "FROM dataset "
					+ "WHERE datatype is not null "
					+ "GROUP BY datatype "
					+ "ORDER BY nDatatype DESC").write().option("sep", ";").csv(output_dir + "/datatypes");
		
		dir.put("datatypes", new ArrayList<String>(Arrays.asList("datatypes", "datatype", "nDatatype")));
		new BuildJSON(session).number(output_dir, dir.get("datatypes"));
	}

	//stat 13
	public void countLanguage() {
		session.sql("SELECT language, COUNT(language) as nLanguage "
					+ "FROM (SELECT substring(object, -3, 3) AS language "
							+ "FROM dataset "
							+ "WHERE object NOT LIKE 'http://dbpedia%' "
							+ "AND object REGEXP '.*@[a-z][a-z]$' ) "
					+ "GROUP BY language "
					+ "ORDER BY nLanguage DESC").write().option("sep", ";").csv(output_dir + "/languages");
		
		dir.put("languages", new ArrayList<String>(Arrays.asList("languages", "language", "nLanguage")));
		new BuildJSON(session).number(output_dir, dir.get("languages"));
	} 
	
	//stat 14
	public void outgoingLinks() {
		session.sql("SELECT COUNT (predicate) AS nOutgoingLinks "
					+ "FROM dataset "
					+ "WHERE subject LIKE '%" +PLD+ "%' "
					+ "AND object NOT LIKE '%" +PLD+ "%' "
					+ "AND type = 'object_relational' ").write().option("sep", ";").csv(output_dir + "/outgoingLinks");
		
		dir.put("outgoingLinks", new ArrayList<String>(Arrays.asList("outgoingLinks", "long")));
		new BuildJSON(session).oneElement(output_dir, dir.get("outgoingLinks"));
	}
	
	//stat 15
	public void incomingLinks() {
		session.sql("SELECT COUNT (predicate) AS nIncomingLinks "
					+ "FROM dataset "
					+ "WHERE object LIKE '%" +PLD+ "%' "
					+ "AND subject NOT LIKE '%" +PLD+ "%' "
					+ "AND type = 'object_relational' ").write().option("sep", ";").csv(output_dir + "/incomingLinks");
		
		dir.put("incomingLinks", new ArrayList<String>(Arrays.asList("incomingLinks", "long")));
		new BuildJSON(session).oneElement(output_dir, dir.get("incomingLinks"));
	}

	//stat 16
	public void rdfsLabel() {
		session.sql("SELECT COUNT(subject) AS nRdfsLabel "
					+ "FROM dataset "
					+ "WHERE predicate = 'http://www.w3.org/2000/01/rdf-schema#label' ").write().option("sep", ";").csv(output_dir + "/rdfsLabel");
		
		dir.put("rdfsLabel", new ArrayList<String>(Arrays.asList("rdfsLabel", "long")));
		new BuildJSON(session).oneElement(output_dir, dir.get("rdfsLabel"));
	}
	
	//stat 17
	public void literalsWithType() {	
		session.sql("SELECT COUNT(type) AS nLiteralsWithType "
					+ "FROM dataset "
					+ "WHERE type = 'dt_relational' "
					+ "AND datatype is not null ").write().option("sep", ";").csv(output_dir + "/literalsWithType");
		
		dir.put("literalsWithType", new ArrayList<String>(Arrays.asList("literalsWithType", "long")));
		new BuildJSON(session).oneElement(output_dir, dir.get("literalsWithType"));
	}
	
	//stat 18
	public void literalsWithoutType() {	
		session.sql("SELECT COUNT(type) AS nLiteralsWithoutType "
					+ "FROM dataset "
					+ "WHERE datatype is null "
					+ "AND type = 'dt_relational' ").write().option("sep", ";").csv(output_dir + "/literalsWithoutType");
		
		dir.put("literalsWithoutType", new ArrayList<String>(Arrays.asList("literalsWithoutType", "long")));
		new BuildJSON(session).oneElement(output_dir, dir.get("literalsWithoutType"));
	}

	//stat 19
	public void vocabularies() {
		session.sql("SELECT namespace, COUNT (namespace) AS NPredicate "
										+ "FROM dataset JOIN list "
										+ "WHERE predicate LIKE CONCAT (namespace,'%')"
										+ "OR subject LIKE CONCAT (namespace,'%')  "
										+ "OR object LIKE CONCAT (namespace,'%') "
										+ "GROUP BY namespace ").write().option("sep", ";").csv(output_dir + "/vocabularies");
		
		dir.put("vocabularies", new ArrayList<String>(Arrays.asList("vocabularies", "namespace", "nPredicate")));
		new BuildJSON(session).number(output_dir, dir.get("vocabularies"));
	}
	
	//stat 22
	public void sameAsLink() {
		session.sql("SELECT MIN(number) AS min, AVG(number) AS avg, MAX(number) AS max, (SELECT COUNT(subject) "
																						+ "FROM dataset "
																						+ "WHERE predicate = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type' ) AS triple "
					+ "FROM (SELECT COUNT(subject) AS number "
							+ "FROM dataset "
							+ "WHERE predicate = 'http://www.w3.org/2002/07/owl#sameAs' "
							+ "GROUP BY subject) ").write().option("sep", ";").csv(output_dir + "/sameAsLink");
		
		dir.put("sameAsLink", new ArrayList<String>(Arrays.asList("sameAsLink", "triple")));
		new BuildJSON(session).minMaxAvgOther(output_dir, dir.get("sameAsLink"));
	}
	
	//stat 23
	public void owlSameas() {
		session.sql("SELECT COUNT(subject) AS withOwlSemeas, (SELECT COUNT(subject) "
																+ "FROM dataset "
																+ "WHERE predicate != 'http://www.w3.org/2002/07/owl#sameAs') AS withoutOwlSemeas "
					+ "FROM dataset "
					+ "WHERE predicate = 'http://www.w3.org/2002/07/owl#sameAs' ").write().option("sep", ";").csv(output_dir + "/owlSameas");
		
		dir.put("owlSameas", new ArrayList<String>(Arrays.asList("owlSameas", "withOwlSemeas", "withoutOwlSemeas")));
		new BuildJSON(session).withAndWithout(output_dir, dir.get("owlSameas"));
	}
	
	//stat 24
	public void lengthStringAndUntypedLiterals() {
		session.sql("SELECT AVG(LENGTH(object)) AS AVGLengthLiterals "
					+ "FROM dataset "
					+ "WHERE type = 'dt_relational' "
					+ "AND datatype is null "
					+ "OR datatype = 'http://www.w3.org/2001/XMLSchema#string' ").write().option("sep", ";").csv(output_dir + "/avgLengthLiterals");
		
		dir.put("avgLengthLiterals", new ArrayList<String>(Arrays.asList("avgLengthLiterals", "double")));
		new BuildJSON(session).oneElement(output_dir, dir.get("avgLengthLiterals"));
	}
	
	//stat 25
	public void typedSubject() {
		session.sql("SELECT COUNT (DISTINCT subject) AS nTypedSubject "
					+ "FROM dataset "
					+ "WHERE predicate = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type' ").write().option("sep", ";").csv(output_dir + "/typedSubject");
		
		dir.put("typedSubject", new ArrayList<String>(Arrays.asList("typedSubject", "long")));
		new BuildJSON(session).oneElement(output_dir, dir.get("typedSubject"));
	}
	
	//stat 26
	public void untypedSubject() {
		session.sql("SELECT COUNT (DISTINCT subject) AS nUntypedSubject "
					+ "FROM dataset "
					+ "WHERE subject NOT IN (SELECT subject "
											+ "FROM dataset "
											+ "WHERE predicate = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type') ").write().option("sep", ";").csv(output_dir + "/untypedSubject");
		
		dir.put("untypedSubject", new ArrayList<String>(Arrays.asList("untypedSubject", "long")));
		new BuildJSON(session).oneElement(output_dir, dir.get("untypedSubject"));
	}
	
	//stat 29
	public void triplesEntity() {
		session.sql("SELECT MIN(nTriples) AS min, AVG(nTriples) AS avg, MAX(nTriples) AS max "
					+ "FROM (SELECT COUNT (subject) AS nTriples "
							+ "FROM dataset "
							+ "WHERE type = 'dt_relational' "
							+ "OR type = 'object_relational' "
							+ "GROUP BY subject) ").write().option("sep", ";").csv(output_dir + "/triplesEntity");
		
		dir.put("triplesEntity", new ArrayList<String>(Arrays.asList("triplesEntity")));
		new BuildJSON(session).minMaxAvg(output_dir, dir.get("triplesEntity"));
	}
	
	//stat 30
	public void subjectPredicates() {
		session.sql("SELECT MIN(nPredicate) AS min, AVG(nPredicate) AS avg, MAX(nPredicate) AS max, STDDEV(nPredicate) AS standardDeviation "
					+ "FROM (SELECT COUNT (DISTINCT predicate) AS nPredicate "
							+ "FROM dataset " 
							+ "GROUP BY subject) ").write().option("sep", ";").csv(output_dir + "/subjectPredicates");
		
		dir.put("subjectPredicates", new ArrayList<String>(Arrays.asList("subjectPredicates", "standardDeviation")));
		new BuildJSON(session).minMaxAvgOther(output_dir, dir.get("subjectPredicates"));
	}
	
	//stat 32
	public void subjectObject() {
		session.sql("SELECT MIN(nPredicate) AS min, AVG(nPredicate) AS avg, MAX(nPredicate) AS max "
					+ "FROM (SELECT COUNT (DISTINCT predicate) AS nPredicate "
							+ "FROM dataset " 
							+ "GROUP BY subject, object) ").write().option("sep", ";").csv(output_dir + "/subjectObject");
		
		dir.put("subjectObject", new ArrayList<String>(Arrays.asList("subjectObject")));
		new BuildJSON(session).minMaxAvg(output_dir, dir.get("subjectObject"));
	}

	//stat 33
	public void subjectCount() {
		session.sql("SELECT MIN(number) AS min, AVG(number) AS avg, MAX(number) AS max "
					+ "FROM (SELECT COUNT (object) AS number "
							+ "FROM dataset "
							+ "WHERE predicate != 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type' "
							+ "GROUP BY subject) ").write().option("sep", ";").csv(output_dir + "/subjectCount");
		
		dir.put("subjectCount", new ArrayList<String>(Arrays.asList("subjectCount")));
		new BuildJSON(session).minMaxAvg(output_dir, dir.get("subjectCount"));
	}
	
	//stat 34
	public void objectCount() {
		session.sql("SELECT MIN(number) AS min, AVG(number) AS avg, MAX(number) AS max "
					+ "FROM (SELECT COUNT (subject) AS number "
							+ "FROM dataset "
							+ "WHERE predicate != 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type' "
							+ "GROUP BY object) ").write().option("sep", ";").csv(output_dir + "/objectCount");
		
		dir.put("objectCount", new ArrayList<String>(Arrays.asList("objectCount")));
		new BuildJSON(session).minMaxAvg(output_dir, dir.get("objectCount"));
	}
	
	//stat 35
	public void predicateTriples() {
		session.sql("SELECT predicate, COUNT (predicate) AS nTriples "
					+ "FROM dataset " 
					+ "GROUP BY predicate "
					+ "ORDER BY nTriples DESC").write().option("sep", ";").csv(output_dir + "/predicateTriples");
		
		dir.put("predicateTriples", new ArrayList<String>(Arrays.asList("predicateTriples", "predicate", "nTriples")));
		new BuildJSON(session).number(output_dir, dir.get("predicateTriples"));
	}
	
	//stat 36
	public void predicateSubjects() {
		session.sql("SELECT predicate, COUNT (DISTINCT subject) AS nSubjects "
					+ "FROM dataset " 
					+ "GROUP BY predicate "
					+ "ORDER BY nSubjects DESC ").write().option("sep", ";").csv(output_dir + "/predicateSubjects");
		
		dir.put("predicateSubjects", new ArrayList<String>(Arrays.asList("predicateSubjects", "predicate", "nSubjects")));
		new BuildJSON(session).number(output_dir, dir.get("predicateSubjects"));
	}

	//stat 37
	public void predicateObjects() {
		session.sql("SELECT predicate, COUNT (DISTINCT object) AS nObjects "
					+ "FROM dataset "
					+ "WHERE predicate != 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type' "     
					+ "GROUP BY predicate "
					+ "ORDER BY nObjects DESC ").write().option("sep", ";").csv(output_dir + "/predicateObjects");
		
		dir.put("predicateObjects", new ArrayList<String>(Arrays.asList("predicateObjects", "predicate", "nObjects")));
		new BuildJSON(session).number(output_dir, dir.get("predicateObjects"));
	}
	
	//stat 38
	public void subjectObjectRatio() {
		session.sql("SELECT subject, COUNT(subject) AS nSubject "
					+ "FROM dataset "
					+ "WHERE type = 'object_relational' "
					+ "OR type = 'dt_relational' "
					+ "GROUP BY subject ").createOrReplaceTempView("Sub");
		
		session.sql("SELECT object, COUNT(object) AS nObject "
					+ "FROM dataset "
					+ "WHERE type = 'object_relational' "
					+ "GROUP BY object ").createOrReplaceTempView("Object");
		
		session.sql("SELECT COALESCE(subject,object) AS subject, NVL(nSubject,0) AS nSubject "
					+ "FROM Sub FULL OUTER JOIN Object "
					+ "ON subject = object ").createOrReplaceTempView("Subject");
		
		session.sql("SELECT COALESCE(subject,object) AS value, NVL(nSubject,0) + NVL(nObject,0) AS nTot "
					+ "FROM Subject FULL OUTER JOIN Object "
					+ "ON subject = object ").createOrReplaceTempView("Tot");
			
		session.sql("SELECT MIN(number) AS min, AVG(number) AS avg, MAX(number) AS max "
		 			+ "FROM(SELECT subject, nSubject/nTot AS number "
		 					+ "FROM Subject, Tot "
		 					+ "WHERE subject = value) ").write().option("sep", ";").csv(output_dir + "/subjectObjectRatio");
		
		dir.put("subjectObjectRatio", new ArrayList<String>(Arrays.asList("subjectObjectRatio")));
		new BuildJSON(session).minMaxAvg(output_dir, dir.get("subjectObjectRatio"));
	}
		
	//stat 39
	public void subjectPredicateRatio() {
		session.sql("SELECT subject, COUNT(subject) AS nSubject "
					+ "FROM dataset "
					+ "WHERE subject NOT LIKE 'http://dbpedia.org/resource/%' "
					+ "GROUP BY subject ").createOrReplaceTempView("Sub");
	
		session.sql("SELECT predicate, COUNT(predicate) AS nPredicate "
					+ "FROM dataset "
					+ "GROUP BY predicate ").createOrReplaceTempView("Predicate");
	
		session.sql("SELECT COALESCE(subject, predicate) AS subject, NVL(nSubject,0) AS nSubject "
					+ "FROM Sub FULL OUTER JOIN Predicate "
					+ "ON subject = predicate ").createOrReplaceTempView("Subject");
	
		session.sql("SELECT COALESCE(subject,predicate) AS value, NVL(nSubject,0) + NVL(nPredicate,0) AS nTot "
					+ "FROM Subject FULL OUTER JOIN Predicate "
					+ "ON subject = predicate ").createOrReplaceTempView("Tot");
	
		session.sql("SELECT MIN(number) AS min, AVG(number) AS avg, MAX(number) AS max "
					+ "FROM(SELECT subject, nSubject/nTot AS number "
							+ "FROM Subject, Tot "
							+ "WHERE subject = value) ").write().option("sep", ";").csv(output_dir + "/subjectPredicateRatio");
		
		dir.put("subjectPredicateRatio", new ArrayList<String>(Arrays.asList("subjectPredicateRatio")));
		new BuildJSON(session).minMaxAvg(output_dir, dir.get("subjectPredicateRatio"));
	}
		
	//stat 40
	public void predicateObjectRatio() {
		session.sql("SELECT object, COUNT(subject) AS nObject "
					+ "FROM dataset "
					+ "WHERE object NOT LIKE 'http://dbpedia.org/resource/%' "
					+ "GROUP BY object ").createOrReplaceTempView("Obj");

		session.sql("SELECT predicate, COUNT(predicate) AS nPredicate "
					+ "FROM dataset "
					+ "GROUP BY predicate ").createOrReplaceTempView("Predicate");

		session.sql("SELECT COALESCE(object, predicate) AS object, NVL(nObject,0) AS nObject "
					+ "FROM Obj FULL OUTER JOIN Predicate "
					+ "ON object = predicate ").createOrReplaceTempView("Object");

		session.sql("SELECT COALESCE(object,predicate) AS value, NVL(nObject,0) + NVL(nPredicate,0) AS nTot "
					+ "FROM Object FULL OUTER JOIN Predicate "
					+ "ON object = predicate ").createOrReplaceTempView("Tot");

		session.sql("SELECT MIN(number) AS min, AVG(number) AS avg, MAX(number) AS max "
					+ "FROM(SELECT predicate, nPredicate/nTot AS number "
						+ "FROM Predicate, Tot "
						+ "WHERE predicate = value) ").write().option("sep", ";").csv(output_dir + "/predicateObjectRatio"); 
		
		dir.put("predicateObjectRatio", new ArrayList<String>(Arrays.asList("predicateObjectRatio")));
		new BuildJSON(session).minMaxAvg(output_dir, dir.get("predicateObjectRatio"));
	}

	//stat 43
	public void rarePredicate() {
		session.sql("SELECT COUNT(predicate) AS nRarePradicate "
					+ "FROM (SELECT predicate "
							+ "FROM dataset "
							+ "GROUP BY predicate "
							+ "HAVING COUNT(predicate) = 1)" ).write().option("sep", ";").csv(output_dir + "/rarePredicate");
		
		dir.put("rarePredicate", new ArrayList<String>(Arrays.asList("rarePredicate", "long")));
		new BuildJSON(session).oneElement(output_dir, dir.get("rarePredicate"));
	}

	//stat 46
	public void countTypedSubject() {
		session.sql("SELECT MIN(nSubject) AS min, AVG(nSubject) AS avg, MAX(nSubject) AS max "
					+ "FROM (SELECT COUNT(subject) as nSubject "
							+ "FROM dataset "
							+ "WHERE predicate = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type' "
							+ "GROUP BY subject) ").write().option("sep", ";").csv(output_dir + "/countTypedSubject");
		
		dir.put("countTypedSubject", new ArrayList<String>(Arrays.asList("countTypedSubject")));
		new BuildJSON(session).minMaxAvg(output_dir, dir.get("countTypedSubject"));
	}
	
	
	public void mergedAndDeleteFolder() throws IOException {
		new BuildJSON(session).mergedAndDeleteFolder(output_dir, dir.keySet());
	}
}
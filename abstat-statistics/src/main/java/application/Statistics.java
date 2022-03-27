package application;

import java.io.IOException;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Statistics {
	
	private SparkSession session;
	private String output_dir;
	private static String[] datasets;
	private String PLD;
	private BuildJSON json_builder;

	public Statistics(String master, String datasets, String output_dir, String PLD) {
		this.session = SparkSession.builder().appName("ABSTAT-statistics").master(master).getOrCreate();
		this.session.sparkContext().setLogLevel("ERROR");
		Statistics.datasets = datasets.split(";");
		this.PLD = PLD;
		this.output_dir = output_dir;
		this.json_builder = new BuildJSON(this.session, this.output_dir);
	}
	
	public static void main(String[] args) throws Exception {
		Statistics s = new Statistics(args[0], args[1], args[2], args[3]);
		
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
		s.mergedAndWrite();
	}
	
	public void preProcessing(String[] datasets) throws Exception {
		JavaRDD<String> input = session.read().textFile(datasets).javaRDD();
		JavaRDD<Triple> rdd = new Splitter().calculate(input);
		Dataset<Row> data = session.createDataFrame(rdd, Triple.class);
		data.createOrReplaceTempView("dataset");
		//data.show(50, false);
		json_builder.fakeTable();
	}
	
	//stat 4
	public void countConceptsPLD(){		
		String like = "";
		String not_like = "";
		String[] PLDs = PLD.split(" ");
		for (int i=0; i< PLDs.length; i++){
			if(i==0){
				not_like += "WHERE object NOT LIKE '%" +PLDs[i]+ "%' ";
				like += "WHERE object LIKE '%" +PLDs[i]+ "%' ";
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
		
		session.sql("SELECT object FROM DistinctObject " + not_like).createOrReplaceTempView("withoutPLD");
		session.sql("SELECT object FROM DistinctObject " + like).createOrReplaceTempView("withPLD");

		session.sql("SELECT (SELECT COUNT(*) FROM withPLD) AS withPLD, (SELECT COUNT(*) FROM withoutPLD) AS withoutPLD")
		.write().option("header", true).option("sep", ";").csv(output_dir + "/countConceptsPLD");

		json_builder.withAndWithout(new String[]{"countConceptsPLD", "withPLD", "withoutPLD"});
	}

	//stat 7
	public void countPropertiesPLD(){	
		String like = "";
		String not_like = "";
		String[] PLDs = PLD.split(" ");
		for (int i=0; i< PLDs.length; i++){
			if(i==0){
				not_like += "WHERE predicate NOT LIKE '%" +PLDs[i]+ "%' ";
				like += "WHERE predicate LIKE '%" +PLDs[i]+ "%' ";
			}
			else {
				not_like += "OR predicate  NOT LIKE  '%" +PLDs[i]+ "%' ";
				like += "OR predicate LIKE  '%" +PLDs[i]+ "%' ";
			}
		}
		
		session.sql("SELECT predicate "
					+ "FROM dataset "
					+ "GROUP BY predicate ").createOrReplaceTempView("DistinctPredicate");
	
		session.sql("SELECT predicate FROM DistinctPredicate " + not_like).createOrReplaceTempView("withoutPLD");
		session.sql("SELECT predicate FROM DistinctPredicate " + like).createOrReplaceTempView("withPLD");

		session.sql("SELECT (SELECT COUNT(*) FROM withPLD) AS withPLD, (SELECT COUNT(*) FROM withoutPLD) AS withoutPLD")
		.write().option("header", true).option("sep", ";").csv(output_dir + "/countPropertiesPLD");
		
		json_builder.withAndWithout(new String[]{"countPropertiesPLD", "withPLD", "withoutPLD"});
	}
	
	//stat 10
	public void bNodesObject() {	
		session.sql("SELECT COUNT (object) AS nBNodesObject "
					+ "FROM dataset "
					+ "WHERE object LIKE '_:%' ").write().option("header", true).option("header", true).option("sep", ";").csv(output_dir + "/bNodesObject");

		json_builder.oneElement(new String[]{"bNodesObject", "nBNodesObject", "long"});
	}
	
	//stat 11
	public void bNodesSubject() {
		session.sql("SELECT COUNT (subject) AS nBNodesSubject "
					+ "FROM dataset "
					+ "WHERE subject LIKE '_:%' ").write().option("header", true).csv(output_dir + "/bNodesSubject");
		
		json_builder.oneElement(new String[]{"bNodesSubject", "nBNodesSubject", "long"});
	}
	
	//stat 12
	public void datatype() {
		session.sql("SELECT datatype, COUNT(datatype) AS nDatatype  "
					+ "FROM dataset "
					+ "WHERE datatype is not null "
					+ "GROUP BY datatype "
					+ "ORDER BY nDatatype DESC").write().option("header", true).option("sep", ";").csv(output_dir + "/datatypes");

		json_builder.number(new String[]{"datatypes", "datatype", "nDatatype"});
	}

	//stat 13
	public void countLanguage() {
		session.sql("SELECT language, COUNT(language) as nLanguage "
					+ "FROM (SELECT substring(object, -3, 3) AS language "
							+ "FROM dataset "
							+ "WHERE object NOT LIKE 'http://dbpedia%' "
							+ "AND object REGEXP '.*@[a-z][a-z]$' ) "
					+ "GROUP BY language "
					+ "ORDER BY nLanguage DESC").write().option("header", true).option("sep", ";").csv(output_dir + "/languages");
		
		json_builder.number(new String[]{"languages", "language", "nLanguage"});
	} 
	
	//stat 14
	public void outgoingLinks() {
		String like = "";
		String not_like = "";
		String[] PLDs = PLD.split(" ");
		for (int i=0; i< PLDs.length; i++){
			if(i==0){
				not_like += "WHERE object NOT LIKE '%" +PLDs[i]+ "%' ";
				like += "WHERE subject LIKE '%" +PLDs[i]+ "%' ";
			}
			else {
				not_like += "OR object  NOT LIKE  '%" +PLDs[i]+ "%' ";
				like += "OR subject LIKE  '%" +PLDs[i]+ "%' ";
			}
		}
		session.sql("SELECT * FROM dataset WHERE type = 'object_relational'").createOrReplaceTempView("temp");
		session.sql("SELECT * FROM temp " + like).createOrReplaceTempView("temp");
		session.sql("SELECT * FROM temp " + not_like).createOrReplaceTempView("temp");
		session.sql("SELECT COUNT(*) AS nOutgoingLinks FROM temp").write().option("header", true).option("sep", ";").csv(output_dir + "/outgoingLinks");
		json_builder.oneElement(new String[]{"outgoingLinks", "nOutgoingLinks", "long"});
	}
	
	//stat 15
	public void incomingLinks() {
		String like = "";
		String not_like = "";
		String[] PLDs = PLD.split(" ");
		for (int i=0; i< PLDs.length; i++){
			if(i==0){
				not_like += "WHERE subject NOT LIKE '%" +PLDs[i]+ "%' ";
				like += "WHERE object LIKE '%" +PLDs[i]+ "%' ";
			}
			else {
				not_like += "OR subject NOT LIKE  '%" +PLDs[i]+ "%' ";
				like += "OR object LIKE '%" +PLDs[i]+ "%' ";
			}
		}
		session.sql("SELECT * FROM dataset WHERE type = 'object_relational'").createOrReplaceTempView("temp");
		session.sql("SELECT * FROM temp " + like).createOrReplaceTempView("temp");
		session.sql("SELECT * FROM temp " + not_like).createOrReplaceTempView("temp");
		session.sql("SELECT COUNT(*) AS nIncomingLinks FROM temp").write().option("header", true).option("sep", ";").csv(output_dir + "/incomingLinks");
		json_builder.oneElement(new String[]{"incomingLinks", "nIncomingLinks", "long"});
	}

	//stat 16
	public void rdfsLabel() {
		session.sql("SELECT COUNT(subject) AS nRdfsLabel "
					+ "FROM dataset "
					+ "WHERE predicate = 'http://www.w3.org/2000/01/rdf-schema#label' ").write().option("header", true).option("sep", ";").csv(output_dir + "/rdfsLabel");
		
		json_builder.oneElement(new String[]{"rdfsLabel", "nRdfsLabel", "long"});
	}
	
	//stat 17
	public void literalsWithType() {	
		session.sql("SELECT COUNT(type) AS nLiteralsWithType "
					+ "FROM dataset "
					+ "WHERE type = 'dt_relational' "
					+ "AND datatype is not null ").write().option("sep", ";").option("header", true).csv(output_dir + "/literalsWithType");
		
		json_builder.oneElement(new String[]{"literalsWithType", "nLiteralsWithType", "long"});
	}
	
	//stat 18
	public void literalsWithoutType() {	
		session.sql("SELECT COUNT(type) AS nLiteralsWithoutType "
					+ "FROM dataset "
					+ "WHERE datatype is null "
					+ "AND type = 'dt_relational' ").write().option("header", true).option("sep", ";").csv(output_dir + "/literalsWithoutType");
					
		json_builder.oneElement(new String[]{"literalsWithoutType", "nLiteralsWithoutType", "long"});
	}

	//stat 19
	public void vocabularies() {
		session.sql("SELECT subject as spo  " 
		          + "FROM (SELECT subject from dataset WHERE type = 'object_relational') "
				  + "UNION ALL (SELECT object from dataset WHERE type = 'object_relational') "
				  + "UNION ALL (SELECT predicate from dataset WHERE type = 'object_relational')"
				  + "UNION ALL (SELECT subject  from dataset WHERE type = 'dt_relational')"
				  + "UNION ALL (SELECT predicate from dataset WHERE type = 'dt_relational')"
				  + "UNION ALL (SELECT subject from dataset WHERE type = 'typing')"
				  + "UNION ALL (SELECT predicate from dataset WHERE type = 'typing')"
				  + "UNION ALL (SELECT object from dataset WHERE type = 'typing')").createOrReplaceTempView("spo");

		//regexp_replace(spo, '\\/[A-Z,a-z,0-9,_,.,(,),-]*$',''
		session.sql("SELECT clean_spo as vocabulary_basic, COUNT(*) AS count_basic FROM " 
		          + "  (SELECT regexp_replace(spo, '(#|\\/)[^\\/#]*$','') as clean_spo " 
				  + "  FROM spo ) "
				  + "  WHERE clean_spo != 'http:/' "
				  + "GROUP BY clean_spo").createOrReplaceTempView("count_basic");
		//session.table("count_basic").show(200, false);


		// the following operations are meant to deal with cases like http://dbpedia.org/ontology/PopulatedPlace
		session.sql("SELECT vocabulary_temp, SUM(count_basic) AS count_temp FROM " 
				+ "  (SELECT regexp_replace(vocabulary_basic, '\\/[^\\/]*$','') as vocabulary_temp, count_basic" 
				+ "  FROM count_basic ) "
				+ "GROUP BY vocabulary_temp").createOrReplaceTempView("count_temp");
		//session.table("count_temp").show(200, false);

		// clean_detailed
		session.sql("SELECT vocabulary_basic, regexp_replace(vocabulary_basic, '\\/[^\\/]*$','') as vocabulary_temp, count_basic" 
				+ "  FROM count_basic ").createOrReplaceTempView("detailed");
		//session.table("detailed").show(200, false);
		
		// calc intersection
		session.sql("SELECT vocabulary_basic as intersection FROM (SELECT vocabulary_basic from count_basic) INTERSECT (SELECT vocabulary_temp from count_temp) ").createOrReplaceTempView("intersect");
		//session.table("intersect").show(200, false);


		// calcoli i prefissi da togliere dalla prima versione.
		session.sql("SELECT vocabulary_basic as to_remove FROM intersect INNER JOIN detailed ON intersect.intersection=detailed.vocabulary_temp").createOrReplaceTempView("to_remove");
		//session.table("to_remove").show(200, false);
		
		// count_basic clean
		session.sql("SELECT vocabulary_basic, count_basic FROM count_basic LEFT JOIN to_remove on vocabulary_basic=to_remove WHERE to_remove IS NULL").createOrReplaceTempView("count_basic");
		//session.table("count_basic").show(200, false);

		// count basic with updated counter
		String[] colNames = {"count_temp"};
		session.sql("SELECT *  from  count_basic LEFT JOIN count_temp ON vocabulary_basic=vocabulary_temp ").na().fill(0, colNames).createOrReplaceTempView("temp");
		session.sql("SELECT vocabulary_basic as vocabulary, (count_basic + count_temp) AS count from temp").write().option("header", true).option("sep", ";").csv(output_dir + "/vocabularies");

		json_builder.number(new String[]{"vocabularies", "vocabulary", "count"});
	}
	
	//stat 22
	public void sameAsLink() {
		session.sql("SELECT MIN(number) AS min, AVG(number) AS avg, MAX(number) AS max, (SELECT COUNT(subject) "
																						+ "FROM dataset "
																						+ "WHERE predicate = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type' ) AS triple "
					+ "FROM (SELECT COUNT(subject) AS number "
							+ "FROM dataset "
							+ "WHERE predicate = 'http://www.w3.org/2002/07/owl#sameAs' "
							+ "GROUP BY subject) ").write().option("header", true).option("sep", ";").csv(output_dir + "/sameAsLink");
		
		json_builder.minMaxAvgOther(new String[]{"sameAsLink", "triple"});
	}
	
	//stat 23
	public void owlSameas() {
		session.sql("SELECT COUNT(subject) AS withOwlSemeas, (SELECT COUNT(subject) "
																+ "FROM dataset "
																+ "WHERE predicate != 'http://www.w3.org/2002/07/owl#sameAs') AS withoutOwlSemeas "
					+ "FROM dataset "
					+ "WHERE predicate = 'http://www.w3.org/2002/07/owl#sameAs' ").write().option("header", true).option("sep", ";").csv(output_dir + "/owlSameas");
		
		json_builder.withAndWithout(new String[]{"owlSameas", "withOwlSemeas", "withoutOwlSemeas"});
	}
	
	//stat 24
	public void lengthStringAndUntypedLiterals() {
		session.sql("SELECT AVG(LENGTH(object)) AS AVGLengthLiterals "
					+ "FROM dataset "
					+ "WHERE type = 'dt_relational' "
					+ "AND datatype is null "
					+ "OR datatype = 'http://www.w3.org/2001/XMLSchema#string' ").write().option("header", true).option("sep", ";").csv(output_dir + "/avgLengthLiterals");
		
		json_builder.oneElement(new String[]{"avgLengthLiterals", "AVGLengthLiterals", "double"});
	}
	
	//stat 25
	public void typedSubject() {
		session.sql("SELECT COUNT (DISTINCT subject) AS nTypedSubject "
					+ "FROM dataset "
					+ "WHERE predicate = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type' ").write().option("header", true).option("sep", ";").csv(output_dir + "/typedSubject");
		
		json_builder.oneElement(new String[]{"typedSubject", "nTypedSubject",  "long"});
	}
	
	//stat 26
	public void untypedSubject() {
		session.sql("select * FROM "
			     + "(SELECT DISTINCT subject FROM dataset) "
				 +	"MINUS "
				 + "(SELECT DISTINCT subject FROM dataset WHERE predicate = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type')").createOrReplaceTempView("untyped_subjects");

		session.sql("SELECT COUNT(DISTINCT subject) AS nUntypedSubject FROM untyped_subjects").write().option("header", true).option("sep", ";").csv(output_dir + "/untypedSubject");
		json_builder.oneElement(new String[]{"untypedSubject", "nUntypedSubject", "long"});
	}
	
	//stat 29
	public void triplesEntity() {
		session.sql("SELECT MIN(nTriples) AS min, AVG(nTriples) AS avg, MAX(nTriples) AS max "
					+ "FROM (SELECT COUNT (subject) AS nTriples "
							+ "FROM dataset "
							+ "WHERE type = 'dt_relational' "
							+ "OR type = 'object_relational' "
							+ "GROUP BY subject) ").write().option("header", true).option("sep", ";").csv(output_dir + "/triplesEntity");
		
		json_builder.minMaxAvg(new String[]{"triplesEntity"});
	}
	
	//stat 30
	public void subjectPredicates() {
		session.sql("SELECT MIN(nPredicate) AS min, AVG(nPredicate) AS avg, MAX(nPredicate) AS max, STDDEV(nPredicate) AS standardDeviation "
					+ "FROM (SELECT COUNT (DISTINCT predicate) AS nPredicate "
							+ "FROM dataset " 
							+ "GROUP BY subject) ").write().option("header", true).option("sep", ";").csv(output_dir + "/subjectPredicates");
		
		json_builder.minMaxAvgOther(new String[]{"subjectPredicates", "standardDeviation"});
	}
	
	//stat 32
	public void subjectObject() {
		session.sql("SELECT MIN(nPredicate) AS min, AVG(nPredicate) AS avg, MAX(nPredicate) AS max "
					+ "FROM (SELECT COUNT (DISTINCT predicate) AS nPredicate "
							+ "FROM dataset " 
							+ "GROUP BY subject, object) ").write().option("header", true).option("sep", ";").csv(output_dir + "/subjectObject");
		
		json_builder.minMaxAvg(new String[]{"subjectObject"});
	}

	//stat 33
	public void subjectCount() {
		session.sql("SELECT MIN(number) AS min, AVG(number) AS avg, MAX(number) AS max "
					+ "FROM (SELECT COUNT (object) AS number "
							+ "FROM dataset "
							+ "WHERE predicate != 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type' "
							+ "GROUP BY subject) ").write().option("header", true).option("sep", ";").csv(output_dir + "/subjectCount");
		
		json_builder.minMaxAvg(new String[]{"subjectCount"});
	}
	
	//stat 34
	public void objectCount() {
		session.sql("SELECT MIN(number) AS min, AVG(number) AS avg, MAX(number) AS max "
					+ "FROM (SELECT COUNT (subject) AS number "
							+ "FROM dataset "
							+ "WHERE predicate != 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type' "
							+ "GROUP BY object) ").write().option("header", true).option("sep", ";").csv(output_dir + "/objectCount");
		
		json_builder.minMaxAvg(new String[]{"objectCount"});
	}
	
	//stat 35
	public void predicateTriples() {
		session.sql("SELECT predicate, COUNT (predicate) AS nTriples "
					+ "FROM dataset " 
					+ "GROUP BY predicate "
					+ "ORDER BY nTriples DESC").write().option("header", true).option("header", true).option("sep", ";").csv(output_dir + "/predicateTriples");
		
		json_builder.number(new String[]{"predicateTriples", "predicate", "nTriples"});
	}
	
	//stat 36
	public void predicateSubjects() {
		session.sql("SELECT predicate, COUNT (DISTINCT subject) AS nSubjects "
					+ "FROM dataset " 
					+ "GROUP BY predicate "
					+ "ORDER BY nSubjects DESC ").write().option("header", true).option("sep", ";").csv(output_dir + "/predicateSubjects");
		
		json_builder.number(new String[]{"predicateSubjects", "predicate", "nSubjects"});
	}

	//stat 37
	public void predicateObjects() {
		session.sql("SELECT predicate, COUNT (DISTINCT object) AS nObjects "
					+ "FROM dataset "
					+ "WHERE predicate != 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type' "     
					+ "GROUP BY predicate "
					+ "ORDER BY nObjects DESC ").write().option("header", true).option("sep", ";").csv(output_dir + "/predicateObjects");
		
		json_builder.number(new String[]{"predicateObjects", "predicate", "nObjects"});
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
		 					+ "WHERE subject = value) ").write().option("header", true).option("sep", ";").csv(output_dir + "/subjectObjectRatio");
		
		json_builder.minMaxAvg(new String[]{"subjectObjectRatio"});
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
							+ "WHERE subject = value) ").write().option("header", true).option("sep", ";").csv(output_dir + "/subjectPredicateRatio");

		json_builder.minMaxAvg(new String[]{"subjectPredicateRatio"});
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
						+ "WHERE predicate = value) ").write().option("header", true).option("sep", ";").csv(output_dir + "/predicateObjectRatio"); 
		
		json_builder.minMaxAvg(new String[]{"predicateObjectRatio"});
	}

	//stat 43
	public void rarePredicate() {
		session.sql("SELECT COUNT(predicate) AS nRarePradicate "
					+ "FROM (SELECT predicate "
							+ "FROM dataset "
							+ "GROUP BY predicate "
							+ "HAVING COUNT(predicate) = 1)" ).write().option("header", true).option("sep", ";").csv(output_dir + "/rarePredicate");

		json_builder.oneElement(new String[]{"rarePredicate", "nRarePradicate", "long"});
	}

	//stat 46
	public void countTypedSubject() {
		session.sql("SELECT MIN(nSubject) AS min, AVG(nSubject) AS avg, MAX(nSubject) AS max "
					+ "FROM (SELECT COUNT(subject) as nSubject "
							+ "FROM dataset "
							+ "WHERE predicate = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type' "
							+ "GROUP BY subject) ").write().option("header", true).option("sep", ";").csv(output_dir + "/countTypedSubject");
		
		json_builder.minMaxAvg(new String[]{"countTypedSubject"});
	}
	
	
	public void mergedAndWrite() throws IOException {
		json_builder.mergedAndWrite();
	}
}
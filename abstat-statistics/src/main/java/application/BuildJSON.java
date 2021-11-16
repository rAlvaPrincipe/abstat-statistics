package application;

import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.zookeeper.server.SessionTracker.Session;

import static org.apache.spark.sql.functions.*;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;


public class BuildJSON {
	
	private SparkSession session;
	
	public BuildJSON(SparkSession session) {
		this.session = session;
	}

	public void withAndWithout (String output_dir, ArrayList<String> arrayList) {
		session.read().format("csv").option("sep", ";").load(output_dir + "/" + arrayList.get(0))
		.select(struct(col("_c0").cast("long").alias(arrayList.get(1)), col("_c1").cast("long").alias(arrayList.get(2))).as(arrayList.get(0)))
		.createOrReplaceTempView("temps");
		
		merged();
	}
	
	public void oneElement (String output_dir, ArrayList<String> arrayList) {
		try {
			session.read().format("csv").option("sep", ";").load(output_dir + "/" + arrayList.get(0))
			.select(col("_c0").cast(arrayList.get(1)).alias(arrayList.get(0)))
			.createOrReplaceTempView("temps");
		}
		catch(java.lang.UnsupportedOperationException e){
			session.emptyDataFrame().createOrReplaceTempView("temps");
		}
		merged();
	}
	
	public void minMaxAvg (String output_dir, ArrayList<String> arrayList) {
		session.read().format("csv").option("sep", ";").load(output_dir + "/" + arrayList.get(0))
		.select(struct(col("_c0").cast("double").alias("min"), col("_c1").cast("double").alias("avg"), col("_c2").cast("double").alias("max")).as(arrayList.get(0)))
		.createOrReplaceTempView("temps");
		
		merged();
	}
	
	public void number (String output_dir, ArrayList<String> arrayList) {
		try{
			session.read().format("csv").option("sep", ";").load(output_dir + "/" + arrayList.get(0))
			.select(collect_list(struct(col("_c0").alias(arrayList.get(1)), col("_c1").cast("long").alias(arrayList.get(2)))).as(arrayList.get(0)))
			.createOrReplaceTempView("temps");
		}
		catch(java.lang.UnsupportedOperationException e){
			session.emptyDataFrame().createOrReplaceTempView("temps");
		}
		
		merged();
	}
	
	public void minMaxAvgOther (String output_dir, ArrayList<String> arrayList) {
		session.read().format("csv").option("sep", ";").load(output_dir + "/" +arrayList.get(0))
		.select(struct(col("_c0").cast("double").alias("min"), col("_c1").cast("double").alias("avg"), col("_c2").cast("double").alias("max"), col("_c3").cast("double").alias(arrayList.get(1))).as(arrayList.get(0)))
		.createOrReplaceTempView("temps");
		
		merged();
	}

	public void fakeTable(){
		Dataset<Row> df4 = session.sql("select cast(null as string) fake");
        df4.createOrReplaceTempView("merged");
	}
	
	public void merged() {
		if (!session.table("temps").rdd().isEmpty())
			session.sql("SELECT merged.*, temps.* "
						+ "FROM temps CROSS JOIN merged ").createOrReplaceTempView("merged");
	}
	
	public void mergedAndDeleteFolder (String output_dir, Set<String> set) throws IOException{
		session.sql("SELECT * FROM merged").write().json(output_dir + "/merged");
		Iterator<String> dir = set.iterator();
		List<String> dirList = IteratorUtils.toList(dir); 
		for(int i=0; i<dirList.size(); i++)
			FileUtils.forceDelete(new File(output_dir + "/" + dirList.get(i)));
	}
}
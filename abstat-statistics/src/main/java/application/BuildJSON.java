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
	private String output_dir;
	
	public BuildJSON(SparkSession session, String output_dir) {
		this.session = session;
		this.output_dir = output_dir;
	}

	public void withAndWithout (String[] list) {
		session.read().format("csv").option("header", true).option("sep", ";").load(output_dir + "/" + list[0])
		.select(struct(col(list[1]).cast("long").alias(list[1]), col(list[2]).cast("long").alias(list[2])).as(list[0]))
		.createOrReplaceTempView("temps");
		merged();
	}
	
	public void oneElement (String[] list) {
		try {
			session.read().format("csv").option("header", true).option("sep", ";").load(output_dir + "/" + list[0])
			.select(col(list[1]).cast(list[2]).alias(list[0]))
			.createOrReplaceTempView("temps");
		}
		catch(java.lang.UnsupportedOperationException e){
			session.emptyDataFrame().createOrReplaceTempView("temps");
		}
		merged();
	}
	
	public void minMaxAvg (String[] list) {
		session.read().format("csv").option("header",true).option("sep", ";").load(output_dir + "/" + list[0])
		.select(struct(col("min").cast("double").alias("min"), col("avg").cast("double").alias("avg"), col("max").cast("double").alias("max")).as(list[0]))
		.createOrReplaceTempView("temps");
		
		merged();
	}
	
	public void number (String[] list) {
		try{
			session.read().format("csv").option("header", true).option("sep", ";").load(output_dir + "/" + list[0])
			.select(collect_list(struct(col(list[1]), col(list[2]).cast("long").alias(list[2]))).as(list[0]))
			.createOrReplaceTempView("temps");
		}
		catch(java.lang.UnsupportedOperationException e){
			session.emptyDataFrame().createOrReplaceTempView("temps");
		}
		
		merged();
	}
	
	public void minMaxAvgOther (String[] list) {
		session.read().format("csv").option("header",true).option("sep", ";").load(output_dir + "/" +list[0])
		.select(struct(col("min").cast("double").alias("min"), col("avg").cast("double").alias("avg"), col("max").cast("double").alias("max"), col(list[1]).cast("double").alias(list[1])).as(list[0]))
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
	
	public void mergedAndWrite() throws IOException{
		session.sql("SELECT * FROM merged").write().json(output_dir + "/merged");
	}
}
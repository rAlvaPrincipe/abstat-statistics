package application;
import java.io.Serializable;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

public class Splitter implements Serializable{

	private static final long serialVersionUID = -7306321498926231792L;

	public JavaRDD<Triple> calculate(JavaRDD<String> input) {
		JavaRDD<Triple> triples = input.map(new Function<String, Triple>() {
			public Triple call(String tripleString) {
				Triple t =  func(tripleString);
				return t;
			}
		});
		return triples;
	}
	
	
	public Triple func(String line) {
		try {
			String s, p, o, dt;
	
			// Blank nodes management
			if (line.startsWith("_:") && line.split("> _:").length==2) {  // bnode al soggetto e oggetto
				String[] temp = line.split(" <http");
				s = temp[0] + "|";
				String[] temp2 = temp[1].split(" _:");
				p = "http" + temp2[0].substring(0, temp2[0].length()-1) +"|";
				o = "_:" + temp2[1].substring(0, temp2[1].length()-2) + "|";
				return new Triple("bnode_triple", s, p, o);
			}
			else if (line.startsWith("_:")){              // bnode solo al soggetto e all'oggetto  uri o literal
				String[] temp = line.split(" <http");
				s = temp[0];
				if (temp.length == 3){                    // l'oggetto è un uri
					p = "http" + temp[1].substring(0,temp[1].length()-1);
					o = "http" + temp[2].substring(0,temp[2].length()-3);
				}
				else {
					String[] temp2 =temp[1].split("> ");  // l'oggetto è un literal
					p = temp2[0];
					o = temp2[1];
				}
				return new Triple("bnode_triple", s, p, o);
			}
			else if (line.split("> _:").length==2){       // bnode solo all'oggetto
				String[] temp = line.split(" _:");
				o = temp[1].substring(0,temp[1].length()-3);
				String[] temp2 = temp[0].split("> <");
				s = temp2[0].substring(1);
				p = temp2[1].substring(0, temp2[1].length()-1 );
				return new Triple("bnode_triple", s, p, o);
			}
			
			// typing/relational assertions management
			String[] splitted = line.split("> <http");
			s = splitted[0].substring(1);
			if (s.contains("<") || s.contains(">") || s.contains("\""))
				return new Triple("null","null","null","null");
	
			if (splitted.length == 2) {                           //probably a datatype relational assertion
				String[] splitted2 = splitted[1].split("> \"");
	
				if (splitted2.length == 2) {
					p = "http" + splitted2[0];
					if (p.contains("<") || p.contains(">") || p.contains("\""))
						return new Triple("null","null","null","null");
	
					String[] splitted3 = splitted2[1].split("\"\\^\\^");
	
					if (splitted3.length == 2) {
						o = splitted3[0];
						o = o.replace("##", "%23%23").replace("\\", "").replace("\"", "\\\"");   //to avoid \n \r, ecc to avoid problems caused by ## and "" inside ""
						dt = splitted3[1].substring(1, splitted3[1].length() - 3);
						return new Triple("dt_relational", s, p, o, dt);
					} else {
						splitted3 = splitted2[1].split("\"@");
						if (splitted3.length == 2) {
							o = splitted3[0];
							o = o.replace("##", "%23%23").replace("\\", "").replace("\"", "\\\""); //to avoid \n \r, ecc to avoid problems caused by ## and "" inside ""
							dt = "@" + splitted3[1].substring(0, splitted3[1].length() - 2);
							return new Triple("dt_relational", s, p, o+dt);
						} else {
							o = splitted3[0].substring(0, splitted3[0].length() - 2);
							o = o.replace("##", "%23%23").replace("\\", "").replace("\"", "\\\""); //to avoid \n \r, ecc to avoid problems caused by ## and "" inside ""
							return new Triple("dt_relational", s, p, o);
						}
					}
				}
	
				if (splitted[1].split("> <").length >= 2) {     // for objects like <ftp://ftp.microsoft.com/deskapps/kids/3dmm.exe> .
					splitted2 = splitted[1].split("> <");
					o = splitted2[1].substring(0, splitted2[1].length() - 3);
					p = "http" + splitted2[0];
					if (p.contains("<") || p.contains(">") || p.contains("\""))
						return new Triple("null","null","null","null");
					if (o.contains("<") || o.contains(">") || o.contains("\""))
						return new Triple("null","null","null","null");
					o = o.replace("##", "%23%23");
					return new Triple("object_relational", s, p, o);
				}
			}
	
			else if (splitted.length == 3) {               //type assertion or obj relational assertion
				p = "http" + splitted[1];
				o = "http" + splitted[2].substring(0, splitted[2].length() - 3);
				if (p.contains("<") || p.contains(">") || p.contains("\""))
					return new Triple("null","null","null","null");
				if (o.contains("<") || o.contains(">") || o.contains("\""))
					return new Triple("null","null","null","null");
				o = o.replace("##", "%23%23");
	
				if (p.equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"))
					return new Triple("typing", s, p, o);
				else
					return new Triple("object_relational", s, p, o);
			}
	
			return new Triple("null","null","null","null");
		}
		catch(Exception e) {
			return new Triple("null","null","null","null");
		}
	}
	
	
	public JavaRDD<Triple> filterTyping(JavaRDD<Triple> triples) {
		return triples.filter( triple -> triple.getType().equals("typing"));
	}
	
	public JavaRDD<Triple> filterObjectRelational(JavaRDD<Triple> triples) {
		return triples.filter( triple -> triple.getType().equals("object_relational"));
	}
	
	public JavaRDD<Triple> filterDatatypeRelational(JavaRDD<Triple> triples) {
		return triples.filter( triple -> triple.getType().equals("dt_relational"));
	}
}

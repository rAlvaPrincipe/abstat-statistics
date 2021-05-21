package application;

import java.io.Serializable;

public class Triple implements Serializable{

	private static final long serialVersionUID = -4064443521393072810L;
	private String type;
	private String subject;
	private String predicate;
	private String object;
	private String datatype;

	
	public Triple(String type, String subject, String predicate, String object, String datatype) {
		this.type = type;
		this.subject = subject;
		this.predicate = predicate;
		this.object = object;
		this.datatype = datatype;
	}
	
	public Triple(String type, String subject, String predicate, String object) {
		this(type, subject, predicate, object, null);
	}
	
	public String getSubject() {
		return subject;
	}
	public void setSubject(String subject) {
		this.subject = subject;
	}
	public String getPredicate() {
		return predicate;
	}
	public void setPredicate(String predicate) {
		this.predicate = predicate;
	}
	public String getObject() {
		return object;
	}
	public void setObject(String object) {
		this.object = object;
	}
	public String getDatatype() {
		return datatype;
	}
	public void setDatatype(String datatype) {
		this.datatype = datatype;
	}
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}

	@Override
	public String toString() {
		return "Triple [subject=" + subject + ", predicate=" + predicate + ", object=" + object + ", datatype="
				+ datatype + ", type=" + type + "]";
	}
}


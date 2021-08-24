package utility;

import org.springframework.stereotype.Service;

@Service
public interface FileCRUD {

	public String[] nextLine();
	
	boolean hasNextLine() throws Exception;

	void open(String path) throws Exception;
	
	String write(Object src, String dest);
	
	boolean delete(String file);
}

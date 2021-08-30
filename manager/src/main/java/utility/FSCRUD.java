package utility;

import java.io.File;
import java.io.InputStream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

@Service
@Profile("single-machine")
public class FSCRUD implements FileCRUD{

	private LineIterator lines;


	public void open(String path) throws Exception {
		this.lines = IOUtils.lineIterator(FileUtils.openInputStream(new File(path)), "UTF-8");
	}
	
	
	public String[] nextLine() {
		String line =  lines.nextLine();
		return line.split("##");
	}

	
	public boolean hasNextLine() throws Exception{
		return lines.hasNext();
	}
	
	
	public String write(Object src, String fileDest) {
		String dirPath = fileDest.substring(0, fileDest.lastIndexOf('/'));
		File dir = new File(dirPath);
		if (!dir.exists()) {
			if (dir.mkdirs()) System.out.println("Directory is created!");
			else System.out.println("Failed to create directory!");
		}
		try {
			FileUtils.copyInputStreamToFile((InputStream)src, new File(fileDest));
			return fileDest;
		}
		catch(Exception e) {
			return null;
		}
	}

	
	public boolean delete(String file) {
		try{
			FileUtils.forceDelete(new File(file));
			return true;
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		return false;
	}
}
package service;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import model.Metadata;

@Service
public class ExecutorServiceImpl implements ExecutorService {
	
	@Autowired
	MetadataService metadataService;
	
	
	public String submit(String id, String PLD) throws Exception {
		
		Metadata config = metadataService.findById(id);
		String datasetname = config.getDataset();
		String datasetPath = config.getPositionDataset(); 
		String output_dir = "/Users/filippocolombo/Downloads";
		output_dir += "/"+ datasetname;
		
		//Creating a File object
	    File file = new File(output_dir);

		if(!config.isCalculateStatistics()) {
		    //Creating the directory
			file.mkdir();
			String[] cmd = { "/bin/bash", "abstat-statistics/submit-job.sh", datasetPath, output_dir, PLD, datasetname};
			ProcessBuilder pb = new ProcessBuilder(cmd);
			pb.redirectErrorStream(true);
			pb.directory(new File(System.getProperty("user.dir")).getParentFile());
			Process p = pb.start();
			BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
			String line = "";
			while ((line = reader.readLine()) != null)
				System.out.println(line);
			if(p.waitFor() != 1) {
				config.setCalculateStatistics(true);
				config.setPositionStatistics(output_dir);
				metadataService.update(config);
			}
			else{
				file.delete();
				return "submit-job.sh exit with error";
			}
		}
		else 
			return "statistic already calculated";
	return "statistics calculated successfully";
	}
}
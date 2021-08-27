package service;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;

import org.codehaus.jackson.map.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import model.Dataset;
import model.Profile;

@Service
public class ExecutorServiceImpl implements ExecutorService {
	
	@Autowired
	DatasetService datasetService;
	
	@Autowired
	ProfileService profileService;
	
	
	public String submit(String id, String PLD) throws Exception {
		
		Dataset dataset = datasetService.findById(id);
		String datasetname = dataset.getDatasetName();
		String datasetPath = dataset.getDatasetPosition(); 
		String output_dir = "/Users/filippocolombo/abstat-statistics/Statistics/" + datasetname;
		
		String stat = "/Users/filippocolombo/abstat-statistics/Statistics/stat/" + datasetname + ".json";
		
		Profile profile = new Profile();
		ObjectMapper mapper = new ObjectMapper();
		
		//Creating a File object
	    File file = new File(output_dir);

		if(!dataset.isCalculateStatistics()) {
		    
			//Creating the directory
			file.mkdir();
			String[] cmd = { "/bin/bash", "abstat-statistics/submit-job.sh", datasetPath, output_dir, PLD};
			ProcessBuilder pb = new ProcessBuilder(cmd);
			pb.redirectErrorStream(true);
			pb.directory(new File(System.getProperty("user.dir")).getParentFile());
			Process p = pb.start();
			BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
			String line = "";
			while ((line = reader.readLine()) != null)
				System.out.println(line);
			if(p.waitFor() != 1) {

				// Convert JSON string from file to Object
				profile = mapper.readValue(new File(stat), Profile.class);
				profile.setIdDataset(dataset.getId());
				profile.setStatisticsPosition(output_dir);
				profileService.save(profile);
				dataset.setCalculateStatistics(true);
				datasetService.update(dataset);
			}
			else
				return "submit-job.sh exit with error";
		}
		else 
			return "statistic already calculated and saved";
	
		return "statistics calculated and saved successfully";
	}
}
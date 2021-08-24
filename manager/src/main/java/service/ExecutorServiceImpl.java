package service;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import model.Profiling;
import model.Dataset;

@Service
public class ExecutorServiceImpl implements ExecutorService {
	
	@Autowired
	DatasetService datasetService;
	
	@Autowired
	ProfilingService profilingService;

	
	public String submit(String id, String PLD) throws Exception {
		
		Dataset dataset = datasetService.findById(id);
		String datasetname = dataset.getDatasetName();
		String datasetPath = dataset.getDatasetPosition(); 
		String output_dir = "/Users/filippocolombo/Downloads";
		output_dir += "/" + datasetname + ".json" ;
		
		Profiling profiling = new Profiling();
		
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
				dataset.setCalculateStatistics(true);
				datasetService.update(dataset);
				
				profiling.setIdDataset(id);
				profiling.setDatasetName(datasetname);
				profiling.setStatisticsPosition(output_dir);
				profilingService.insert(profiling);
				
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
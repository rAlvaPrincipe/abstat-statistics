package controller;

import java.io.File;

import org.codehaus.jackson.map.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import model.Profile;
import model.Profiling;
import service.ProfilingService;
import service.ProfileService;

@RestController
public class ConsolidateAPI {
	
	@Autowired
	ProfilingService profilingService;
	
	@Autowired
	ProfileService profileService;
    
	
	@PostMapping(value = "/api/consolidate")
	public String consolidate(@RequestParam(value="id", required=true) String id) throws Exception{
		Profiling profiling = profilingService.findDatasetById(id);
        ObjectMapper mapper = new ObjectMapper();
        
        if(!profiling.isConsolidate()) {
        	// Convert JSON string from file to Object
        	Profile profile = mapper.readValue(new File(profiling.getStatisticsPosition()), Profile.class);
        	profile.setIdDataset(profiling.getIdDataset());
        	profileService.save(profile);
        	profiling.setConsolidate(true);
        	profilingService.update(profiling);
        	return "statistics saved";
        }
        else
        	return"statistics already saved";
	}
}
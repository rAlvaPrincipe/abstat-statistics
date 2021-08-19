package controller;

import java.io.File;

import org.codehaus.jackson.map.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import model.Statistics;
import model.Metadata;
import service.MetadataService;
import service.StatisticsService;

@RestController
public class ConsolidateAPI {
	
	@Autowired
	MetadataService metadataService;
	
	@Autowired
	StatisticsService statisticsService;
    
	
	@RequestMapping(value = "/api/consolidate", method = RequestMethod.POST)
	public void consolidate(@RequestParam(value="id", required=true) String id) throws Exception{
		Metadata config = metadataService.findById(id);
        ObjectMapper mapper = new ObjectMapper();
        
        // Convert JSON string from file to Object
        Statistics model = mapper.readValue(new File(config.getPositionStatistics()), Statistics.class);
		statisticsService.save(model);
	}
}
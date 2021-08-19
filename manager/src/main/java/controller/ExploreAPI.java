package controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import model.Metadata;
import model.Statistics;
import model.View;
import repository.MetadataRepository;
import service.MetadataService;
import service.StatisticsService;

@RestController
public class ExploreAPI {
	
	@Autowired
	StatisticsService statisticsService;
	
	@Autowired
	MetadataService metadataService;
	
	@Autowired
	MetadataRepository metadataRepository;
	
	
	@GetMapping(value = "/api/v1/statistics", produces = "application/json")
	public @ResponseBody Statistics explore(@RequestParam(value="id", required=true) String id) throws Exception{
		return statisticsService.findById(id);
	}
	
	
	@GetMapping(value = "/api/v1/datasets", produces = "application/json")
	public String dataset() throws Exception {	
		ObjectMapper mapper = new ObjectMapper();
		mapper.configure(MapperFeature.DEFAULT_VIEW_INCLUSION, false);
		List<Metadata> metadata = metadataRepository.findAll();

		// to enable pretty print
        mapper.enable(SerializationFeature.INDENT_OUTPUT);
        String View = mapper.writerWithView(View.Datasets.class).writeValueAsString(metadata);
        return View;
	}	
}
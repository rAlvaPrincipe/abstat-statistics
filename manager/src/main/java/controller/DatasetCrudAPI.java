package controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import model.Dataset;
import model.View;
import service.DatasetService;

@RestController
public class DatasetCrudAPI {
	
	@Autowired
	DatasetService datasetService;
	
	
	@GetMapping(value = "/api/v1/show/datasets", produces = "application/json")
	public String datasetShow() throws Exception {	
		ObjectMapper mapper = new ObjectMapper();
		mapper.configure(MapperFeature.DEFAULT_VIEW_INCLUSION, false);
		Iterable<Dataset> dataset = datasetService.read();

		// to enable pretty print
        mapper.enable(SerializationFeature.INDENT_OUTPUT);
        String View = mapper.writerWithView(View.Datasets.class).writeValueAsString(dataset);
        return View;
	}	

	@GetMapping(value = "/api/v1/delite/dataset", produces = "application/json")
	public void datasetDelite(@RequestParam(value="id", required=true) String id) throws Exception {	
		datasetService.delete(id);
	}
}
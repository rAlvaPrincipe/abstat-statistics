package controller;

import java.io.File;

import org.codehaus.jackson.map.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import model.FileModel;
import model.Metadata;
import service.FileModelService;
import service.MetadataService;

@RestController
public class Controller {
	
	@Autowired
	MetadataService metadataService;
	
	@Autowired
	FileModelService fileModelService;
    
	@PostMapping(value = "/api/consolidate")
	public void consolidate(@RequestParam(value="id", required=true) String id) throws Exception{
		
		Metadata config = metadataService.findById(id);
        ObjectMapper mapper = new ObjectMapper();

        // Convert JSON string from file to Object
        FileModel model = mapper.readValue(new File(config.getPosition()), FileModel.class);
        String prettymodel = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(model);
        System.out.println(prettymodel);
            
		fileModelService.save(model);
	}

   @GetMapping(value = "/api/v1/explore")
   public FileModel explore(@RequestParam(value="id", required=true) String id) throws Exception{
  
    	return fileModelService.findById(id);
   }
}
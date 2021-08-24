package controller;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.multipart.MultipartFile;


import model.Dataset;
import service.DatasetService;
import utility.FSCRUD;


@Controller
public class UploadAPI {
	
	@Autowired
	DatasetService datasetService;

	FSCRUD fsCRUD = new FSCRUD();
	private final String FS_FOLDER = "/Users/filippocolombo/abstat-statistics/Datasets";

	
	@RequestMapping(value = "/api/v1/upload/dataset")
	public String datasetUpload(@RequestParam("file") MultipartFile file) throws Exception {
		if (file.isEmpty()) {
			return ("The '" + file.getName() + "' is empty" );
        }
		
		InputStream stream = file.getInputStream();
		Dataset dataset = new Dataset();
		dataset.setDatasetName(file.getOriginalFilename());
		String dir_path = FS_FOLDER + "/" + dataset.getDatasetName();
		dataset.setDatasetPosition(fsCRUD.write(stream, dir_path));
		datasetService.update(dataset);
		
		return ("You successfully uploaded '" + file.getName() + "'");
    }
}
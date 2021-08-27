package controller;

import java.io.InputStream;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
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

	
	@PostMapping(value = "/api/v1/upload/dataset")
	public String datasetUpload(@RequestParam("file") MultipartFile file) throws Exception {
		if (file.isEmpty()) {
			return ("The '" + file.getName() + "' is empty" );
        }
		
		InputStream stream = file.getInputStream();
		Dataset dataset = new Dataset();
		dataset.setDatasetName(file.getOriginalFilename().replaceFirst("[.][^.]+$", ""));
		String dir_path = FS_FOLDER + "/" + file.getOriginalFilename();
		dataset.setDatasetPosition(fsCRUD.write(stream, dir_path));
		datasetService.update(dataset);
		
		return ("You successfully uploaded '" + file.getName() + "'");
    }
}
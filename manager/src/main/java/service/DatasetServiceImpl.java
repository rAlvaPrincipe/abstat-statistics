package service;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import model.Dataset;
import repository.DatasetRepository;

@Service
public class DatasetServiceImpl implements DatasetService {
	
	@Autowired
	DatasetRepository datasetRepository;
	
	
	public Dataset findById(String id) {
		return datasetRepository.findOne(id);
	}
	
	public Iterable<Dataset> read() {
		return datasetRepository.findAll();
	}
	
	public void update(Dataset dataset) {
		datasetRepository.save(dataset);
	}
	
	public void delete(String id) {
		datasetRepository.delete(id);
	}
}
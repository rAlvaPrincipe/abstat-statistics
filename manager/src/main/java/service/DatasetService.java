package service;


import org.springframework.stereotype.Service;

import model.Dataset;

@Service
public interface DatasetService {
	
	public Dataset findById(String id);
	
	public Iterable<Dataset> read();
	
	public void update(Dataset dataset);
	
	public void delete(String id);

}
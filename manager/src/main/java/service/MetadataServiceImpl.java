package service;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import model.Metadata;
import repository.MetadataRepository;

@Service
public class MetadataServiceImpl implements MetadataService {
	
	@Autowired
	MetadataRepository metadataRepository;
	
	
	public Metadata findById(String id) {
		return metadataRepository.findOne(id);
	}
	
	public List<Metadata> findAll() {
		return metadataRepository.findAll();
	}
	
	public void update(Metadata config) {
		metadataRepository.save(config);
	}
}
package service;

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
}
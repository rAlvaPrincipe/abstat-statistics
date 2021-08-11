package service;

import org.springframework.stereotype.Service;

import model.Metadata;

@Service
public interface MetadataService {
	
	public Metadata findById(String id);
}
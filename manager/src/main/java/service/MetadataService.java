package service;

import java.util.List;

import org.springframework.stereotype.Service;

import model.Metadata;

@Service
public interface MetadataService {
	public Metadata findById(String id);
	
	public List<Metadata> findAll();
	
	public void update(Metadata config);
}
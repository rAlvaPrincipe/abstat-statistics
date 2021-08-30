package service;

import org.springframework.stereotype.Service;

import model.Profile;

@Service
public interface ProfileService {

	public Profile findById(String id);
	
	public void save(Profile profile);
	
	public void delete(String id);
	
	public Iterable<Profile> read();
}
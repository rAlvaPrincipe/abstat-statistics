package service;

import java.io.IOException;

import org.springframework.stereotype.Service;

import model.Profile;

@Service
public interface ProfileService {

	public Profile findById(String id);
	
	public void save(Profile profile);
	
	public void delete(String id) throws IOException;
	
	public Iterable<Profile> read();
}
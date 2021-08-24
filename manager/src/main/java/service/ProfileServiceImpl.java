package service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import model.Profile;
import repository.ProfileRepository;

@Service
public class ProfileServiceImpl implements ProfileService {
	
	@Autowired
	ProfileRepository profileRepository;
	
	
	public Profile findById(String id) {
		return profileRepository.findOne(id);
	}

    public void save(Profile profile) {
        profileRepository.save(profile);
    }
    
	public void delete(String id) {
		profileRepository.delete(id);
	}
	
	public Iterable<Profile> read() {
		return profileRepository.findAll();
	}
}
package service;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import model.Dataset;
import model.Profile;
import repository.ProfileRepository;

@Service
public class ProfileServiceImpl implements ProfileService {
	
	@Autowired
	ProfileRepository profileRepository;
	
	@Autowired
	DatasetService datasetService;
	
	
	public Profile findById(String id) {
		return profileRepository.findOne(id);
	}

    public void save(Profile profile) {
        profileRepository.save(profile);
    }
    
	public void delete(String id) throws IOException {
		Profile profile = findById(id);
		Dataset dataset = datasetService.findById(profile.getIdDataset());
		FileUtils.forceDelete(new File(profile.getStatisticsPosition()));
		dataset.setCalculateStatistics(false);
		datasetService.update(dataset);
		profileRepository.delete(id);
	}
	
	public Iterable<Profile> read() {
		return profileRepository.findAll();
	}
}
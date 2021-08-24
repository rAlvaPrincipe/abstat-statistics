package service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import model.Profiling;
import repository.ProfilingRepository;

@Service
public class ProfilingServiceImpl implements ProfilingService {
	
	@Autowired
	ProfilingRepository profilingRepository;
	
	
	public Profiling findDatasetById(String id) {
		return profilingRepository.findOne(id);
	}
	
	public void update(Profiling profiling) {
		profilingRepository.save(profiling);
	}
	
	public void insert(Profiling profiling) {
        profilingRepository.insert(profiling);
    }
}
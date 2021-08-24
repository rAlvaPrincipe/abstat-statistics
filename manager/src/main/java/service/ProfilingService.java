package service;

import org.springframework.stereotype.Service;

import model.Profiling;

@Service
public interface ProfilingService {
	
	public Profiling findDatasetById(String id);
	
	public void update(Profiling config);
	
	public void insert(Profiling profiling);
}
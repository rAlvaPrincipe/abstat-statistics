package service;

import org.springframework.stereotype.Service;

import model.Statistics;

@Service
public interface StatisticsService {

	public Statistics findById(String id);
	
	public Statistics save(Statistics statistics);
}
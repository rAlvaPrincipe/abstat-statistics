package service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import model.Statistics;
import repository.StatisticsRepository;

@Service
public class StatisticsServiceImpl implements StatisticsService {
	
	@Autowired
	StatisticsRepository statisticsRepository;
	
	
	public Statistics findById(String id) {
		return statisticsRepository.findOne(id);
	}

    public Statistics save(Statistics statistics) {
        return statisticsRepository.save(statistics);
    }
}
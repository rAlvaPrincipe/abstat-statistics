package service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import model.FileModel;
import repository.StatisticsRepository;

@Service
public class FileModelServiceImpl implements FileModelService {
	
	@Autowired
	private StatisticsRepository statisticsRepository;
	
	public FileModel findById(String id) {
		return statisticsRepository.findOne(id);
	}

    public FileModel save(FileModel fileModel) {
        return statisticsRepository.save(fileModel);
    }
}
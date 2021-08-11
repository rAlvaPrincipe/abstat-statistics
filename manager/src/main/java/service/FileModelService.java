package service;

import org.springframework.stereotype.Service;

import model.FileModel;

@Service
public interface FileModelService {

	public FileModel findById(String id);

    public FileModel save(FileModel fileModel);
}
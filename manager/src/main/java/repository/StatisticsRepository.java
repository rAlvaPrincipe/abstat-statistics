package repository;

import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import model.FileModel;

@Repository
public interface StatisticsRepository extends MongoRepository<FileModel, String> {
}
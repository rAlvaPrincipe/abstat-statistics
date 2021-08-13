package repository;

import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import model.Statistics;

@Repository
public interface StatisticsRepository extends MongoRepository<Statistics, String> {
}
package repository;

import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import model.Profiling;

@Repository
public interface ProfilingRepository extends MongoRepository<Profiling, String> {
}

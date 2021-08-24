package repository;

import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import model.Dataset;

@Repository
public interface DatasetRepository extends CrudRepository<Dataset, String> {
}
package repository;

import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import model.Metadata;

@Repository
public interface MetadataRepository extends MongoRepository<Metadata, String> {
}
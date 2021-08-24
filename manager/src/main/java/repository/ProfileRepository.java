package repository;

import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import model.Profile;

@Repository
public interface ProfileRepository extends CrudRepository<Profile, String> {
}
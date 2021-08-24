package controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import model.Profile;
import service.ProfileService;

@RestController
public class ProfileCrudAPI {

	@Autowired
	ProfileService profileService;
	
	
	@GetMapping(value = "/api/v1/show/profileById", produces = "application/json")
	public Profile showProfileById(@RequestParam(value="id", required=true) String id) throws Exception{
		return profileService.findById(id);
	}
	
	@GetMapping(value = "/api/v1/delete/profileById")
	public void deleteProfileById(@RequestParam(value="id", required=true) String id) throws Exception{
		profileService.delete(id);
	}
	
	@GetMapping(value = "/api/v1/show/profile", produces = "application/json")
	public Iterable<Profile> showAllProfile() throws Exception{
		return profileService.read();
	}
}
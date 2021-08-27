package controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import model.Profile;
import model.View;
import service.ProfileService;

@RestController
public class ProfileCrudAPI {

	@Autowired
	ProfileService profileService;
	
	
	@GetMapping(value = "/api/v1/show/profile", produces = "application/json")
	public Profile showProfileById(@RequestParam(value="id", required=true) String id) throws Exception{
		return profileService.findById(id);
	}
	
	@GetMapping(value = "/api/v1/delete/profile")
	public void deleteProfileById(@RequestParam(value="id", required=true) String id) throws Exception{
		profileService.delete(id);
	}
	
	@GetMapping(value = "/api/v1/show/profiles", produces = "application/json")
	public String showAllProfile() throws Exception{
		ObjectMapper mapper = new ObjectMapper();
		mapper.configure(MapperFeature.DEFAULT_VIEW_INCLUSION, false);
		Iterable<Profile> profile = profileService.read();

		// to enable pretty print
        mapper.enable(SerializationFeature.INDENT_OUTPUT);
        String viewProfiles = mapper.writerWithView(View.Profiles.class).writeValueAsString(profile);
        return viewProfiles;
	}	
}
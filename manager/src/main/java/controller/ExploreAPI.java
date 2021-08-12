package controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import model.Statistics;
import service.StatisticsService;

@RestController
public class ExploreAPI {
	
	@Autowired
	StatisticsService statisticsService;
	
	
	@GetMapping(value = "/api/v1/statistics", produces = "application/json")
	public @ResponseBody Statistics explore(@RequestParam(value="id", required=true) String id) throws Exception{
		return statisticsService.findById(id);
	}
}
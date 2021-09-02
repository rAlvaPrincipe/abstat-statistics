package controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import service.ExecutorService;

@RestController
public class ExecutorAPI {
	
	@Autowired
	ExecutorService executorService;
		

		@PostMapping(value = "/api/v1/profiling")
		public String execute(@RequestParam(value="id", required=true) String id, @RequestParam(value="PLD", required=true) String PLD) throws Exception {
			 return executorService.submit(id, PLD);
		}
}
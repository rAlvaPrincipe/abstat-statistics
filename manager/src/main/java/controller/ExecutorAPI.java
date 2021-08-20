package controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import service.ExecutorService;

@RestController
public class ExecutorAPI {
	
	@Autowired
	ExecutorService executorService;
		

		@RequestMapping(value = "/api/v1/compute", method = RequestMethod.POST)
		public String compute(@RequestParam(value="id", required=true) String id, @RequestParam(value="PLD", required=true) String PLD) throws Exception {
			 return executorService.submit(id, PLD);
		}
}
package service;

import org.springframework.stereotype.Service;

@Service
public interface ExecutorService {
	public String submit(String id, String PLD) throws Exception;
	
	
}

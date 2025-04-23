package com.softwareag.signalmigration.rest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import com.cumulocity.rest.representation.inventory.ManagedObjectRepresentation;
import com.softwareag.signalmigration.model.MigrationJob;
import com.softwareag.signalmigration.model.MigrationJobConfig;
import com.softwareag.signalmigration.service.MigrationJobService;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@RestController
public class MigrationJobRESTContoller {

	@Autowired
	private MigrationJobService migrationJobService;
	
	@PostMapping(value = "/migrationjob", 
    		consumes = MediaType.APPLICATION_JSON_VALUE,
    		produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(code = HttpStatus.CREATED)    
	public ResponseEntity<?> createMigrationJob(@RequestBody MigrationJobConfig config) {
    	log.info("POST /migrationjob");
    	try{
    		MigrationJob job = new MigrationJob(config);
    		
    		ManagedObjectRepresentation jobMO = migrationJobService.runMigrationJob(job);
    		
    		return ResponseEntity
    				.status(HttpStatus.CREATED)
    				.body(jobMO);
    	}
    	catch(Exception e) {
    		return ResponseEntity
    				.status(HttpStatus.INTERNAL_SERVER_ERROR)
    				.body(e.getMessage());
    	}
    }
	
	@PostMapping(value = "/migrationjob/retry/{jobId}", 
    		produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(code = HttpStatus.CREATED)
	public ResponseEntity<?> retryMigrationJob(@PathVariable String jobId,
			 @RequestParam(name = "force", required = false, defaultValue = "false") boolean force) {
    	log.info("POST /migrationjob/retry/{}", jobId);
    	try{
    		MigrationJob job = migrationJobService.loadJob(jobId);
    		migrationJobService.retryMigrationJob(job, force);
    		
    		return ResponseEntity
    				.status(HttpStatus.CREATED)
    				.body(null);
    	}
    	catch(Exception e) {
    		return ResponseEntity
    				.status(HttpStatus.INTERNAL_SERVER_ERROR)
    				.body(e.getMessage());
    	}
    }
	
	
}

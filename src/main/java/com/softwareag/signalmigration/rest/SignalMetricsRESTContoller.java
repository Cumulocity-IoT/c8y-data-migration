package com.softwareag.signalmigration.rest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import com.cumulocity.sdk.client.Platform;
import com.softwareag.signalmigration.model.GroupSignalMetrics;
import com.softwareag.signalmigration.model.SignalMetricsCollectionRequestDTO;
import com.softwareag.signalmigration.service.PlatformUtil;
import com.softwareag.signalmigration.service.SignalMetricCollectionService;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@RestController
public class SignalMetricsRESTContoller {

	@Autowired
	private SignalMetricCollectionService signalMetricCollectionService;
	
	@Autowired
	private PlatformUtil platformUtil;
	
	/**
	 * To convert the returned json to device list csv:
	 * 	cat acms-devices-Mar-10-to-13.json | jq '.deviceSignalMetrics' | jq -r '(.[0] | keys_unsorted), (.[] | [.[]]) | @csv' | tee acms-devices-Mar-10-to-13.csv
	 * To check which devices are active: in excel csv
	 * 	- create table, add new column 'has signals',  formula: =[@count]>0
	 *  - filter signal type to event and measurement (no alarm - could be unavailability)
	 *  - filte 'has signals' to true
	 *  - copy device id list to new tab and deduplicate - this gives active devices 
	 * @param collectionRequest
	 * @return
	 */
	@PostMapping(value = "signalmetrics/collect", 
    		consumes = MediaType.APPLICATION_JSON_VALUE,
    		produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(code = HttpStatus.CREATED)    
	public ResponseEntity<?> collectSignalMetrics(@RequestBody SignalMetricsCollectionRequestDTO  collectionRequest) {
    	log.info("POST /collectSignalMetrics");
    	try{
    		Platform srcPlatform = platformUtil.resolvePlatform(collectionRequest.getSourcePlatformHost(), collectionRequest.getSourcePlatformLoginString());
    		
    		GroupSignalMetrics groupSignalMetrics = signalMetricCollectionService.collectSignalMetrics(collectionRequest.getSourceDevicesQuery(),
    				srcPlatform, collectionRequest.getDateFrom(), collectionRequest.getDateTo());
    		
    		groupSignalMetrics.setCollectionConfig(collectionRequest);
    		
    		return ResponseEntity
    				.status(HttpStatus.OK)
    				.body(groupSignalMetrics);
    	}
    	catch(Exception e) {
    		return ResponseEntity
    				.status(HttpStatus.INTERNAL_SERVER_ERROR)
    				.body(e.getMessage());
    	}
    }	
	
}

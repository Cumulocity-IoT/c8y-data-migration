package com.softwareag.signalmigration.model;

import org.joda.time.DateTime;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class SignalMetricsCollectionRequestDTO {
	
	private String sourcePlatformLoginString; // TODO might be best encrypt credentials, could add endpoint for that	
	
	private String sourcePlatformHost;
	
	/**
	 * e.g.
	 * "$filter=(has('foo') and has('c8y_IsDevice'))"
	 * 
	 */
	private String sourceDevicesQuery;
	
	
	/**
	 * format: "2021-07-04T05:03:23.157Z" 
	 */
	private DateTime dateFrom;
	
	private DateTime dateTo;
	
}

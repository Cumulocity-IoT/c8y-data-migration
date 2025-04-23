package com.softwareag.signalmigration.model;

import com.cumulocity.sdk.client.Filter;
import com.cumulocity.sdk.client.QueryParam;
import com.cumulocity.sdk.client.alarm.AlarmFilter;
import com.cumulocity.sdk.client.event.EventFilter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import com.cumulocity.sdk.client.measurement.MeasurementFilter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.softwareag.signalmigration.util.CustomQueryParam;
import com.softwareag.signalmigration.util.DateUtil;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Builder
@Data
@NoArgsConstructor
@AllArgsConstructor
public class MigrationJobConfig {
	
	private String jobName;
	
	private SignalType signalType;
	
	/**
	 * <tenantId>/<username>:<password>
	 */
	@ToString.Exclude
	private String sourcePlatformLoginString; // TODO might be best encrypt credentials, could add endpoint for that	
	
	private String sourcePlatformHost;
		
	private String targetPlatformLoginString;
	
	private String targetPlatformHost; //  if LOCAL_PLATFORM_URL is supplied, no credentials needed 	
	
	/**
	 * e.g.
	 * "$filter=(has('foo') and has('c8y_IsDevice'))"
	 * 
	 */
	private String sourceDevicesQuery;
	
	
	/**
	 * format: "2021-07-04T05:03:23.157Z" 
	 */
	protected String dateFrom;
	
	protected String dateTo;
	
	/**
	 * 
	 * Optionally, the user can explicitly supply external id mapping where the source external id is different from the target external id
	 * This field is not required.
	 * 
	 * Note:to easily generate mapping json from a csv file, the miller linux tool can be used:
	 * mlr --icsv --ojson cat ext-id-mapping-advice.csv > ext-id-mapping-advice.json
	 */
	@ToString.Exclude
	protected ArrayList<ExternalIdMappingAdvice> externalIdMappingAdvice;
	
	@JsonIgnore
	public Filter getSignalFilter() {
		Date from = DateUtil.parser.parseDateTime(dateFrom).toDate();
		Date to = DateUtil.parser.parseDateTime(dateTo).toDate();		
		switch (signalType) {
		
		case MEASUREMENT: {			
			return new MeasurementFilter()
		    		.byDate(from, to);
		}
		case EVENT: {
			return new EventFilter().byDate(from, to);
		}
		case ALARM: {
			return new AlarmFilter().byDate(from, to);
		}
		
		default:
			throw new IllegalArgumentException("Unexpected value: " + signalType);
		}
	}
	
	
	@JsonIgnore
	public List<QueryParam> getSignalQueryParams() {

		// API gives error if there is a timezone(?!), so we convert to Z
		//"2021-02-01T01:05:07.513+01:00" -> err
		//"2021-07-04T05:03:23.157Z" -> OK		

		List<QueryParam> params = Arrays.asList(
				CustomQueryParam.DATE_FROM.setValue(DateUtil.toISODateTimeString(dateFrom)).toQueryParam(),
				CustomQueryParam.DATE_TO.setValue(DateUtil.toISODateTimeString(dateTo)).toQueryParam());
		
		return params;
	}
}

package com.softwareag.signalmigration.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import org.joda.time.DateTime;

import com.cumulocity.rest.representation.PageStatisticsRepresentation;
import com.cumulocity.rest.representation.alarm.AlarmRepresentation;
import com.cumulocity.sdk.client.QueryParam;
import com.cumulocity.sdk.client.alarm.AlarmApi;
import com.cumulocity.sdk.client.alarm.PagedAlarmCollectionRepresentation;
import com.cumulocity.sdk.client.measurement.MeasurementApi;
import com.softwareag.signalmigration.model.DeviceSignalMetrics;
import com.softwareag.signalmigration.model.DeviceSignalMetrics.DeviceSignalMetricsBuilder;
import com.softwareag.signalmigration.model.SignalType;

public class AlarmUtil {

	public static Iterable<AlarmRepresentation> getAlarms(String sourceDeviceId,
			List<QueryParam> signalQueryParams, AlarmApi alarmApi) {
		
		
		ArrayList<QueryParam> params = new ArrayList<QueryParam>(signalQueryParams);
		params.add(CustomQueryParam.SOURCE.setValue(sourceDeviceId).toQueryParam());
		
		
		Iterable<AlarmRepresentation> alarms = 
				alarmApi.getAlarms().get(500, params.toArray(new QueryParam[0])).allPages();
		
		return alarms;
	}
	
	/**
	 * 
	 * @param sm
	 * @param tm
	 * @return
	 */
	public static boolean equalIgnoringTenantSpecificFields(AlarmRepresentation r1,
			AlarmRepresentation r2) {
		int hash1 = Objects.hash(r1.getAttrs(), r1.getType(), r1.getDateTime(), r1.getFirstOccurrenceDateTime());
		int hash2 = Objects.hash(r2.getAttrs(), r2.getType(), r2.getDateTime(), r2.getFirstOccurrenceDateTime());		
		return hash1 ==  hash2;
	}
	
	/**
	 * 
	 * // create a uniqueHash - that uniquely identifies the measurement 
				// across tenants; It will include time, type, etc.. (but NOT the id & creationTime 
				// - could be done by removing/nulling some fields of the source signal, like 
				// id, self, owner). The purpose of the uniqueHash is to check if the signal already 
				// exists in the target tenant. This allows migration jobs to restart if the service 
				// crashes, and not create duplicate copies.
				//
				// Note: duplicates may already exist on the source tenant. We want to copy them all to 
				// the target tenant,
				// i.e. we need to count the source duplicates and create as many target duplicates.
				//	
	 * 
	 * a hash that uniquely identifies the measurement
	 *  across tenants; It will include time, type, etc.. (but NOT tenant-dependent things like self, id, source;
	 *  will also not include creationTime
	 *   
	 *  NOTE: will modify the msmt!
	 *   
	 * @param msmtRep
	 * @return 
	 */
	public static int generateUniqueHash(AlarmRepresentation alarmRep) {
		String json = alarmRep.toJSON();
		return json.hashCode();
	}


	public static void trimTenantSpecificFields(AlarmRepresentation alarm) {
		alarm.setSelf(null);
		alarm.setId(null);		
		alarm.setSource(null);
		alarm.setLastUpdatedDateTime(null);
		alarm.setCreationDateTime(null);
		alarm.setHistory(null);
	}
	
	public static DeviceSignalMetrics getMetrics(String deviceId, AlarmApi alarmApi) {
		DateTime dateFrom = DateTime.now().minusYears(1000);
		DateTime dateTo = DateTime.now().plusYears(1000);
		
		return getMetrics(deviceId, alarmApi, dateFrom, dateTo);
	}

	public static DeviceSignalMetrics getMetrics(String deviceId, AlarmApi alarmApi, DateTime dateFrom, DateTime dateTo) {
		
		List<QueryParam> params = Arrays.asList(
				CustomQueryParam.DATE_FROM.setValue(DateUtil.toISODateTimeString(dateFrom)).toQueryParam(),
				CustomQueryParam.DATE_TO.setValue(DateUtil.toISODateTimeString(dateTo)).toQueryParam(),
				CustomQueryParam.SOURCE.setValue(deviceId).toQueryParam(),
				CustomQueryParam.WITH_TOTAL_PAGES.setValue("true").toQueryParam()
				);
		
		
		DeviceSignalMetricsBuilder builder = DeviceSignalMetrics.builder();
		builder.signalType(SignalType.ALARM)
			.deviceId(deviceId);
		
		PagedAlarmCollectionRepresentation collRep = alarmApi.getAlarms().get(1, params.toArray(new QueryParam[0]));
		
		PageStatisticsRepresentation pageStatistics = collRep.getPageStatistics();
		Integer totalPages = pageStatistics.getTotalPages();		
		builder.count(totalPages);
		
		if (totalPages > 1) {
			//need to get the last page - the alarms are newest first, revert has no effect
			params = new ArrayList<>(params);
			params.add(CustomQueryParam.CURRENT_PAGE.setValue(totalPages.toString()).toQueryParam());
			collRep = alarmApi.getAlarms().get(1, params.toArray(new QueryParam[0]));
		}
		
		Iterable<AlarmRepresentation> signals = collRep.allPages();
		if (signals.iterator().hasNext()) {
			AlarmRepresentation next = signals.iterator().next();
			builder.dateFrom(next.getDateTime());
		}
		
		
		return builder.build();

	}
	
}

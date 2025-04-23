package com.softwareag.signalmigration.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import org.joda.time.DateTime;

import com.cumulocity.rest.representation.PageStatisticsRepresentation;
import com.cumulocity.rest.representation.alarm.AlarmRepresentation;
import com.cumulocity.rest.representation.event.EventRepresentation;
import com.cumulocity.sdk.client.QueryParam;
import com.cumulocity.sdk.client.alarm.AlarmApi;
import com.cumulocity.sdk.client.alarm.PagedAlarmCollectionRepresentation;
import com.cumulocity.sdk.client.event.EventApi;
import com.cumulocity.sdk.client.event.PagedEventCollectionRepresentation;
import com.softwareag.signalmigration.model.DeviceSignalMetrics;
import com.softwareag.signalmigration.model.SignalType;
import com.softwareag.signalmigration.model.DeviceSignalMetrics.DeviceSignalMetricsBuilder;

public class EventUtil {
	
	public static Iterable<EventRepresentation> getEvents(String sourceDeviceId,
			List<QueryParam> signalQueryParams, EventApi eventApi) {
		ArrayList<QueryParam> params = new ArrayList<QueryParam>(signalQueryParams);
		params.add(CustomQueryParam.SOURCE.setValue(sourceDeviceId).toQueryParam());
		
		
		Iterable<EventRepresentation> evts = eventApi
				.getEvents().get(500, params.toArray(new QueryParam[0])).allPages();
		return evts;
	}
	
	/**
	 * 
	 * @param e1
	 * @param e2
	 * @return
	 */
	public static boolean equalIgnoringTenantSpecificFields(EventRepresentation e1,
			EventRepresentation e2) {
		int hash1 = Objects.hash(e1.getAttrs(), e1.getType(), e1.getDateTime(), e1.getText());
		int hash2 = Objects.hash(e2.getAttrs(), e2.getType(), e2.getDateTime(), e2.getText());		
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
	 *  NOTE: will modify the evt!
	 *   
	 * @param evt
	 * @return 
	 */
	public static int generateUniqueHash(EventRepresentation evt) {
		trimTenantSpecificFields(evt);
		//return Objects.hash(msmtRep.getAttrs(), msmtRep.getType(), msmtRep.getTime());
		String json = evt.toJSON();
		return json.hashCode();
	}

	public static void trimTenantSpecificFields(EventRepresentation evt) {
		evt.setSelf(null);
		evt.setId(null);		
		evt.setSource(null);
		evt.setCreationDateTime(null);
		evt.setLastUpdatedDateTime(null);
	}
	
	public static DeviceSignalMetrics getMetrics(String deviceId, EventApi eventApi) {
		DateTime dateFrom = DateTime.now().minusYears(1000);
		DateTime dateTo = DateTime.now().plusYears(1000);
		
		return getMetrics(deviceId, eventApi, dateFrom, dateTo);
	}

	public static DeviceSignalMetrics getMetrics(String deviceId, EventApi eventApi, DateTime dateFrom, DateTime dateTo) {
		
		List<QueryParam> params = Arrays.asList(
				CustomQueryParam.DATE_FROM.setValue(DateUtil.toISODateTimeString(dateFrom)).toQueryParam(),
				CustomQueryParam.DATE_TO.setValue(DateUtil.toISODateTimeString(dateTo)).toQueryParam(),
				CustomQueryParam.SOURCE.setValue(deviceId).toQueryParam(),
				CustomQueryParam.WITH_TOTAL_PAGES.setValue("true").toQueryParam(),
				CustomQueryParam.REVERT.setValue("true").toQueryParam()
				);
		
		 /*
		  
		  If you are using a range query (that is, at least one of the dateFrom or dateTo parameters is included in the request), 
		  then setting revert=true will sort the results by the oldest events first. By default, the results are 
		  sorted by the newest events first.
		  
		   Opposite to msmts!
		  */
		
		DeviceSignalMetricsBuilder builder = DeviceSignalMetrics.builder();
		builder.signalType(SignalType.EVENT)
			.deviceId(deviceId);
		
		PagedEventCollectionRepresentation collRep = eventApi.getEvents().get(1, params.toArray(new QueryParam[0]));
		
		PageStatisticsRepresentation pageStatistics = collRep.getPageStatistics();
		Integer totalPages = pageStatistics.getTotalPages();
		
		builder.count(totalPages);
		
		Iterable<EventRepresentation> signals = collRep.allPages();
		if (signals.iterator().hasNext()) {
			EventRepresentation next = signals.iterator().next();
			builder.dateFrom(next.getDateTime());
		}
		
		
		return builder.build();

	}

}

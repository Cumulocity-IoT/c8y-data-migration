package com.softwareag.signalmigration.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import org.joda.time.DateTime;

import com.cumulocity.model.idtype.GId;
import com.cumulocity.rest.representation.PageStatisticsRepresentation;
import com.cumulocity.rest.representation.measurement.MeasurementRepresentation;
import com.cumulocity.sdk.client.Platform;
import com.cumulocity.sdk.client.QueryParam;
import com.cumulocity.sdk.client.measurement.MeasurementApi;
import com.cumulocity.sdk.client.measurement.PagedMeasurementCollectionRepresentation;
import com.softwareag.signalmigration.model.DeviceSignalMetrics;
import com.softwareag.signalmigration.model.DeviceSignalMetrics.DeviceSignalMetricsBuilder;
import com.softwareag.signalmigration.model.SignalType;

public class MeasurementUtil {

	public static Iterable<MeasurementRepresentation> getMeasurements(String sourceDeviceId,
			List<QueryParam> signalQueryParams, MeasurementApi measurementApi) {
		ArrayList<QueryParam> params = new ArrayList<QueryParam>(signalQueryParams);
		params.add(CustomQueryParam.SOURCE.setValue(sourceDeviceId).toQueryParam());
		
		
		Iterable<MeasurementRepresentation> sourceMsmts = measurementApi
				.getMeasurements().get(500, params.toArray(new QueryParam[0])).allPages();
		return sourceMsmts;
	}
	
	/**
	 * 
	 * @param sm
	 * @param tm
	 * @return
	 */
	public static boolean equalIgnoringTenantSpecificFields(MeasurementRepresentation m1,
			MeasurementRepresentation m2) {
		int hash1 = Objects.hash(m1.getAttrs(), m1.getType(), m1.getDateTime());
		int hash2 = Objects.hash(m2.getAttrs(), m2.getType(), m2.getDateTime());		
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
	public static int generateUniqueHash(MeasurementRepresentation msmtRep) {
		trimTenantSpecificFields(msmtRep);
		//return Objects.hash(msmtRep.getAttrs(), msmtRep.getType(), msmtRep.getTime());
		String json = msmtRep.toJSON();
		return json.hashCode();
	}

	public static void trimTenantSpecificFields(MeasurementRepresentation msmt) {
		msmt.setSelf(null);
		msmt.setId(null);		
		msmt.setSource(null);
	}
	
	public static DeviceSignalMetrics getMetrics(String deviceId, MeasurementApi measurementApi) {
		DateTime dateFrom = DateTime.now().minusYears(1000);
		DateTime dateTo = DateTime.now().plusYears(1000);
		
		return getMetrics(deviceId, measurementApi, dateFrom, dateTo);
	}

	public static DeviceSignalMetrics getMetrics(String deviceId, MeasurementApi measurementApi, DateTime dateFrom, DateTime dateTo) {
		List<QueryParam> params = Arrays.asList(
				CustomQueryParam.DATE_FROM.setValue(DateUtil.toISODateTimeString(dateFrom)).toQueryParam(),
				CustomQueryParam.DATE_TO.setValue(DateUtil.toISODateTimeString(dateTo)).toQueryParam(),
				CustomQueryParam.SOURCE.setValue(deviceId).toQueryParam(),
				CustomQueryParam.WITH_TOTAL_PAGES.setValue("true").toQueryParam()
				);
		
		
		DeviceSignalMetricsBuilder builder = DeviceSignalMetrics.builder();
		builder.signalType(SignalType.MEASUREMENT)
			.deviceId(deviceId);
		
		PagedMeasurementCollectionRepresentation collRep 
			= measurementApi.getMeasurements().get(1, params.toArray(new QueryParam[0]));
		
		PageStatisticsRepresentation pageStatistics = collRep.getPageStatistics();
		Integer totalPages = pageStatistics.getTotalPages();
		
		builder.count(totalPages);
		
		Iterable<MeasurementRepresentation> signals = collRep.allPages();
		if (signals.iterator().hasNext()) {
			MeasurementRepresentation next = signals.iterator().next();
			builder.dateFrom(next.getDateTime());
		}
		
		
		return builder.build();
	}


}

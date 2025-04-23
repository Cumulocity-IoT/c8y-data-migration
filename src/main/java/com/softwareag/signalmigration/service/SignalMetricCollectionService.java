package com.softwareag.signalmigration.service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.joda.time.DateTime;
import org.springframework.stereotype.Component;

import com.cumulocity.rest.representation.inventory.ManagedObjectRepresentation;
import com.cumulocity.sdk.client.Platform;
import com.cumulocity.sdk.client.inventory.InventoryFilter;
import com.softwareag.signalmigration.model.DeviceSignalMetrics;
import com.softwareag.signalmigration.model.GroupSignalMetrics;
import com.softwareag.signalmigration.model.SignalMetricsCollectionRequestDTO;
import com.softwareag.signalmigration.model.SignalType;
import com.softwareag.signalmigration.util.AlarmUtil;
import com.softwareag.signalmigration.util.CustomInventoryFilter;
import com.softwareag.signalmigration.util.EventUtil;
import com.softwareag.signalmigration.util.MeasurementUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class SignalMetricCollectionService {
	

	public GroupSignalMetrics collectSignalMetrics(String sourceDevicesQuery,
			Platform sourceC8yPlatform, DateTime dateFrom, DateTime dateTo) throws InterruptedException {
		List<DeviceSignalMetrics> deviceSignalMetrics = collectDeviceSignalMetrics(sourceDevicesQuery, sourceC8yPlatform, dateFrom, dateTo);
		
//		LongSummaryStatistics statistics = deviceSignalMetrics.stream().collect(Collectors.summarizingLong(DeviceSignalMetrics::getCount));
		LongSummaryStatistics alarmStatistics = deviceSignalMetrics.stream()
				.filter(dsm -> dsm.getSignalType().equals(SignalType.ALARM))
				.collect(Collectors.summarizingLong(DeviceSignalMetrics::getCount));
		
		LongSummaryStatistics eventStatistics = deviceSignalMetrics.stream()
				.filter(dsm -> dsm.getSignalType().equals(SignalType.EVENT))
				.collect(Collectors.summarizingLong(DeviceSignalMetrics::getCount));
		
		LongSummaryStatistics msmtStatistics = deviceSignalMetrics.stream()
				.filter(dsm -> dsm.getSignalType().equals(SignalType.MEASUREMENT))
				.collect(Collectors.summarizingLong(DeviceSignalMetrics::getCount));
		
		SignalMetricsCollectionRequestDTO config = SignalMetricsCollectionRequestDTO.builder()
			.dateFrom(dateFrom)
			.dateTo(dateTo)
			.sourceDevicesQuery(sourceDevicesQuery)
			.build();
		
		GroupSignalMetrics groupSignalMetrics = GroupSignalMetrics.builder()
			.collectionConfig(config)
			.alarmStatistics(alarmStatistics)
			.eventStatistics(eventStatistics)
			.measurementStatistics(msmtStatistics)
			.deviceSignalMetrics(deviceSignalMetrics)
			.build();
		
		return groupSignalMetrics;
		
	}
	

	public List<DeviceSignalMetrics> collectDeviceSignalMetrics(String sourceDevicesQuery,
			Platform sourceC8yPlatform, DateTime dateFrom, DateTime dateTo) throws InterruptedException {
		
		ExecutorService executorService = Executors.newFixedThreadPool(20);
		
		InventoryFilter filter = new CustomInventoryFilter().byQuery(sourceDevicesQuery);
		
		Iterable<ManagedObjectRepresentation> sourceDevicesItor = sourceC8yPlatform.getInventoryApi().getManagedObjectsByFilter(filter).get(200).allPages();
		
		List<DeviceSignalMetrics> metricsList = Collections.synchronizedList(new ArrayList<DeviceSignalMetrics>());
		
		for (ManagedObjectRepresentation sourceDevice : sourceDevicesItor) {
			try {
				executorService.execute( ()-> {
					ArrayList<DeviceSignalMetrics> metrics = collectDeviceMetrics(sourceDevice.getId().getValue(), sourceC8yPlatform, dateFrom, dateTo);
					metricsList.addAll(metrics);
					log.info("Colleced metrics: {}", metricsList.size());
					
				});
				
			} catch (Exception e) {
				log.error("Error", e);
				throw e;
			}
		}
		
		try {
			executorService.shutdown();
			boolean done = executorService.awaitTermination(30, TimeUnit.MINUTES);
			log.info("Tasks finished, timed out: " + !done);
		} catch (InterruptedException e) {
			e.printStackTrace();
			throw e;
		}
		
		metricsList.sort(this::compareMetrics);
		
		return metricsList;
	}
	
	private int compareMetrics(DeviceSignalMetrics o1, DeviceSignalMetrics o2) {
					DateTime dt1 = o1.getDateFrom();
			DateTime dt2 = o2.getDateFrom();
			if (dt1 == null && dt2 == null) {
	            return 0;
	        }
	        if (dt2 == null) {
	            return -1;
	        }

	        if (dt1 == null) {
	            return 1;
	        }
			return dt1.compareTo(dt2);
		
	}

	private ArrayList<DeviceSignalMetrics> collectDeviceMetrics(String deviceId, Platform sourceC8yPlatform, DateTime dateFrom, DateTime dateTo) {
		log.info("Collecting metrics of device " + deviceId);
		
		ArrayList<DeviceSignalMetrics> metricsList = new ArrayList<>();
		
		DeviceSignalMetrics metrics = MeasurementUtil.getMetrics(deviceId, sourceC8yPlatform.getMeasurementApi(), dateFrom, dateTo);
		metricsList.add(metrics);
		
		metrics = AlarmUtil.getMetrics(deviceId, sourceC8yPlatform.getAlarmApi(), dateFrom, dateTo);
		metricsList.add(metrics);
		
		metrics = EventUtil.getMetrics(deviceId, sourceC8yPlatform.getEventApi(), dateFrom, dateTo);
		metricsList.add(metrics);
		
		return metricsList;
	}

	
}

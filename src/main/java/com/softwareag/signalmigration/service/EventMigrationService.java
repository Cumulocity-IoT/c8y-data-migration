package com.softwareag.signalmigration.service;

import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.cumulocity.model.idtype.GId;
import com.cumulocity.rest.representation.event.EventRepresentation;
import com.cumulocity.rest.representation.inventory.ManagedObjectRepresentation;
import com.cumulocity.sdk.client.Platform;
import com.cumulocity.sdk.client.QueryParam;
import com.cumulocity.sdk.client.event.EventApi;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import com.softwareag.signalmigration.model.DeviceSignalMigrationReport;
import com.softwareag.signalmigration.util.EventUtil;

import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class EventMigrationService {
	
	@Autowired
	private MeterRegistry registry;

	/**
	 * @param sourceDeviceId
	 * @param targetDeviceId
	 * @param signalQueryParams
	 * @param sourceC8yPlatform
	 */
	public void migrateEvents(String sourceDeviceId, 
			String targetDeviceId,
			List<QueryParam> signalQueryParams,
			Platform sourceC8yPlatform,
			Platform targetC8yPlatform,
			Consumer<DeviceSignalMigrationReport> reportHandler) {

		log.info(String.format("Will now migrate device events, src device id: %s, targetDeviceId: %s", sourceDeviceId,
				targetDeviceId));
		
		long startTime = System.currentTimeMillis();
		
		try {
			
			ManagedObjectRepresentation targetMo = new ManagedObjectRepresentation();
			targetMo.setId(GId.asGId(targetDeviceId));
			
			EventApi targetEventAPI = targetC8yPlatform.getEventApi();
			Iterable<EventRepresentation> trgtEvents = EventUtil.getEvents(targetDeviceId, signalQueryParams, targetEventAPI);
			Multiset<Integer> uniqueHashesSet = createUniqueHashesSet(trgtEvents);
			
			Iterable<EventRepresentation> sourceEvents = EventUtil.getEvents(sourceDeviceId, signalQueryParams, sourceC8yPlatform.getEventApi());
						
			Iterator<EventRepresentation> eventItor = sourceEvents.iterator();
			
			int numMigrated = 0;
			int numErrors = 0;
			int numDuplicatesSkipped = 0;
			while (eventItor.hasNext()) {
				try {					
					EventRepresentation event = eventItor.next();
					
					EventUtil.trimTenantSpecificFields(event);
					
					// test code verification
//					if (numMigrated == 10) {
//						event.setText("foo");
//					}
					
					// if already present in the target tenant, skip
					int uniqueHash = EventUtil.generateUniqueHash(event);
					if (uniqueHashesSet.contains(uniqueHash)) {
						// use a counter: allows for the possibility of multiple identical signals on tenant A to be copied to tenant B
						uniqueHashesSet.remove(uniqueHash); 
						numDuplicatesSkipped ++;
						registry.counter("EventMigrationService.duplicatesSkipped").increment();
						continue;
					}						
					
					event.setSource(targetMo);
					
					targetEventAPI.create(event);
					numMigrated++;
					registry.counter("EventMigrationService.migrated").increment();					
					
				} catch (Exception e) {
					log.error("Error migrating event", e);
					numErrors++;
					registry.counter("EventMigrationService.errors").increment();
					// TODO: handle exception, track errors
				}
			}
			log.info(String.format("SourceDeviceId %s, DONE migrating events, target device id %s, numMigrated %d, numErrors %d, numDuplicatesSkipped %d ",
					sourceDeviceId, targetDeviceId, numMigrated, numErrors, numDuplicatesSkipped ));
			
			reportHandler.accept(DeviceSignalMigrationReport.builder()
					.sourceDeviceId(sourceDeviceId)
					.targetDeviceId(targetDeviceId)
					//.signalType(SignalType.MEASUREMENT)
					.migrated(numMigrated)
					.errors(numErrors)
					.duplicatesSkipped(numDuplicatesSkipped)
					.durationSec((System.currentTimeMillis() - startTime)/1000)
					.build());
		
			
		} catch (Exception e) {
			log.error("Error migrating events of src device id " + sourceDeviceId, e);
			registry.counter("EventMigrationService.deviceErrors").increment();
			reportHandler.accept(DeviceSignalMigrationReport.builder()
					.durationSec((System.currentTimeMillis() - startTime)/1000)
					.error(e.getMessage()).build());
		}
	}


	private Multiset<Integer> createUniqueHashesSet(Iterable<EventRepresentation> trgtEvts) {		
		Multiset<Integer> uniqueHashesSet = HashMultiset.create();

		for (EventRepresentation evtRep : trgtEvts) {
			
			int uniqueHash = EventUtil.generateUniqueHash(evtRep);
			
			uniqueHashesSet.add(uniqueHash);
										
		}
		
		return uniqueHashesSet;
		
	}
}
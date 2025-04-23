package com.softwareag.signalmigration.service;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.cumulocity.model.event.CumulocityAlarmStatuses;
import com.cumulocity.model.idtype.GId;
import com.cumulocity.rest.representation.alarm.AlarmRepresentation;
import com.cumulocity.rest.representation.inventory.ManagedObjectRepresentation;
import com.cumulocity.sdk.client.Platform;
import com.cumulocity.sdk.client.QueryParam;
import com.cumulocity.sdk.client.alarm.AlarmApi;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import com.softwareag.signalmigration.model.DeviceSignalMigrationReport;
import com.softwareag.signalmigration.util.AlarmUtil;

import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class AlarmMigrationService {
		
	@Autowired
	private MeterRegistry registry;


	/**
	 * Specifies the max value of the 'count' property for migrated alarms. If the source alarm has higher count, the 
	 * migrated alarm's count will be limited to maxAlarmCountValue. THis is to reduce API requests for very high-count alarms.
	 * 
	 * If an ACTIVE or ACKNOWLEDGED alarm with the same source and type exists, no new alarm is created. 
	   Instead, the existing alarm is updated by incrementing the count property
	 */
	@Value("${AlarmMigrationService.maxAlarmCountValue:99}")
	private long maxAlarmCountValue;
	
	/**
	 * 
	 *  limitation for cleared alarms: on the source device, 
	 *  UI will show something like "CLEARED: was active for 17 minutes"
	 	but on the target device, it will show "CLEARED: was active for 7 days"
		 where 7 days = time from alarm.time (creation time) to now (when we're copying)
		 there doesn't seem to be a way to keep 'was active' consistent between 
		 original and copy; even clearing the alarm after creating it as ACTIVE gives the same result
		
		TODO see if we can manipulate the audit record via API and fix this
		see https://cumulocity.com/api/core/10.18.0/#operation/postAuditRecordCollectionResource
		
	 * @param sourceDeviceId
	 * @param targetDeviceId
	 * @param signalQueryParams
	 * @param sourceC8yPlatform
	 * @param reportHandler
	 */
	public void migrateAlarms(String sourceDeviceId, 
			String targetDeviceId,
			List<QueryParam> signalQueryParams,
			Platform sourceC8yPlatform,
			Platform targetC8yPlatform,
			Consumer<DeviceSignalMigrationReport> reportHandler) {

		log.info(String.format("Will now migrate device alarms, src device id: %s, targetDeviceId: %s", sourceDeviceId,
				targetDeviceId));
		
		/*
		    Alarm update via PUT can not update firstOcurrence, count:
			    Update a specific alarm by a given ID. Only text, status, severity and custom properties can be modified. A request will be rejected when non-modifiable properties are provided in the request body.
				
				So we need to be smart about replicating the alarm by posting it multiple times if needed (when count >1)
			
			Alarm de-duplication (posting the same alarm to increase 'count' property):
				If an ACTIVE or ACKNOWLEDGED alarm with the same source and type exists, no new alarm is created. 
				Instead, the existing alarm is updated by incrementing the count property; the time property is 
				also updated. Any other changes are ignored, and the alarm history is not updated. 
				
				Alarms with status CLEARED are not de-duplicated. 
				
				The first occurrence of the alarm is recorded in the firstOccurrenceTime property.
		 */
		long startTime = System.currentTimeMillis();

		try {			
			DeviceSignalMigrationReport report = DeviceSignalMigrationReport.builder()
			.sourceDeviceId(sourceDeviceId)
			.targetDeviceId(targetDeviceId)
			.build();
			
					
			//String tenant = subscriptions.getAll().iterator().next().getTenant();
			
			Iterable<AlarmRepresentation> trgtAlarmsIterable = AlarmUtil.getAlarms(targetDeviceId, signalQueryParams, targetC8yPlatform.getAlarmApi());
			Multiset<Integer> uniqueHashesSet = createUniqueHashesSet(trgtAlarmsIterable);

//			ArrayList<QueryParam> params = new ArrayList<QueryParam>(signalQueryParams);
//			params.add(CustomQueryParam.SOURCE.setValue(sourceDeviceId).toQueryParam());
//						
			Iterable<AlarmRepresentation> sourceAlarmsIterable = AlarmUtil.getAlarms(sourceDeviceId, signalQueryParams, sourceC8yPlatform.getAlarmApi());
									
			ArrayList<AlarmRepresentation> srcAlarms = new ArrayList<>();
			for (AlarmRepresentation alarmRepresentation : sourceAlarmsIterable) {
				srcAlarms.add(alarmRepresentation);
			}
			
			// migrate CLEARED first
			// otherwise incrementing the count of a CLEARED alarm (by posting it as ACTIVE)
			// can increment another alarm of the same type that is ACTIVE
			// After that, migrate ACTIVE/ ACKNOWLEDGED
			srcAlarms.sort( (a1, a2) ->{
				return -1*a1.getStatus().compareTo(a2.getStatus());// will sort in reverse alphabetical, clear->active->ack
			});
			
			Iterator<AlarmRepresentation> alarmItor = srcAlarms.iterator();

			doMigrateAlarms(alarmItor, sourceDeviceId, targetDeviceId, targetC8yPlatform, report, uniqueHashesSet);
			
			log.info(String.format("SourceDeviceId %s, DONE migrating alarms, target device id %s, numMigrated %d, numErrors %d, numDuplicatesSkipped %d ",
					sourceDeviceId, targetDeviceId, report.migrated, report.errors, report.duplicatesSkipped ));
			
			report.setDurationSec((System.currentTimeMillis() - startTime)/1000);		
			reportHandler.accept(report);

		} catch (Exception e) {
			log.error("Error migrating alarms for device, sourceDeviceId " + sourceDeviceId, e);
			registry.counter("AlarmMigrationService.deviceErrors").increment();
			reportHandler.accept(DeviceSignalMigrationReport.builder()
					.durationSec((System.currentTimeMillis() - startTime)/1000)
					.error(e.getMessage()).build());
		}
	}


	private void doMigrateAlarms(Iterator<AlarmRepresentation> alarmItor, 
		String sourceDeviceId,
		String targetDeviceId, 
		Platform targetC8yPlatform, DeviceSignalMigrationReport report, 
		Multiset<Integer> uniqueHashesSet) {
		
		AlarmApi alarms = targetC8yPlatform.getAlarmApi();
	
		ManagedObjectRepresentation targetMo = new ManagedObjectRepresentation();
		targetMo.setId(GId.asGId(targetDeviceId));
		
		while (alarmItor.hasNext()) {
			try {
				AlarmRepresentation srcAlarm = alarmItor.next();
				String srcAlarmId;
				srcAlarmId = srcAlarm.getId().getValue();
				
				// if already present in the target tenant, skip
				
				AlarmUtil.trimTenantSpecificFields(srcAlarm);
				
				// limit count
				if (srcAlarm.getCount() > maxAlarmCountValue) {
					srcAlarm.setCount(maxAlarmCountValue);
				}
				
				int uniqueHash = AlarmUtil.generateUniqueHash(srcAlarm);
				
				if (uniqueHashesSet.contains(uniqueHash)) {
					// use a counter: allows for the possibility of multiple identical signals on tenant A to be copied to tenant B
					uniqueHashesSet.remove(uniqueHash); 
					report.duplicatesSkipped++;
					registry.counter("AlarmMigrationService.duplicatesSkipped").increment();
					continue;
				}						
				
				AlarmRepresentation copyAlarm = new AlarmRepresentation();
				copyAlarm.setSource(targetMo);
				copyAlarm.setType(srcAlarm.getType());
				copyAlarm.setText(srcAlarm.getText());
// test validation code
//				if (report.getMigrated() == 23) {
//					copyAlarm.setText("foo");
//				}
				copyAlarm.setAttrs(srcAlarm.getAttrs());
				copyAlarm.setSeverity(srcAlarm.getSeverity());
			
				
				if (srcAlarm.getCount() == 1) {
					// it's a one-time alarm
					
					copyAlarm.setDateTime(srcAlarm.getDateTime());
					copyAlarm.setStatus(srcAlarm.getStatus());

					alarms.create(copyAlarm);
				} else {
					// it's a multi-count alarm  
			        // creating the first occurence
					copyAlarm.setDateTime(srcAlarm.getFirstOccurrenceDateTime());
					copyAlarm.setStatus(CumulocityAlarmStatuses.ACTIVE.toString()); // ACTIVE forces deduping (no new alarm when posting again, just increase count) 
				
					AlarmRepresentation created = alarms.create(copyAlarm);
					
			        // now increment count
					copyAlarm.setDateTime(srcAlarm.getDateTime()); // create all copies with time same as last copy 
					for (int i = 0; i < srcAlarm.getCount()-1; i++) {
						// don't worry about creating the multi alarms at the exact times, 
                        // they're not tracked (even by audit)
						alarms.create(copyAlarm);
					}
				
			        // finally, if not active, update status
					if (!srcAlarm.getStatus().equals(CumulocityAlarmStatuses.ACTIVE.name())) {
						copyAlarm.setStatus(srcAlarm.getStatus());
						copyAlarm.setId(created.getId());
						alarms.update(copyAlarm);
//						alarms.create(copyAlarm);
					}
				}
				
				report.migrated ++;
				registry.counter("AlarmMigrationService.migrated").increment();
				

				if (log.isTraceEnabled()) {
					log.trace(String.format("SourceDeviceId %s, migrating alarm id %s (count %d), target device id %s, numMigrated %d, numErrors %d, numDuplicatesSkipped %d ",
							sourceDeviceId, srcAlarmId, srcAlarm.getCount(), targetDeviceId, report.migrated, report.errors, report.duplicatesSkipped ));
				}
			} catch (Exception e) {
				log.error("Error migrating alarm", e);
				report.errors++;
				registry.counter("AlarmMigrationService.errors").increment();
			}
		}		
	}


	private Multiset<Integer> createUniqueHashesSet(Iterable<AlarmRepresentation> trgtAlarms) {		
		Multiset<Integer> uniqueHashesSet = HashMultiset.create();
						
		for (AlarmRepresentation alarmRep : trgtAlarms) {
			
			AlarmUtil.trimTenantSpecificFields(alarmRep);
			int uniqueHash = AlarmUtil.generateUniqueHash(alarmRep);
			
			uniqueHashesSet.add(uniqueHash);
										
		}
		
		return uniqueHashesSet;
	}


	

}

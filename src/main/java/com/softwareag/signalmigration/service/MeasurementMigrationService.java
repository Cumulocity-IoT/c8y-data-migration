package com.softwareag.signalmigration.service;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.cumulocity.model.idtype.GId;
import com.cumulocity.rest.representation.inventory.ManagedObjectRepresentation;
import com.cumulocity.rest.representation.measurement.MeasurementCollectionRepresentation;
import com.cumulocity.rest.representation.measurement.MeasurementRepresentation;
import com.cumulocity.sdk.client.Platform;
import com.cumulocity.sdk.client.QueryParam;
import com.cumulocity.sdk.client.measurement.MeasurementApi;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import com.softwareag.signalmigration.model.DeviceSignalMigrationReport;
import com.softwareag.signalmigration.util.MeasurementUtil;

import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class MeasurementMigrationService {
		
	@Autowired
	private MeterRegistry registry;

	/**
	 * @param sourceDeviceId
	 * @param targetDeviceId
	 * @param signalQueryParams
	 * @param sourceC8yPlatform
	 */
	public void migrateMeasurements(String sourceDeviceId, 
			String targetDeviceId,
			List<QueryParam> signalQueryParams,
			Platform sourceC8yPlatform,
			Platform targetC8yPlatform,
			Consumer<DeviceSignalMigrationReport> reportHandler) {

		log.info(String.format("Will now migrate device measurements, src device id: %s, targetDeviceId: %s", sourceDeviceId,
				targetDeviceId));
		
		long startTime = System.currentTimeMillis();
		
		try {
			
			ManagedObjectRepresentation targetMo = new ManagedObjectRepresentation();
			targetMo.setId(GId.asGId(targetDeviceId));
			
			MeasurementApi targetMeasurementsAPI = targetC8yPlatform.getMeasurementApi();
			Iterable<MeasurementRepresentation> trgtMsmts = MeasurementUtil.getMeasurements(targetDeviceId, signalQueryParams, targetMeasurementsAPI);
			Multiset<Integer> uniqueHashesSet = createUniqueHashesSet(trgtMsmts);

			Iterable<MeasurementRepresentation> sourceMsmts = MeasurementUtil.getMeasurements(sourceDeviceId, signalQueryParams, sourceC8yPlatform.getMeasurementApi());
			Iterator<MeasurementRepresentation> msmtItor = sourceMsmts.iterator();
			
			int numMigrated = 0;
			int numErrors = 0;
			int numDuplicatesSkipped = 0;
			ArrayList<MeasurementRepresentation> msmtBatch = new ArrayList<>();
			int maxBatchSize = 200;
			String sourceMsmtId = null;
			while (msmtItor.hasNext()) {
				try {
					MeasurementRepresentation msmt = msmtItor.next();
					sourceMsmtId = msmt.getId().getValue();
					
					MeasurementUtil.trimTenantSpecificFields(msmt);
					
					// if already present in the target tenant, skip
					int uniqueHash = MeasurementUtil.generateUniqueHash(msmt);
					if (uniqueHashesSet.contains(uniqueHash)) {
						// use a counter: allows for the possibility of multiple identical signals on tenant A to be copied to tenant B
						uniqueHashesSet.remove(uniqueHash); 
						numDuplicatesSkipped ++;
						registry.counter("MeasurementMigrationService.numDuplicatesSkipped").increment();
						continue;
					}						
					
					msmt.setSource(targetMo);
					msmtBatch.add(msmt);

					if (msmtBatch.size() >= maxBatchSize || !msmtItor.hasNext()) {							
						MeasurementCollectionRepresentation msmtColl = new MeasurementCollectionRepresentation();
						msmtColl.setMeasurements(msmtBatch);
						targetMeasurementsAPI.createBulkWithoutResponse(msmtColl);
						/*
						 * c8y_SupportedMeasurements are not refreshed when using bulk creation, so they don't show in
						 * the UI which uses c8y_SupportedMeasurements to display?!
						 * per web:
						 * “c8y_SupportedMeasurements” and “c8y_supportedSeries” are 
						 * refreshed asynchronous, as far as I remember 2 times a day.
						 *  Unused c8y_SupportedMeasurements and c8y_supportedSeries are removed automatically.
						 */							
						//measurements.createWithoutResponse(msmt); // Does not send Accept header to make the request be processed faster.
						numMigrated += msmtBatch.size();
						registry.counter("MeasurementMigrationService.migrated").increment(msmtBatch.size());						
						msmtBatch.clear();
						log.debug(String.format("SourceDeviceId %s, migrating measurements, target device id %s, numMigrated %d, numErrors %d, numDuplicatesSkipped %d ",
								sourceDeviceId, targetDeviceId, numMigrated, numErrors, numDuplicatesSkipped ));							
					}
				} catch (Exception e) {
					log.error("Error migrating a measurement, sourceMsmtId " + sourceMsmtId, e);
					numErrors+= msmtBatch.size();
					registry.counter("MeasurementMigrationService.errors").increment();
					// TODO: handle exception, track errors
				}
			}
			log.info(String.format("SourceDeviceId %s, DONE migrating measurements, target device id %s, numMigrated %d, numErrors %d, numDuplicatesSkipped %d ",
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
			log.error("Error migrating measurements of source device id " + sourceDeviceId, e);
			registry.counter("MeasurementMigrationService.deviceErrors").increment();
			reportHandler.accept(DeviceSignalMigrationReport.builder()
					.durationSec((System.currentTimeMillis() - startTime)/1000)
					.error(e.getMessage()).build());
		}
	}


	private Multiset<Integer> createUniqueHashesSet(Iterable<MeasurementRepresentation> trgtMsmts) {		
		Multiset<Integer> uniqueHashesSet = HashMultiset.create();

		for (MeasurementRepresentation msmtRep : trgtMsmts) {
			
			int uniqueHash = MeasurementUtil.generateUniqueHash(msmtRep);
			uniqueHashesSet.add(uniqueHash);
			registry.counter("MeasurementMigrationService.uniqueHashesGenerated").increment();
										
		}
		
		return uniqueHashesSet;
		
	}


	
}

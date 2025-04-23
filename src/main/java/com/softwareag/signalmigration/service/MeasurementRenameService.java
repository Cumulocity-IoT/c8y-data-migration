package com.softwareag.signalmigration.service;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Executors;

import org.springframework.stereotype.Component;

import com.cumulocity.rest.representation.measurement.MeasurementRepresentation;
import com.cumulocity.sdk.client.QueryParam;
import com.cumulocity.sdk.client.measurement.MeasurementApi;
import com.softwareag.signalmigration.util.CustomQueryParam;

import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class MeasurementRenameService {
	
	@Builder
	private static class RenameResult {
		boolean error;
		boolean skipped;
	}
	
	private volatile long numCopied;
	
	private ExecutorCompletionService<RenameResult> executorService = new ExecutorCompletionService<RenameResult>(Executors.newFixedThreadPool(10));
	
	private boolean dryRun = false;
	private long maxMsmtsToProcess = Long.MAX_VALUE;
	
	/**
	 * 
	 * Creates copy measurements where msmt.valueFragmentType.valueFragmentSeries 
	 * is mapped to copy.valueFragmentType.newValueFragmentSeries and the original measurement
	 * is deleted (in effect, valueFragmentSeries is renamed newValueFragmentSeries)
	 * 
	 * @param c8yPlatform
	 * @param valueFragmentType filter on msmts to be processed
	 * @param valueFragmentSeries
	 * @param newValueFragmentSeries
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 */
	public synchronized void renameValueFragmentSeries(
			MeasurementApi measurementApi,
			String valueFragmentType,
			String valueFragmentSeries,
			String newValueFragmentSeries) throws InterruptedException, ExecutionException {
		
		List<QueryParam> params = Arrays.asList(
				CustomQueryParam.VALUE_FRAGMENT_TYPE.setValue(valueFragmentType).toQueryParam());
		
		Iterable<MeasurementRepresentation> sourceMsmts = measurementApi
				.getMeasurements().get(500, params.toArray(new QueryParam[0])).allPages();
		
		numCopied = 0;
		int numProcessed = 0;
		int numSkipped = 0;
		int numErrors = 0;
		for (MeasurementRepresentation msmt : sourceMsmts) {

			executorService.submit(()-> {
				return renameMsmt(msmt, measurementApi, valueFragmentType, valueFragmentSeries, newValueFragmentSeries);
			});
			
			if (++numProcessed >= maxMsmtsToProcess) {
				break;
			};
			
		}	
		
		
		for (int i = 0; i < numProcessed; i++) {
			RenameResult result = executorService.take().get();
			if (result.error) numErrors++;
			if (result.skipped) numSkipped++;
			
		}
		
		log.info("Finished processing (dryRun: {}), numProcessed {}, numSkipped {}, numErrors {}", 
				dryRun, numProcessed, numSkipped, numErrors);
	}

	private RenameResult renameMsmt(MeasurementRepresentation msmt, MeasurementApi measurementApi, String valueFragmentType, String valueFragmentSeries, String newValueFragmentSeries) {
		boolean error = false;
		boolean skipped = false;
		try {
			@SuppressWarnings("unchecked")
			Map<String, Object> valueFragment = (Map<String, Object>) msmt.get(valueFragmentType);
			
			boolean hasSeries = valueFragment != null && valueFragment.containsKey(valueFragmentSeries);
			

			if (!hasSeries) {
				skipped = true;
			} else if (!dryRun) {
			
//				duplicate
//		        modify the series
//		        create the modified on the same source device //(w duplicate check if possible)
//		        delete original if copy successful
				MeasurementRepresentation msmtToDelete = new MeasurementRepresentation();
				msmtToDelete.setId(msmt.getId());
				
				
				// we will reuse the msmt to create the copy
				// first null non-updateble fields
				msmt.setSelf(null);
				msmt.setId(null);	
				
				// now rename
				@SuppressWarnings("unchecked")
				Map<String, Object> series = (Map<String, Object>) valueFragment.get(valueFragmentSeries);
				valueFragment.remove(valueFragmentSeries);
				valueFragment.put(newValueFragmentSeries, series);
			
				// create copy
				MeasurementRepresentation created = measurementApi.create(msmt);
				
				// delete if successful creation
				measurementApi.delete(msmtToDelete);
				
				
				numCopied++;
				if (numCopied % 10 == 0) {
					log.debug("{} msmts copied..", numCopied);
				}
				//log.debug("Copied msmt, old id {}, new id {}", msmtToDelete.getId().getValue(), created.getId().getValue());
			}
			
			//log.trace("Processing: {}", msmt.toString());
			
		} catch (Exception e) {
			log.debug("Error", e);
			error = true;
		}
		

		return RenameResult.builder().error(error).skipped(skipped).build();
	
		
	}
}

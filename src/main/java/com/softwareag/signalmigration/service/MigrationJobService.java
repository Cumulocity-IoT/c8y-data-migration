package com.softwareag.signalmigration.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import com.cumulocity.microservice.subscription.model.MicroserviceSubscriptionsInitializedEvent;
import com.cumulocity.microservice.subscription.service.MicroserviceSubscriptionsService;
import com.cumulocity.model.ID;
import com.cumulocity.model.idtype.GId;
import com.cumulocity.model.operation.OperationStatus;
import com.cumulocity.rest.representation.identity.ExternalIDRepresentation;
import com.cumulocity.rest.representation.inventory.ManagedObjectRepresentation;
import com.cumulocity.sdk.client.Platform;
import com.cumulocity.sdk.client.identity.ExternalIDCollection;
import com.cumulocity.sdk.client.identity.IdentityApi;
import com.cumulocity.sdk.client.inventory.InventoryFilter;
import com.softwareag.signalmigration.model.DeviceSignalMigrationReport;
import com.softwareag.signalmigration.model.ExternalIdMappingAdvice;
import com.softwareag.signalmigration.model.MigrationJob;
import com.softwareag.signalmigration.model.MigrationJobConfig;
import com.softwareag.signalmigration.model.SignalType;
import com.softwareag.signalmigration.util.CustomInventoryFilter;
import com.softwareag.signalmigration.util.ManagedObjectUtil;

import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryRegistry;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;


@Component
@Slf4j
public class MigrationJobService {
	
	public static final String JOB_FRAGMENT_NAME = "signalMigrationJob";

	@Autowired
	private CumulocityService cumulocityService;
	
	@Autowired
	private MeasurementMigrationService measurementMigrationService;
	
	@Autowired
	private AlarmMigrationService alarmMigrationService;
	
	@Autowired
	private EventMigrationService eventMigrationService;
	
	@Autowired
	private PlatformUtil platformUtil;
	
	@Autowired
	@Qualifier("defaultExternalAPIRetry")	
	private RetryRegistry retryRegistry;
	
	@Value("${MigrationJobService.numParallelDeviceMigrations:9}")
	private int numParallelDeviceMigrations;
	
	@Value("${MigrationJobService.resumeIncompleteJobsOnStartup:true}")
	private boolean resumeIncompleteJobsOnStartup;
	
	@Autowired
	private MeterRegistry registry;
		
	private ThreadPoolExecutor executorService;
	
	private Retry apiRequestRetry;
	
	@PostConstruct
	private void init() {
		executorService = (ThreadPoolExecutor) Executors.newFixedThreadPool(numParallelDeviceMigrations);
		BlockingQueue<Runnable> queue = executorService.getQueue();
		registry.gauge("MigrationJobService.pendingDeviceMigrationsQueueSize", queue, BlockingQueue::size);
		registry.gauge("MigrationJobService.executorServiceApproxActiveCount", executorService, ThreadPoolExecutor::getActiveCount);

				
		apiRequestRetry = retryRegistry.retry("create-c8y-operation");		
	}
	
	@EventListener
    @Async
	public void onSubscriptionsInitialized(MicroserviceSubscriptionsInitializedEvent event) {
		log.info("Subscriptions have been initialized on application startup");
			
		if (resumeIncompleteJobsOnStartup) {
			resumeIncompleteJobs();
		}		
	}

	private void resumeIncompleteJobs() {
		ArrayList<MigrationJob> jobs = loadIncompleteJobs();
		for (MigrationJob job : jobs) {
			try {
				resumeMigrationJob(job);
			} catch (Exception e) {
				log.error("Error resuming job " + job, e);
			}
		}
	}

	public void resumeMigrationJob(MigrationJob job) throws Exception {
		log.info("Resuming job " + job);
		runMigrationJob(job);
	}
	
	public void retryMigrationJob(MigrationJob job, boolean force) throws Exception {
		if (job.getStatus().equals(OperationStatus.EXECUTING)
				|| job.getStatus().equals(OperationStatus.PENDING)) {
			if (!force) {
				throw new IllegalArgumentException("Job is not completed!");
			}
		}
		int removed = job.removeReportsWithErrors();
		if (removed == 0) {
			throw new IllegalStateException("No device migration errors found! Will not retry.");
		}
		log.info("Retrying job, {} devices with errors removed, job: {}", removed, job);
		runMigrationJob(job);
	}
	
	private ArrayList<MigrationJob> loadIncompleteJobs() {
		CustomInventoryFilter filter = new CustomInventoryFilter().byQuery(
				String.format("has('%s') and %s.version eq %d and %s.status eq '%s'",
						JOB_FRAGMENT_NAME, 
						JOB_FRAGMENT_NAME,
						MigrationJob.VERSION,
						JOB_FRAGMENT_NAME,
						OperationStatus.EXECUTING.name()));
		ArrayList<MigrationJob> jobs = new ArrayList<>();
		for (Iterator<ManagedObjectRepresentation> iterator = cumulocityService.getManagedObjectsByFilter(filter).iterator(); iterator.hasNext();) {
			ManagedObjectRepresentation moRep = iterator.next();
			MigrationJob job = ManagedObjectUtil.readObjectFromFragment(moRep, MigrationJob.class, JOB_FRAGMENT_NAME);
			job.setC8yId(moRep.getId().getValue());
			jobs.add(job);
		}
		return jobs;
	}
	
	public MigrationJob loadJob(String id) {
		ManagedObjectRepresentation moRep = cumulocityService.getManagedObject(id);
		MigrationJob job = ManagedObjectUtil.readObjectFromFragment(moRep, MigrationJob.class, JOB_FRAGMENT_NAME);
		job.setC8yId(moRep.getId().getValue());
		return job;
	}

	private ManagedObjectRepresentation saveJob(MigrationJob job) throws Exception {
		ManagedObjectRepresentation saved = null;
		saved = apiRequestRetry.executeCallable( ()-> {
			return doSaveJob(job);
		});
		return saved;
	}
	
	private ManagedObjectRepresentation doSaveJob(MigrationJob job) {
		ManagedObjectRepresentation jobMo = new ManagedObjectRepresentation();
		
		ManagedObjectUtil.writeObjectAsFragment(jobMo, job, JOB_FRAGMENT_NAME);
		
		ManagedObjectRepresentation managedObject;
		if (job.getC8yId() == null) {
			log.info("Creating job in c8y " + job);
			managedObject = cumulocityService.createManagedObject(jobMo);
			job.setC8yId(managedObject.getId().getValue());
		} else {
			log.info("Updating job in c8y " + job);
			jobMo.setId(GId.asGId(job.getC8yId()));
			managedObject = cumulocityService.updateManagedObject(jobMo);
		}
		return managedObject;
	}

	public ManagedObjectRepresentation runMigrationJob(MigrationJob job) throws Exception {		
		log.info(String.format("Starting migration job %s", job));
		
		ManagedObjectRepresentation savedJob = null;

		try {
			
			MigrationJobConfig config = job.getConfig();
			
			job.setSourcePlatform(platformUtil.resolvePlatform(job.getConfig().getSourcePlatformHost(), 
					job.getConfig().getSourcePlatformLoginString()));			
			job.setTargetPlatform(platformUtil.resolvePlatform(job.getConfig().getTargetPlatformHost(), 
					job.getConfig().getTargetPlatformLoginString()));
	
			String sourceDevicesQuery = config.getSourceDevicesQuery();
			
			InventoryFilter filter = new CustomInventoryFilter().byQuery(sourceDevicesQuery);
			
			// get source devices
			Iterable<ManagedObjectRepresentation> sourceDevicesItor = job.getSourcePlatform().getInventoryApi().getManagedObjectsByFilter(filter).get(200).allPages();
			
			ArrayList<String> sourceDeviceIds = new ArrayList<>();
			for (ManagedObjectRepresentation sourceDevice : sourceDevicesItor) {
				sourceDeviceIds.add(sourceDevice.getId().getValue());
			}
			
			log.info("{} source devices found, job {}", sourceDeviceIds.size(), job.toString());
			
			// save
			job.setNumTotalDevices(sourceDeviceIds.size());
			job.setStatus(OperationStatus.EXECUTING);
			savedJob = saveJob(job);
			
			if (sourceDeviceIds.isEmpty()) {
				job.setStatus(OperationStatus.SUCCESSFUL);
				saveJob(job);			
			}
			
			// start migration
			for (String sourceDeviceId : sourceDeviceIds) {
				// migrate only if not already processed (e.g. if this is a resumed partial job)
				if (!job.isDeviceProcessed(sourceDeviceId)) {
					executorService.execute( ()-> {
						migrateDeviceSignals(sourceDeviceId, job);			
					});
				} else {
					log.info("device is processed, skipping; source device {}, job {}", sourceDeviceId, job.toString());
				}
			}
			
		
		} catch (Exception e) {
			log.error("Error starting job " + job.toString(), e);
			throw e;
		} 
		
		return savedJob;
	}
	
	private void migrateDeviceSignals(String sourceDeviceId, MigrationJob job) {
		log.info(String.format("Will now migrate device signals, src device id: %s, job: %s", sourceDeviceId, job));
		try {
			String targetDeviceId = getTargetDeviceForSource(sourceDeviceId, job).getId().getValue();

			if (job.getConfig().getSignalType().equals(SignalType.MEASUREMENT)) {
				
				//targetDeviceId= "2114265";
				measurementMigrationService.migrateMeasurements(sourceDeviceId, targetDeviceId, 
						job.getConfig().getSignalQueryParams(),
						job.getSourcePlatform(),
						job.getTargetPlatform(),
						(DeviceSignalMigrationReport report) -> {
							handleDeviceReport(report, job);
						});
				
			} else if (job.getConfig().getSignalType().equals(SignalType.EVENT)) {				
				
				eventMigrationService.migrateEvents(sourceDeviceId, targetDeviceId, job.getConfig().getSignalQueryParams(),
						job.getSourcePlatform(),
						job.getTargetPlatform(),
						(DeviceSignalMigrationReport report) -> {
							handleDeviceReport(report, job);
						});
			
			} else if (job.getConfig().getSignalType().equals(SignalType.ALARM)) {
			
				alarmMigrationService.migrateAlarms(sourceDeviceId, targetDeviceId,job.getConfig().getSignalQueryParams(),
						job.getSourcePlatform(),
						job.getTargetPlatform(),
						(DeviceSignalMigrationReport report) -> {
							handleDeviceReport(report, job);
						});
				
			} else {
				throw new UnsupportedOperationException();
			}
		} catch (Exception e) {
			log.error("error", e);
			handleDeviceReport(DeviceSignalMigrationReport.builder()
					.sourceDeviceId(sourceDeviceId)
					.error(e.getMessage())
					.build(), job);
		}		
	}

	private synchronized void handleDeviceReport(DeviceSignalMigrationReport report, MigrationJob job) {
		try {
			job.addDeviceReport(report);
			job.updateStatus();
			saveJob(job);
		} catch (Exception e) {
			//must swallow ex, otherwise messes up caller logic - will submit an additional error report
			log.error("Error hanndling report", e);
		}
	}
	
	private ManagedObjectRepresentation getTargetDeviceForSource(String sourceDeviceId, MigrationJob job) throws Exception {
		// TODO other mapping methods (e.g. c8yIdMappingAdvice)
		ArrayList<ExternalIdMappingAdvice> externalIdMappingAdvice = job.getConfig().getExternalIdMappingAdvice();		
		return getTargetDeviceForSourceByExtId(sourceDeviceId, externalIdMappingAdvice, job.getSourcePlatform(), job.getTargetPlatform());
	}
	
	private ManagedObjectRepresentation getTargetDeviceForSourceByExtId(String sourceDeviceId, ArrayList<ExternalIdMappingAdvice> externalIdMappingAdvice, Platform sourcePlatform, Platform targetPlatform) throws Exception {
		// match by external id
		log.info("Looking for target device for source device with id {}", sourceDeviceId);
		
		List<ExternalIDRepresentation> externalIds = apiRequestRetry.executeCallable( ()-> {
			ExternalIDCollection externalIdsColl = sourcePlatform.getIdentityApi().getExternalIdsOfGlobalId(GId.asGId(sourceDeviceId));			
			return externalIdsColl.get(200).getExternalIds();			
		});
//		ExternalIDCollection externalIdsColl = remoteC8yPlatform.getIdentityApi().getExternalIdsOfGlobalId(GId.asGId(sourceDeviceId));
//		List<ExternalIDRepresentation> externalIds = externalIdsColl.get(200).getExternalIds();
		
		log.info("sourceDeviceId {}; Found external ids: {}", sourceDeviceId, externalIds.toString());
		List<String> ignoredExtIdTypes = List.of("c8y_OpcuaDevice");
		// filter
		externalIds = externalIds.stream().filter( e-> {
			return !ignoredExtIdTypes.contains(e.getType());
			}).collect(Collectors.toList());
		
		if (externalIds.isEmpty()) {
			throw new IllegalStateException("no suitable external ids of id " + sourceDeviceId);
		} 
		
		ManagedObjectRepresentation targetDevice = null;
		String targetExtId = "";
		for (ExternalIDRepresentation extId : externalIds) {
			try {
				String sourceExtId = extId.getExternalId();
				
				// see if there's explicit mapping advice
				
				ExternalIdMappingAdvice mappingAdvice = null;
				if (externalIdMappingAdvice != null) {
					mappingAdvice = externalIdMappingAdvice.stream().filter( adv -> adv.getSourceExternalId().equals(sourceExtId)).findFirst().orElse(null);
				}
				if (mappingAdvice != null) {
					targetExtId = mappingAdvice.getTargetExternalId(); // use mapping advice
					log.info("mapping advice present source ext id: {} target ext id: {}", sourceExtId, targetExtId);
				} else {
					targetExtId = sourceExtId; // use source id
				}
				
				// find target device
				targetDevice = getDeviceForExternalId(extId.getType(), targetExtId, targetPlatform);
				log.info("sourceDeviceId {}; Found matching target device; ext id: {}:{}, target device id: {}, ext id: {}:{}", sourceDeviceId, extId.getType(), sourceExtId, 
						targetDevice.getId().getValue(), extId.getType(), targetExtId);
				break;
			} catch (IOException e) {
				log.info("Unable to find by extId " + extId.getType() + " : " + targetExtId);
			}
		}
		
		if (targetDevice == null) {
			throw new IOException("unable to find target device for source device id: "  + sourceDeviceId);
		}
		
		return targetDevice;
	}

	private ManagedObjectRepresentation getDeviceForExternalId(String type, String value, Platform platform) throws IOException {
		try {
			ID extId = new ID(type, value);
			ExternalIDRepresentation targetExternalIdRep = platform.getIdentityApi().getExternalId(extId);
			return targetExternalIdRep.getManagedObject();
		} catch (Exception e) {
			throw new IOException("unable to find external id", e);
		}
	}

//	private void getPublicIp() throws IOException {
//		// TODO Auto-generated method stub
//		
//		
//		URL whatismyip = new URL("http://checkip.amazonaws.com");
//		BufferedReader in = new BufferedReader(new InputStreamReader(
//		                whatismyip.openStream()));
//
//		String ip = in.readLine(); //you get the IP as a String
//		log.info(ip);	
//
//	}
}

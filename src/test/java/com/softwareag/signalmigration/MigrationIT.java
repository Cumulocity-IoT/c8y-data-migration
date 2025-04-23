package com.softwareag.signalmigration;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.awaitility.Awaitility;
import org.joda.time.DateTime;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

import com.cumulocity.microservice.subscription.service.MicroserviceSubscriptionsService;
import com.cumulocity.model.idtype.GId;
import com.cumulocity.model.operation.OperationStatus;
import com.cumulocity.rest.representation.alarm.AlarmRepresentation;
import com.cumulocity.rest.representation.event.EventRepresentation;
import com.cumulocity.rest.representation.identity.ExternalIDRepresentation;
import com.cumulocity.rest.representation.inventory.ManagedObjectRepresentation;
import com.cumulocity.rest.representation.measurement.MeasurementRepresentation;
import com.cumulocity.sdk.client.Platform;
import com.cumulocity.sdk.client.SDKException;
import com.cumulocity.sdk.client.identity.IdentityApi;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.softwareag.signalmigration.model.DeviceSignalMetrics;
import com.softwareag.signalmigration.model.DeviceSignalMigrationReport;
import com.softwareag.signalmigration.model.MigrationJob;
import com.softwareag.signalmigration.model.MigrationJobConfig;
import com.softwareag.signalmigration.model.SignalType;
import com.softwareag.signalmigration.service.CumulocityService;
import com.softwareag.signalmigration.service.DeviceMigrationService;
import com.softwareag.signalmigration.service.MeasurementRenameService;
import com.softwareag.signalmigration.service.MigrationJobService;
import com.softwareag.signalmigration.service.PlatformUtil;
import com.softwareag.signalmigration.service.SignalMetricCollectionService;
import com.softwareag.signalmigration.util.AlarmUtil;
import com.softwareag.signalmigration.util.CustomInventoryFilter;
import com.softwareag.signalmigration.util.EventUtil;
import com.softwareag.signalmigration.util.ManagedObjectUtil;
import com.softwareag.signalmigration.util.MeasurementUtil;

import c8y.IsDevice;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@SpringBootTest(classes = Application.class, webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT)
@ActiveProfiles({"default","env-dev","integration-test"})
class MigrationIT {

	private static final String TEST_DEVICE_FRAGMENT = "integrationTestDevice";

	@Autowired
	private MicroserviceSubscriptionsService subscriptions;
	
	@Autowired
	private IdentityApi identityApi;
	
	@Autowired
	private	MigrationJobService migrationJobService;
	
	@Autowired
	private DeviceMigrationService deviceMigrationService;
	
	@Autowired
	private SignalMetricCollectionService signalMetricCollectionService;
	
	@Autowired
	private	CumulocityService cumulocityService;
	
	@Autowired
	private PlatformUtil platformUtil;
	
	@Autowired
	private MeasurementRenameService measurementRenameService;
		
	@Value("${sourceC8yURL}")
	private String sourceC8yURL;
	@Value("${sourceTenantId}")
	private String sourceTenantId;
	@Value("${sourceUsername}")
	private String sourceUsername;
	@Value("${sourcePassword}")
	private String sourcePassword;	
	
	@Value("${targetC8yURL}")
	private String targetC8yURL;
	@Value("${targetTenantId}")
	private String targetTenantId;
	@Value("${targetUsername}")
	private String targetUsername;
	@Value("${targetPassword}")
	private String targetPassword;
	
//	@Test
//	@DirtiesContext
	void renamePM25Series() throws IOException, InterruptedException, SDKException, ExecutionException {		
//		Platform srcPlatform = platformUtil.resolvePlatform(sourceC8yURL, sourceTenantId + "/" + sourceUsername + ":" + sourcePassword);
		
		String prodPwd = "t21092648/Mihail.Kostira@softwareag.com:gHID9L7A2Nvt98AG!";
		Platform srcPlatform = platformUtil.resolvePlatform("https://thegear.jp.cumulocity.com", prodPwd );
		
		String valueFragmentType = "ktg_air_quality_sensor";
		
		measurementRenameService.renameValueFragmentSeries(srcPlatform.getMeasurementApi(),
				valueFragmentType , "pm2.5", "pm25");
		
	}
	
//	@Test
//	@DirtiesContext
	void collectSignalMetrics() throws IOException, InterruptedException {		
		Awaitility.await().atMost(30, TimeUnit.SECONDS).ignoreExceptions().until(()->subscriptions.isRegisteredSuccessfully());
		
//		String metricName = "Smartbox-new-staging";
//		String sourceGroupId = "83673536";
//		String sourceDeviceQuery = "$filter=bygroupid(" + sourceGroupId + ")";
		
		
//		String metricName = "AICamera-new-staging";
//		String sourceGroupId = "60672582";
//		String sourceDeviceQuery = "$filter=bygroupid(" + sourceGroupId + ")";
		
//		String metricName = "Solargy-new-staging";
//		String sourceDeviceQuery = "$filter=(has('c8y_IsDevice') and (c8y_OpcuaServerId eq '61909223'))"; 

//		String metricName = "BMS-new-staging";
//		String sourceDeviceQuery = "$filter=(has('c8y_IsDevice') and (c8y_OpcuaServerId eq '734285718'))"; 

//		String metricName = "ACMS-VMS-new-staging";
//		String sourceGroupId = "14672577";
//		String sourceDeviceQuery = "$filter=bygroupid(" + sourceGroupId + ")";
				
//		String metricName = "CCTV-new-staging";
//		String sourceGroupId = "79673543";
//		String sourceDeviceQuery = "$filter=bygroupid(" + sourceGroupId + ")";
		
		
		
//		String metricName = "PMS-new-staging";
//		String sourceGroupId = "34672110";// PMS newstaging
//		String sourceDeviceQuery = "$filter=bygroupid(" + sourceGroupId + ")";				
		
//		String metricName = "Restroom-new-staging";
//		String sourceGroupId = "45672112";
//		String sourceDeviceQuery = "$filter=bygroupid(" + sourceGroupId + ")";	
		
//		String metricName = "Smartlandscaping-new-staging";
//		String sourceGroupId = "43673542";
//		String sourceDeviceQuery = "$filter=bygroupid(" + sourceGroupId + ")";	
		
//		String metricName = "IAQ-new-staging";
//		String sourceGroupId = "66672111";
//		String sourceDeviceQuery = "$filter=bygroupid(" + sourceGroupId + ")";	
		
		
//		String metricName = "test";
//		String sourceDeviceId = "6410740553";
//		String sourceDeviceQuery = "$filter=id eq '" + sourceDeviceId + "'";
		
		
		String metricName = "Solargy-OLD-STAGING";
		String sourceDeviceQuery = "$filter=(has('c8y_IsDevice') and (c8y_OpcuaServerId eq '4530428'))";
		

		//DateTime dateFrom = DateTime.now().minusYears(1000);
		//DateTime dateTo = DateTime.now().plusYears(1000);
//		DateTime dateFrom = DateTime.parse("1000-01-01T00:00:00.000Z");
//		DateTime dateTo = DateTime.parse("2023-12-01T00:00:00.000Z");
		DateTime dateFrom = DateTime.parse("2023-12-01T00:00:00.000Z");
		DateTime dateTo = DateTime.parse("2024-01-01T00:00:00.000Z");
		//DateTime dateTo = DateTime.parse("2023-02-02T11:00:35.506Z");
		
		Platform srcPlatform = platformUtil.resolvePlatform(sourceC8yURL, sourceTenantId + "/" + sourceUsername + ":" + sourcePassword);
		

		SimpleDateFormat isoFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
		ObjectMapper mapper = new ObjectMapper()
				.registerModule(new JodaModule())
				.setDateFormat(isoFormat);

		List<DeviceSignalMetrics> metrics = signalMetricCollectionService.collectDeviceSignalMetrics(sourceDeviceQuery, srcPlatform, dateFrom, dateTo);
		//System.out.println(mapper.writeValueAsString(metrics));
		
		String metricsJSON = mapper.writeValueAsString(metrics);
		String fileName = metricName + "-signalMetrics-" + DateTime.now().getMillis() + ".json";
        //URI filePath = URI.create(fileName);
		Files.write(Paths.get(fileName ), metricsJSON.getBytes());

		log.info("Metrics written to file  {}", fileName);
		
		log.info("done");
	}

	
	@Test
	@DirtiesContext
	void migrateDevice() {		
		Awaitility.await().atMost(30, TimeUnit.SECONDS).ignoreExceptions().until(()->subscriptions.isRegisteredSuccessfully());
				
		cumulocityService.deleteManagedObjectsByFragment(TEST_DEVICE_FRAGMENT);

		
		ManagedObjectRepresentation targetDeviceTemplate = new ManagedObjectRepresentation();
		targetDeviceTemplate.set("foo", TEST_DEVICE_FRAGMENT);
		
		// source device & range
		// TODO remove hardcoded
		
//		String sourceDeviceId = "9015116022";
//		String sourceDeviceQuery = "$filter=id eq '" + sourceDeviceId + "'";	
				
//		String sourceGroupId = "79673543";
//		String sourceDeviceQuery = "$filter=bygroupid(" + sourceGroupId + ")";
		
		// bms new staging
		String sourceDeviceQuery ="$filter=(has('c8y_IsDevice') and (c8y_OpcuaServerId eq '734285718') and name eq '*ELEC_PM_L1_EDPM_MSB_2_Q2*')";  
		
		// PMS old staging
//		String sourceGroupId = "62211";
//		String sourceDeviceQuery = "$filter=bygroupid(" + sourceGroupId + ")";
		
		
		Platform srcPlatform = platformUtil.resolvePlatform(sourceC8yURL, sourceTenantId + "/" + sourceUsername + ":" + sourcePassword);
		Platform trgPlatform = platformUtil.resolvePlatform(targetC8yURL, targetTenantId + "/" + targetUsername + ":" + targetPassword);
		
		deviceMigrationService.migrateDevices(sourceDeviceQuery, srcPlatform, trgPlatform, targetDeviceTemplate);

		log.info("done");
	}
	
	/**
	 * TODO remove hardcoded config fields, use a testcode-created source 
	 * device instead of a real device
	 * @throws Exception 
	 */
	@Test
	@DirtiesContext
	void migrateMsmtsOneDevice() throws Exception {		
		Awaitility.await().atMost(30, TimeUnit.SECONDS).ignoreExceptions().until(()->subscriptions.isRegisteredSuccessfully());
				
		// cleanup
		cumulocityService.deleteManagedObjectsByFragment(MigrationJobService.JOB_FRAGMENT_NAME);
		cumulocityService.deleteManagedObjectsByFragment(TEST_DEVICE_FRAGMENT);

		// source device & range
		// TODO remove hardcoded
		String sourceDeviceId = "23513";
		String sourceDeviceQuery = "$filter=id eq '" + sourceDeviceId + "'";		
		String extIdType = "ktg_acsid";
		String extIdVal = "1";
		String dateFrom = "2024-02-13T01:05:07.513+01:00";
		String dateTo = "2024-02-17T01:05:07.513+01:00";
		int numMigratedExpected = 384;

		// create test dev
		ManagedObjectRepresentation device1 = createTestDevice();
		log.info("Created " + device1.getId());
		// link by ext id
		setExternalId(device1.getId().getValue(), extIdType, extIdVal);

		// create job
		String jobName = "jobName" + UUID.randomUUID().toString();
		
		MigrationJobConfig jobConfig = MigrationJobConfig.builder()
				
			.jobName(jobName)
			.sourcePlatformHost(sourceC8yURL)
			.sourcePlatformLoginString(sourceTenantId + "/" + sourceUsername + ":" + sourcePassword)
			.targetPlatformHost(targetC8yURL)
			.targetPlatformLoginString(targetTenantId + "/" + targetUsername + ":" + targetPassword)
			.signalType(SignalType.MEASUREMENT)
			.sourceDevicesQuery(sourceDeviceQuery)
			.dateFrom(dateFrom)
			.dateTo(dateTo)
			.build();
		
		// cleanup 213202 /2114265
		// measurementMigrationService.deleteMeasurements(((MeasurementFilter) jobConfig.getSignalFilter()).bySource(GId.asGId("213202")));
			
		//start
		
		MigrationJob job = new MigrationJob(jobConfig);
		
		migrationJobService.runMigrationJob(job);	
		 
		var filter = new CustomInventoryFilter();
        filter.byQuery(String.format("$filter=(has('%s') and %s.config.jobName eq '%s')",
        		MigrationJobService.JOB_FRAGMENT_NAME,
        		MigrationJobService.JOB_FRAGMENT_NAME,
        		jobName));

        // await created
        await().atMost(30, TimeUnit.SECONDS).until(()->{
    		return cumulocityService.getManagedObjectsByFilter(filter).iterator().hasNext();
    	});
        
        ManagedObjectRepresentation moRep = cumulocityService.getManagedObjectsByFilter(filter).iterator().next();
        MigrationJob createdJob = ManagedObjectUtil.readObjectFromFragment(moRep, MigrationJob.class, MigrationJobService.JOB_FRAGMENT_NAME);
        
        assertTrue(createdJob.getConfig().equals(job.getConfig()));
        
        
        
        filter.byQuery(String.format("$filter=(has('%s') and %s.config.jobName eq '%s' and %s.status eq '%s')",
        		MigrationJobService.JOB_FRAGMENT_NAME,
        		MigrationJobService.JOB_FRAGMENT_NAME,
        		jobName,
        		MigrationJobService.JOB_FRAGMENT_NAME,
        		OperationStatus.SUCCESSFUL.toString()));
        
        // await completed
        await().atMost(300, TimeUnit.SECONDS).until(()->{
    		return cumulocityService.getManagedObjectsByFilter(filter).iterator().hasNext();
    	});
        
        
        moRep = cumulocityService.getManagedObjectsByFilter(filter).iterator().next();
        createdJob = ManagedObjectUtil.readObjectFromFragment(moRep, MigrationJob.class, MigrationJobService.JOB_FRAGMENT_NAME);
        
        
        assertTrue(createdJob.getDeviceReports().size() == 1);
        
        DeviceSignalMigrationReport deviceReport = createdJob.getDeviceReports().get(0);
        assertTrue(deviceReport.getError() == null);
		assertTrue(deviceReport.getMigrated() == numMigratedExpected);
		
		// query both devices and compare signals 1 by 1
		
		String targetDeviceId = deviceReport.getTargetDeviceId();
		
		Platform srcPlatform = platformUtil.resolvePlatform(job.getConfig().getSourcePlatformHost(), 
				job.getConfig().getSourcePlatformLoginString());			
		Platform targetPlatform = platformUtil.resolvePlatform(job.getConfig().getTargetPlatformHost(), 
				job.getConfig().getTargetPlatformLoginString());
		
		
		ArrayList<MeasurementRepresentation> srcMsmts = new ArrayList<>();
		MeasurementUtil.getMeasurements(sourceDeviceId, jobConfig.getSignalQueryParams(), srcPlatform.getMeasurementApi())
			.iterator().forEachRemaining(srcMsmts::add);
		
		ArrayList<MeasurementRepresentation> trgMsmts = new ArrayList<>();
		MeasurementUtil.getMeasurements(targetDeviceId, jobConfig.getSignalQueryParams(), targetPlatform.getMeasurementApi())
			.iterator().forEachRemaining(trgMsmts::add);
		
		assertEquals(srcMsmts.size(), trgMsmts.size());
		
		srcMsmts.forEach( sm -> {
			Optional<MeasurementRepresentation> found = trgMsmts.stream().filter( tm -> {
				return MeasurementUtil.equalIgnoringTenantSpecificFields(sm,tm);
			}).findFirst();
			assertTrue(found.isPresent());
			
			MeasurementUtil.trimTenantSpecificFields(sm);
			MeasurementUtil.trimTenantSpecificFields(found.get());
			assertEquals(sm.toJSON(), found.get().toJSON());
			trgMsmts.remove(found.get());
		});
	}
	
	@Test
	@DirtiesContext
	void migrateAlarmsOneDevice() throws Exception {		
		Awaitility.await().atMost(30, TimeUnit.SECONDS).ignoreExceptions().until(()->subscriptions.isRegisteredSuccessfully());
		
		// cleanup
		cumulocityService.deleteManagedObjectsByFragment(MigrationJobService.JOB_FRAGMENT_NAME);
		cumulocityService.deleteManagedObjectsByFragment(TEST_DEVICE_FRAGMENT);

		// source device & range
		// TODO remove hardcoded 
		String sourceDeviceId = "12067973";
		String extIdType = "ktg_Bms_Id";
		String extIdVal = "ACMV_PAHU_B1_PAHU_B1_1_CAV_CAV_B1_2";
		String dateFrom = "2024-02-01T01:05:07.513+01:00";
		String dateTo = "2024-02-24T23:05:07.513+01:00";
		int numMigratedExpected = 5;

		
		// create test dev
		ManagedObjectRepresentation device1 = createTestDevice();
		log.info("Created " + device1.getId());
		setExternalId(device1.getId().getValue(), extIdType, extIdVal);
		
		// create job
		String sourceDeviceQuery = "$filter=id eq '" + sourceDeviceId + "'";		
		
		String jobName = "jobName" + UUID.randomUUID().toString();
		
		MigrationJobConfig jobConfig = MigrationJobConfig.builder()
				
			.jobName(jobName)
			.sourcePlatformHost(sourceC8yURL)
			.sourcePlatformLoginString(sourceTenantId + "/" + sourceUsername + ":" + sourcePassword)
			.targetPlatformHost(targetC8yURL)
			.targetPlatformLoginString(targetTenantId + "/" + targetUsername + ":" + targetPassword)
			.signalType(SignalType.ALARM)
			.sourceDevicesQuery(sourceDeviceQuery)
			.dateFrom(dateFrom)
			.dateTo(dateTo)
			.build();
		
		
		//start
		
		MigrationJob job = new MigrationJob(jobConfig);
		
		migrationJobService.runMigrationJob(job);	
		 
		var filter = new CustomInventoryFilter();
        filter.byQuery(String.format("$filter=(has('%s') and %s.config.jobName eq '%s')",
        		MigrationJobService.JOB_FRAGMENT_NAME,
        		MigrationJobService.JOB_FRAGMENT_NAME,
        		jobName));

        // await created
        await().atMost(30, TimeUnit.SECONDS).until(()->{
    		return cumulocityService.getManagedObjectsByFilter(filter).iterator().hasNext();
    	});
        
        ManagedObjectRepresentation moRep = cumulocityService.getManagedObjectsByFilter(filter).iterator().next();
        MigrationJob createdJob = ManagedObjectUtil.readObjectFromFragment(moRep, MigrationJob.class, MigrationJobService.JOB_FRAGMENT_NAME);
        
        assertTrue(createdJob.getConfig().equals(job.getConfig()));
        
        
        
        filter.byQuery(String.format("$filter=(has('%s') and %s.config.jobName eq '%s' and %s.status eq '%s')",
        		MigrationJobService.JOB_FRAGMENT_NAME,
        		MigrationJobService.JOB_FRAGMENT_NAME,
        		jobName,
        		MigrationJobService.JOB_FRAGMENT_NAME,
        		OperationStatus.SUCCESSFUL.toString()));
        
        // await completed
        await().atMost(300, TimeUnit.SECONDS).until(()->{
    		return cumulocityService.getManagedObjectsByFilter(filter).iterator().hasNext();
    	});
        
        
        moRep = cumulocityService.getManagedObjectsByFilter(filter).iterator().next();
        createdJob = ManagedObjectUtil.readObjectFromFragment(moRep, MigrationJob.class, MigrationJobService.JOB_FRAGMENT_NAME);
        
        
        assertTrue(createdJob.getDeviceReports().size() == 1);
        
        DeviceSignalMigrationReport deviceReport = createdJob.getDeviceReports().get(0);
        assertTrue(deviceReport.getError() == null);
		assertTrue(deviceReport.getMigrated() == numMigratedExpected);
		
		
		// query both and compare 1 by 1
		
		
		String targetDeviceId = deviceReport.getTargetDeviceId();
		
		Platform srcPlatform = platformUtil.resolvePlatform(job.getConfig().getSourcePlatformHost(), 
				job.getConfig().getSourcePlatformLoginString());			
		Platform targetPlatform = platformUtil.resolvePlatform(job.getConfig().getTargetPlatformHost(), 
				job.getConfig().getTargetPlatformLoginString());
		
		
		ArrayList<AlarmRepresentation> srcSigs = new ArrayList<>();
		AlarmUtil.getAlarms(sourceDeviceId, jobConfig.getSignalQueryParams(), srcPlatform.getAlarmApi())
			.iterator().forEachRemaining(srcSigs::add);
		
		ArrayList<AlarmRepresentation> trgSigs = new ArrayList<>();
		AlarmUtil.getAlarms(targetDeviceId, jobConfig.getSignalQueryParams(), targetPlatform.getAlarmApi())
			.iterator().forEachRemaining(trgSigs::add);
		
		assertEquals(srcSigs.size(), trgSigs.size());
		
		srcSigs.forEach( sm -> {
			Optional<AlarmRepresentation> found = trgSigs.stream().filter( tm -> {
				return AlarmUtil.equalIgnoringTenantSpecificFields(sm,tm);
			}).findFirst();
			assertTrue(found.isPresent());
			
			AlarmUtil.trimTenantSpecificFields(sm);
			AlarmUtil.trimTenantSpecificFields(found.get());
			assertEquals(sm.toJSON(), found.get().toJSON());
			trgSigs.remove(found.get());
		});

	
		
		// rerun, check that it did not duplicate copies
		
		
		jobName = "jobName" + UUID.randomUUID().toString();
		jobConfig.setJobName(jobName);
		MigrationJob job2 = new MigrationJob(jobConfig);
		migrationJobService.runMigrationJob(job2);	
		
		filter.byQuery(String.format("$filter=(has('%s') and %s.config.jobName eq '%s' and %s.status eq '%s')",
        		MigrationJobService.JOB_FRAGMENT_NAME,
        		MigrationJobService.JOB_FRAGMENT_NAME,
        		jobName,
        		MigrationJobService.JOB_FRAGMENT_NAME,
        		OperationStatus.SUCCESSFUL.toString()));
        
        // await completed
        await().atMost(30, TimeUnit.SECONDS).until(()->{
    		return cumulocityService.getManagedObjectsByFilter(filter).iterator().hasNext();
    	});

        
        moRep = cumulocityService.getManagedObjectsByFilter(filter).iterator().next();
        createdJob = ManagedObjectUtil.readObjectFromFragment(moRep, MigrationJob.class, MigrationJobService.JOB_FRAGMENT_NAME);
        
        assertTrue(createdJob.getDeviceReports().size() == 1);
        
        deviceReport = createdJob.getDeviceReports().get(0);
        assertTrue(deviceReport.getError() == null);
		assertEquals(0, deviceReport.getMigrated());
		assertEquals(numMigratedExpected, deviceReport.getDuplicatesSkipped());
				
		
		
		
		log.info("done");
			
	}
	
	
	@Test
	@DirtiesContext
	void migrateEventsOneDevice() throws Exception {		
		Awaitility.await().atMost(30, TimeUnit.SECONDS).ignoreExceptions().until(()->subscriptions.isRegisteredSuccessfully());
		
		// cleanup
		cumulocityService.deleteManagedObjectsByFragment(MigrationJobService.JOB_FRAGMENT_NAME);
		cumulocityService.deleteManagedObjectsByFragment(TEST_DEVICE_FRAGMENT);

		// source device & range
		// TODO remove hardcoded 
//		String sourceDeviceId = "977248780";
//		String extIdType = "ktg_control_pc";
//		String extIdVal = "007107";
//		String dateFrom = "2024-02-27T18:00:07.513+01:00";
//		String dateTo = "2024-02-27T18:05:07.513+01:00";
//		int numMigratedExpected = 173;

		String sourceDeviceId = "2899021";
		String extIdType = "ktg_name_cctv";
		String extIdVal = "B1-C-02";
		String dateFrom = "2024-02-07T08:00:07.513+01:00";
		String dateTo = "2024-02-10T08:05:07.513+01:00";
		int numMigratedExpected = 20;

		
		// create test dev
		ManagedObjectRepresentation device1 = createTestDevice();
		log.info("Created " + device1.getId());
		setExternalId(device1.getId().getValue(), extIdType, extIdVal);
		
		// create job
		String sourceDeviceQuery = "$filter=id eq '" + sourceDeviceId + "'";		
		
		String jobName = "jobName" + UUID.randomUUID().toString();
		
		MigrationJobConfig jobConfig = MigrationJobConfig.builder()
				
			.jobName(jobName)
			.sourcePlatformHost(sourceC8yURL)
			.sourcePlatformLoginString(sourceTenantId + "/" + sourceUsername + ":" + sourcePassword)
			.targetPlatformHost(targetC8yURL)
			.targetPlatformLoginString(targetTenantId + "/" + targetUsername + ":" + targetPassword)
			.signalType(SignalType.EVENT)
			.sourceDevicesQuery(sourceDeviceQuery)
			.dateFrom(dateFrom)
			.dateTo(dateTo)
			.build();
		
		
		//start
		
		MigrationJob job = new MigrationJob(jobConfig);
		
		migrationJobService.runMigrationJob(job);	
		 
		var filter = new CustomInventoryFilter();
        filter.byQuery(String.format("$filter=(has('%s') and %s.config.jobName eq '%s')",
        		MigrationJobService.JOB_FRAGMENT_NAME,
        		MigrationJobService.JOB_FRAGMENT_NAME,
        		jobName));

        // await created
        await().atMost(30, TimeUnit.SECONDS).until(()->{
    		return cumulocityService.getManagedObjectsByFilter(filter).iterator().hasNext();
    	});
        
        ManagedObjectRepresentation moRep = cumulocityService.getManagedObjectsByFilter(filter).iterator().next();
        MigrationJob createdJob = ManagedObjectUtil.readObjectFromFragment(moRep, MigrationJob.class, MigrationJobService.JOB_FRAGMENT_NAME);
        
        assertTrue(createdJob.getConfig().equals(job.getConfig()));
        
        
        
        filter.byQuery(String.format("$filter=(has('%s') and %s.config.jobName eq '%s' and %s.status eq '%s')",
        		MigrationJobService.JOB_FRAGMENT_NAME,
        		MigrationJobService.JOB_FRAGMENT_NAME,
        		jobName,
        		MigrationJobService.JOB_FRAGMENT_NAME,
        		OperationStatus.SUCCESSFUL.toString()));
        
        // await completed
        await().atMost(300, TimeUnit.SECONDS).until(()->{
    		return cumulocityService.getManagedObjectsByFilter(filter).iterator().hasNext();
    	});
        
        
        moRep = cumulocityService.getManagedObjectsByFilter(filter).iterator().next();
        createdJob = ManagedObjectUtil.readObjectFromFragment(moRep, MigrationJob.class, MigrationJobService.JOB_FRAGMENT_NAME);
        
        
        assertTrue(createdJob.getDeviceReports().size() == 1);
        
        DeviceSignalMigrationReport deviceReport = createdJob.getDeviceReports().get(0);
        assertTrue(deviceReport.getError() == null);
		assertEquals(numMigratedExpected, deviceReport.getMigrated());
		
		
		// query both and compare 1 by 1
		
		
		String targetDeviceId = deviceReport.getTargetDeviceId();
		
		Platform srcPlatform = platformUtil.resolvePlatform(job.getConfig().getSourcePlatformHost(), 
				job.getConfig().getSourcePlatformLoginString());			
		Platform targetPlatform = platformUtil.resolvePlatform(job.getConfig().getTargetPlatformHost(), 
				job.getConfig().getTargetPlatformLoginString());
		
		
		ArrayList<EventRepresentation> srcSigs = new ArrayList<>();
		EventUtil.getEvents(sourceDeviceId, jobConfig.getSignalQueryParams(), srcPlatform.getEventApi())
			.iterator().forEachRemaining(srcSigs::add);
		
		ArrayList<EventRepresentation> trgSigs = new ArrayList<>();
		EventUtil.getEvents(targetDeviceId, jobConfig.getSignalQueryParams(), targetPlatform.getEventApi())
			.iterator().forEachRemaining(trgSigs::add);
		
		assertEquals(srcSigs.size(), trgSigs.size());
		
		srcSigs.forEach( sm -> {
			Optional<EventRepresentation> found = trgSigs.stream().filter( tm -> {
				return EventUtil.equalIgnoringTenantSpecificFields(sm,tm);
			}).findFirst();
			assertTrue(found.isPresent());
			
			EventUtil.trimTenantSpecificFields(sm);
			EventUtil.trimTenantSpecificFields(found.get());
			assertEquals(sm.toJSON(), found.get().toJSON());
			trgSigs.remove(found.get());
		});

	
		
		// rerun, check that it did not duplicate copies
		
		
		jobName = "jobName" + UUID.randomUUID().toString();
		jobConfig.setJobName(jobName);
		MigrationJob job2 = new MigrationJob(jobConfig);
		migrationJobService.runMigrationJob(job2);	
		
		filter.byQuery(String.format("$filter=(has('%s') and %s.config.jobName eq '%s' and %s.status eq '%s')",
        		MigrationJobService.JOB_FRAGMENT_NAME,
        		MigrationJobService.JOB_FRAGMENT_NAME,
        		jobName,
        		MigrationJobService.JOB_FRAGMENT_NAME,
        		OperationStatus.SUCCESSFUL.toString()));
        
        // await completed
        await().atMost(30, TimeUnit.SECONDS).until(()->{
    		return cumulocityService.getManagedObjectsByFilter(filter).iterator().hasNext();
    	});

        
        moRep = cumulocityService.getManagedObjectsByFilter(filter).iterator().next();
        createdJob = ManagedObjectUtil.readObjectFromFragment(moRep, MigrationJob.class, MigrationJobService.JOB_FRAGMENT_NAME);
        
        assertTrue(createdJob.getDeviceReports().size() == 1);
        
        deviceReport = createdJob.getDeviceReports().get(0);
        assertTrue(deviceReport.getError() == null);
		assertEquals(0, deviceReport.getMigrated());
		assertEquals(numMigratedExpected, deviceReport.getDuplicatesSkipped());
			
		
		
		
		log.info("done");
			
	}
	
	
	@Test
	@DirtiesContext
	void testRetry() throws Exception {		
		
		Awaitility.await().atMost(30, TimeUnit.SECONDS).ignoreExceptions().until(()->subscriptions.isRegisteredSuccessfully());
		
		// cleanup
		cumulocityService.deleteManagedObjectsByFragment(MigrationJobService.JOB_FRAGMENT_NAME);
		cumulocityService.deleteManagedObjectsByFragment(TEST_DEVICE_FRAGMENT);

		// source device & range
		// TODO remove hardcoded 
		String sourceDeviceId = "977248780";
		String extIdType = "ktg_control_pc";
		String extIdVal = "007107";
		String dateFrom = "2024-02-27T18:00:07.513+01:00";
		String dateTo = "2024-02-27T18:05:07.513+01:00";
		int numMigratedExpected = 173;
		
		
		// create job
		String sourceDeviceQuery = "$filter=id eq '" + sourceDeviceId + "'";		
		
		String jobName = "jobName" + UUID.randomUUID().toString();
		
		MigrationJobConfig jobConfig = MigrationJobConfig.builder()
				
			.jobName(jobName)
			.sourcePlatformHost(sourceC8yURL)
			.sourcePlatformLoginString(sourceTenantId + "/" + sourceUsername + ":" + sourcePassword)
			.targetPlatformHost(targetC8yURL)
			.targetPlatformLoginString(targetTenantId + "/" + targetUsername + ":" + targetPassword)
			.signalType(SignalType.EVENT)
			.sourceDevicesQuery(sourceDeviceQuery)
			.dateFrom(dateFrom)
			.dateTo(dateTo)
			.build();
		
		
		//start
		
		MigrationJob job = new MigrationJob(jobConfig);
		
		migrationJobService.runMigrationJob(job);	
		 
		var filter = new CustomInventoryFilter();
        filter.byQuery(String.format("$filter=(has('%s') and %s.config.jobName eq '%s')",
        		MigrationJobService.JOB_FRAGMENT_NAME,
        		MigrationJobService.JOB_FRAGMENT_NAME,
        		jobName));

        // await created
        await().atMost(30, TimeUnit.SECONDS).until(()->{
    		return cumulocityService.getManagedObjectsByFilter(filter).iterator().hasNext();
    	});
        
        ManagedObjectRepresentation moRep = cumulocityService.getManagedObjectsByFilter(filter).iterator().next();
        MigrationJob createdJob = ManagedObjectUtil.readObjectFromFragment(moRep, MigrationJob.class, MigrationJobService.JOB_FRAGMENT_NAME);
        
        assertTrue(createdJob.getConfig().equals(job.getConfig()));
        
        
        
        filter.byQuery(String.format("$filter=(has('%s') and %s.config.jobName eq '%s' and %s.status eq '%s')",
        		MigrationJobService.JOB_FRAGMENT_NAME,
        		MigrationJobService.JOB_FRAGMENT_NAME,
        		jobName,
        		MigrationJobService.JOB_FRAGMENT_NAME,
        		OperationStatus.FAILED.toString()));
        
        //should fail - no matching device
        // await completed
        await().atMost(300, TimeUnit.SECONDS).until(()->{
    		return cumulocityService.getManagedObjectsByFilter(filter).iterator().hasNext();
    	});
        
        
        moRep = cumulocityService.getManagedObjectsByFilter(filter).iterator().next();
        createdJob = ManagedObjectUtil.readObjectFromFragment(moRep, MigrationJob.class, MigrationJobService.JOB_FRAGMENT_NAME);
        
        
        assertTrue(createdJob.getDeviceReports().size() == 1);
        
        DeviceSignalMigrationReport deviceReport = createdJob.getDeviceReports().get(0);
        assertTrue(deviceReport.getError() != null);
		
        
        // create test dev
 		ManagedObjectRepresentation device1 = createTestDevice();
 		log.info("Created " + device1.getId());
 		setExternalId(device1.getId().getValue(), extIdType, extIdVal);
     		
		
 		
 		migrationJobService.retryMigrationJob(createdJob,false);
 		
 		
		
		filter.byQuery(String.format("$filter=(has('%s') and %s.config.jobName eq '%s' and %s.status eq '%s')",
        		MigrationJobService.JOB_FRAGMENT_NAME,
        		MigrationJobService.JOB_FRAGMENT_NAME,
        		jobName,
        		MigrationJobService.JOB_FRAGMENT_NAME,
        		OperationStatus.SUCCESSFUL.toString()));
        
        // await completed
        await().atMost(30, TimeUnit.SECONDS).until(()->{
    		return cumulocityService.getManagedObjectsByFilter(filter).iterator().hasNext();
    	});
		
		
		log.info("done");
			
	}
	
	private void setExternalId(String id, String extIdType, String extIdVal) {
		subscriptions.runForTenant(subscriptions.getAll().iterator().next().getTenant(), () -> {
			ExternalIDRepresentation extIdRep = new ExternalIDRepresentation();
			extIdRep.setType(extIdType);
			extIdRep.setExternalId(extIdVal);
			ManagedObjectRepresentation moRep = new ManagedObjectRepresentation();
			moRep.setId(GId.asGId(id));
			extIdRep.setManagedObject(moRep);
			identityApi.create(extIdRep );
		});
		
	}

	public ManagedObjectRepresentation createTestDevice() {
		ManagedObjectRepresentation moRep = new ManagedObjectRepresentation();
		String uuid = UUID.randomUUID().toString();
		moRep.set(new IsDevice());
		moRep.setName("Integration Test Device" + uuid);
		moRep.set(uuid, TEST_DEVICE_FRAGMENT);
		ManagedObjectRepresentation managedObject = cumulocityService.createManagedObject(moRep);
		return managedObject;
	}
	
}

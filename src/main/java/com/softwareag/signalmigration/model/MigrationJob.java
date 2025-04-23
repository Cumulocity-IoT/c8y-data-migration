package com.softwareag.signalmigration.model;

import java.util.ArrayList;

import com.cumulocity.model.authentication.CumulocityBasicCredentials;
import com.cumulocity.model.operation.OperationStatus;
import com.cumulocity.sdk.client.Platform;
import com.cumulocity.sdk.client.PlatformImpl;
import com.fasterxml.jackson.annotation.JsonIgnore;

import lombok.Data;
import lombok.ToString;

@Data
public class MigrationJob {
	
	public static final int VERSION = 2;

	private MigrationJobConfig config;
	
	private volatile int numTotalDevices = -1;
	private volatile int numCompletedDevices = 0;
	
	@JsonIgnore
	private Platform sourcePlatform;
	
	@JsonIgnore
	private Platform targetPlatform; 
	
	private String c8yId;
	
	private OperationStatus status = OperationStatus.PENDING;
	
	@ToString.Exclude
	private ArrayList<DeviceSignalMigrationReport> deviceReports = new ArrayList<>();
	
	private int version = VERSION;
	
	public MigrationJob() {
	}

	public MigrationJob(MigrationJobConfig jobConfig) {
		super();
		this.config = jobConfig;
		
	}
	
	public synchronized void addDeviceReport(DeviceSignalMigrationReport report) {
		deviceReports.add(report);
	}
	
	public synchronized int removeReportsWithErrors() {
		int size = deviceReports.size();
		deviceReports.removeIf( rep -> {
			return rep.getError() != null || rep.getErrors() > 0;
		});
		numCompletedDevices = deviceReports.size(); 
		return size - deviceReports.size();
	}

	public synchronized boolean isDeviceProcessed(String sourceDeviceId) {
		return deviceReports.stream().anyMatch( r -> {
			return r.getSourceDeviceId().equals(sourceDeviceId);
		});
	}
	
	public synchronized void updateStatus() {
		if (status.equals(OperationStatus.EXECUTING)) {
			// initialized 
			if (deviceReports.size() > numTotalDevices) {
				throw new IllegalStateException(); // sanity check
			}
			numCompletedDevices = deviceReports.size();  
			if (deviceReports.size() == numTotalDevices) {
				boolean hasErrors = deviceReports.stream().anyMatch(rep -> {
					return rep.getError() != null || rep.getErrors() > 0;
				});
				if (hasErrors) {
					status = OperationStatus.FAILED;
				} else {
					status = OperationStatus.SUCCESSFUL;
				}
			} 
		}	
	}
	
}

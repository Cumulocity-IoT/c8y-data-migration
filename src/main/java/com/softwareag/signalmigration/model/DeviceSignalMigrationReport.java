package com.softwareag.signalmigration.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class DeviceSignalMigrationReport {
//	private SignalType signalType;
	private String sourceDeviceId; 
	private String targetDeviceId;
	public int migrated;
	public int errors;
	public int duplicatesSkipped;
	private String error;
	private long durationSec;
}

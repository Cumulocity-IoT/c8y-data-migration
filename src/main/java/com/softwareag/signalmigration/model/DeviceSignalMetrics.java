package com.softwareag.signalmigration.model;

import org.joda.time.DateTime;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class DeviceSignalMetrics {
	private String deviceId;
	private SignalType signalType;
	private long count;
	private DateTime dateFrom;
	//private DateTime dateTo;
}

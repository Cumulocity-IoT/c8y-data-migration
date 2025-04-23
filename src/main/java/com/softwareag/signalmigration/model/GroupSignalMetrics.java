package com.softwareag.signalmigration.model;

import java.util.List;
import java.util.LongSummaryStatistics;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class GroupSignalMetrics {
	private SignalMetricsCollectionRequestDTO collectionConfig;
	private LongSummaryStatistics alarmStatistics;
	private LongSummaryStatistics measurementStatistics;
	private LongSummaryStatistics eventStatistics;
	private  List<DeviceSignalMetrics> deviceSignalMetrics;
}

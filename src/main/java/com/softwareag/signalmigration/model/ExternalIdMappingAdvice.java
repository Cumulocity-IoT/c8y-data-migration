package com.softwareag.signalmigration.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ExternalIdMappingAdvice {
	private String sourceExternalId;
	private String targetExternalId;
}

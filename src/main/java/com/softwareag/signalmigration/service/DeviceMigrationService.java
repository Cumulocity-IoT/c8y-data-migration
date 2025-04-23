package com.softwareag.signalmigration.service;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Component;

import com.cumulocity.rest.representation.identity.ExternalIDRepresentation;
import com.cumulocity.rest.representation.inventory.ManagedObjectRepresentation;
import com.cumulocity.sdk.client.Platform;
import com.cumulocity.sdk.client.QueryParam;
import com.cumulocity.sdk.client.identity.ExternalIDCollection;
import com.cumulocity.sdk.client.inventory.InventoryFilter;
import com.softwareag.signalmigration.util.CustomInventoryFilter;
import com.softwareag.signalmigration.util.ManagedObjectUtil;

import lombok.extern.slf4j.Slf4j;

/**
 * Helper for testing.
 * 
 * Given device query in the source platform, create dummy devices in the target platform 
 * with the same external ids as the source devices
 */
@Slf4j
@Component
public class DeviceMigrationService {	
	

	public void migrateDevices(String sourceDevicesQuery,
			Platform sourceC8yPlatform,
			Platform targetC8yPlatform) {
		migrateDevices(sourceDevicesQuery, sourceC8yPlatform, targetC8yPlatform, null);
	}

	public void migrateDevices(String sourceDevicesQuery,
			Platform sourceC8yPlatform,
			Platform targetC8yPlatform,
			ManagedObjectRepresentation targetDeviceTemplate) {
		
		InventoryFilter filter = new CustomInventoryFilter().byQuery(sourceDevicesQuery);
		
		Iterable<ManagedObjectRepresentation> sourceDevicesItor = sourceC8yPlatform.getInventoryApi().getManagedObjectsByFilter(filter).get(200).allPages();
		
		ArrayList<String> sourceDeviceIds = new ArrayList<>();
		for (ManagedObjectRepresentation sourceDevice : sourceDevicesItor) {
			sourceDeviceIds.add(sourceDevice.getId().getValue());
			try {
				migrateDevice(sourceDevice, sourceC8yPlatform, targetC8yPlatform, targetDeviceTemplate);
			} catch (Exception e) {
				log.error("Error", e);
			}
		}
		
	}

	private void migrateDevice(ManagedObjectRepresentation sourceDevice, Platform sourceC8yPlatform,
			Platform targetC8yPlatform, ManagedObjectRepresentation targetDeviceTemplate) throws IllegalAccessException, InvocationTargetException {
		List<ExternalIDRepresentation> externalIds = sourceC8yPlatform.getIdentityApi().getExternalIdsOfGlobalId(sourceDevice.getId()).get().getExternalIds();
		
		ManagedObjectRepresentation targetDevice = new ManagedObjectRepresentation();
		if (targetDeviceTemplate != null) {
			BeanUtils.copyProperties(targetDeviceTemplate, targetDevice);
		}

		BeanUtils.copyProperties(sourceDevice, targetDevice);
		
		if (targetDeviceTemplate != null) {
			targetDeviceTemplate.getAttrs().keySet().forEach( k -> {
				if (!targetDevice.getAttrs().containsKey(k)) {
					targetDevice.getAttrs().put(k, targetDeviceTemplate.getAttrs().get(k));
				}
			});
		}
		
		ManagedObjectUtil.filterNonUpdateableMnagedObjectFields(targetDevice);
		
		ManagedObjectRepresentation createdMO = targetC8yPlatform.getInventoryApi().create(targetDevice);
		
		ManagedObjectRepresentation externalIdDevice = new ManagedObjectRepresentation();
		externalIdDevice.setId(createdMO.getId());
		
		externalIds.forEach( eid -> {
			eid.setSelf(null);			
			eid.setManagedObject(externalIdDevice);			
			targetC8yPlatform.getIdentityApi().create(eid);
		});		
		
		log.info("Created migrated device id {} for source device id {} with externalIds {}", 
				externalIdDevice.getId().getValue(),
				sourceDevice.getId().getValue(),
				externalIds
				);
	}

	
}

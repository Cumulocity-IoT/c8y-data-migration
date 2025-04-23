package com.softwareag.signalmigration.service;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

import com.cumulocity.microservice.subscription.model.MicroserviceSubscriptionAddedEvent;
import com.cumulocity.microservice.subscription.service.MicroserviceSubscriptionsService;
import com.cumulocity.model.idtype.GId;
import com.cumulocity.rest.representation.inventory.ManagedObjectRepresentation;
import com.cumulocity.rest.representation.measurement.MeasurementRepresentation;
import com.cumulocity.rest.representation.operation.OperationRepresentation;
import com.cumulocity.sdk.client.Platform;
import com.cumulocity.sdk.client.QueryParam;
import com.cumulocity.sdk.client.devicecontrol.OperationCollection;
import com.cumulocity.sdk.client.devicecontrol.OperationFilter;
import com.cumulocity.sdk.client.inventory.InventoryFilter;
import com.cumulocity.sdk.client.inventory.ManagedObjectCollection;
import com.cumulocity.sdk.client.measurement.MeasurementFilter;
import com.softwareag.signalmigration.util.CustomInventoryFilter;
import com.softwareag.signalmigration.util.CustomQueryParam;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class CumulocityService {
	
    @Autowired
    private MicroserviceSubscriptionsService service;

    @Autowired
    private Platform platform;

    //@Value("${c8y.tenant}")
    private String tenant;
    
    /**
     * do not delete, create or update inventory objects
     * Note: operations will still be created! 
     */
    @Value("${CumulocityService.disablePlatformWrites:false}")
    protected boolean disablePlatformWrites;

    public CumulocityService() {
    }
    
    
	@EventListener
	public void init(final MicroserviceSubscriptionAddedEvent event) {
		tenant = event.getCredentials().getTenant();
    }
    
    
	public OperationRepresentation getOperation(String id) {
    	return service.callForTenant(tenant, () -> {
            return this.platform.getDeviceControlApi()
                    .getOperation(GId.asGId(id));
        });
    }
    
    
	public OperationRepresentation updateOperation(OperationRepresentation op) {
    	return service.callForTenant(tenant, () -> {
            return this.platform.getDeviceControlApi().update(op);
        });
    }
    
    
	public ManagedObjectRepresentation getManagedObject(String id) {
    	return service.callForTenant(tenant, () -> {
            return this.platform.getInventoryApi().get(GId.asGId(id));
        });
    }
    
    
	public ManagedObjectRepresentation createManagedObject(ManagedObjectRepresentation moRep) {
    	if (disablePlatformWrites) {
    		log.info("skipping: createManagedObject; disablePlatformWrites is true");
    		return null;
    	}
    	return service.callForTenant(tenant, () -> {
            return this.platform.getInventoryApi().create(moRep);
        });
    }
    
    
	public ManagedObjectRepresentation updateManagedObject(ManagedObjectRepresentation moRep) {
    	if (disablePlatformWrites) {
    		log.info("skipping: updateManagedObject; disablePlatformWrites is true");
    		return null;
    	}
    	return service.callForTenant(tenant, () -> {
            return this.platform.getInventoryApi().update(moRep);
        });
    }
    
    
	public void deleteManagedObject(String id) {
    	if (disablePlatformWrites) {
    		log.info("skipping: deleteManagedObject; disablePlatformWrites is true");
    		return;
    	}
    	service.runForTenant(tenant, () -> {
            this.platform.getInventoryApi().delete(GId.asGId(id));
        });
    }
    
    
	public void deleteManagedObjectsByType(String type) {
    	CustomInventoryFilter filter = new CustomInventoryFilter().byQuery(
				String.format("type eq '%s'",
						type));
		deleteManagedObjectsByFilter(filter);
    }
    
    
	public void deleteManagedObjectsByFragment(String fragment) {
    	CustomInventoryFilter filter = new CustomInventoryFilter().byQuery(
				String.format("has('%s')",
						fragment));
		deleteManagedObjectsByFilter(filter);
    }
    
    
	public void deleteManagedObjectsByFilter(InventoryFilter filter) {
    	Iterable<ManagedObjectRepresentation> managedObjectsByFilter = getManagedObjectsByFilter(filter);
    	if (managedObjectsByFilter == null) {
    		return;
    	}
    	ArrayList<String> idsToDelete = new ArrayList<String>();  
    	Iterator<ManagedObjectRepresentation> iterator = managedObjectsByFilter.iterator();
    	while (iterator.hasNext()) {
    		ManagedObjectRepresentation mo = iterator.next();
    		idsToDelete.add(mo.getId().getValue());
    		
    	}
    	idsToDelete.forEach( id -> {
    		try {
    			log.trace("Deleting " + id);
    			deleteManagedObject(id);
    		} catch (Exception e) {
    			log.error("Error deleting " + id);
    		}
    	});
    	log.info("Deleted filtered ManagedObjects, total: " + idsToDelete.size());
    }
    
    
	public Iterable<ManagedObjectRepresentation> getManagedObjectsByFilter(InventoryFilter filter) {
    	return service.callForTenant(tenant, () -> {
    		return this.platform.getInventoryApi().getManagedObjectsByFilter(filter).get().allPages();
        });
    }
    
    
	public Iterable<ManagedObjectRepresentation> getManagedObjectsByFilter(InventoryFilter filter, int pageSize) {
    	return service.callForTenant(tenant, () -> {
    		return this.platform.getInventoryApi().getManagedObjectsByFilter(filter).get(pageSize).allPages();
        });
    }
    
    
	public ManagedObjectCollection getManagedObjectsCollectionByFilter(InventoryFilter filter) {
    	return service.callForTenant(tenant, () -> {
    		ManagedObjectCollection collection = this.platform.getInventoryApi().getManagedObjectsByFilter(filter);
    		return collection;
//    		PagedManagedObjectCollectionRepresentation page = collection.get();
//    		collection.getNextPage(page);//    		
//    		return collectionRepresentation;
        });
    }    
    
 	public OperationRepresentation createOperation(OperationRepresentation operation) {    	
    	return service.callForTenant(tenant, () -> {
    		return this.platform.getDeviceControlApi().create(operation);
        });
    }
    
    
	public Iterable<OperationRepresentation> getOperations(String deviceId) { //  c8y id of the device
		OperationFilter filter = new OperationFilter();
		filter.byDevice(deviceId);
		OperationCollection operationCollection = service.callForTenant(tenant, () -> {
     		return this.platform.getDeviceControlApi().getOperationsByFilter(filter);
         });
			return operationCollection.get().allPages();
    }

	
	public void createMeasurements(MeasurementRepresentation measurementRepresentation) {
		 service.runForTenant(tenant, () -> {
    		 this.platform.getMeasurementApi().create(measurementRepresentation);
        });
		
	}

	
	public Iterable<MeasurementRepresentation> getMeasurementsByFilter(MeasurementFilter measurementFilter) {
		return service.callForTenant(tenant, () -> {
			return this.platform.getMeasurementApi().getMeasurementsByFilter(measurementFilter).get(200).allPages();
		});
	}

	
	public void deleteMeasurements(MeasurementFilter measurementFilter) {
		service.runForTenant(tenant, () -> {
   		 this.platform.getMeasurementApi().deleteMeasurementsByFilter(measurementFilter);
       });
		
	}

	
	public void addChildAssets(GId groupId, GId deviceId) {
		service.runForTenant(tenant, () -> {
			this.platform.getInventoryApi().getManagedObjectApi(groupId).addChildAssets(deviceId);
		});

	}

	
	public void deleteChildAsset(GId groupId, GId deviceId) {
		service.runForTenant(tenant, () -> {
			this.platform.getInventoryApi().getManagedObjectApi(groupId).deleteChildAsset(deviceId);
		});
	}

	public List<ManagedObjectRepresentation> getManagedObjectsByFilter(QueryParam queryParam) {
		
		return service.callForTenant(tenant, () -> {
			return this.platform.getInventoryApi().getManagedObjects()
					.get(2000, queryParam).getManagedObjects();
		});
	}
		
	/**
	 * Iterate over managed objects with efficient paging (via ordering by id) 
	 * @param action
	 * @param query c8y query expressions such as 
	 * 	"has('classification_complete') and NCMRecord.equipmentData.equiNo eq '18'"
	 */
	
	public void processManagedObjects(String query, Consumer<ManagedObjectRepresentation> action) {
		int pageSize = 2000;
		int pageFetchDelayMillis = 100;
		
		service.runForTenant(tenant, () -> {			
			boolean done = false;
			String last_id = "0";
			int pageCounter = 0;
			int processedDevicesCount = 0;
			AtomicInteger processingErrors = new AtomicInteger(0);
			
			while (!done) {
				try {
					pageCounter++;
					log.debug("Requesting page {}", pageCounter);

					// build query based on last retrieved id (this speeds up performance significantly)

					final QueryParam q = CustomQueryParam.QUERY.setValue(buildQueryById(query, last_id)).toQueryParam();

					//String query = "$filter=_id gt '" + last_id + "'$orderby=_id asc";
					//final QueryParam q = CustomQueryParam.DEVICE_QUERY.setValue(query).toQueryParam();

					final List<ManagedObjectRepresentation> managedObjects = this.platform.getInventoryApi().getManagedObjects().get(pageSize, q).getManagedObjects();					
					final int size = managedObjects.size();
					if (size > 0) {
						log.debug("Processing {} devices", size);
						// process them						
						managedObjects.forEach( mo -> {
							try {
								action.accept(mo);
							} catch (Exception e) {
								log.error("Error processing", e);
								processingErrors.incrementAndGet();
							}
						});	
						processedDevicesCount+=size;
						//meterRegistry.counter("svcMonitoring.checkAndUpdateFleet.processedDevices").increment(size);
						last_id = managedObjects.get(size-1).getId().getValue();
					}

					done = managedObjects.size() < pageSize;
					
					if (processedDevicesCount % 5000 == 0) {
						log.info("processedDevicesCount: " + processedDevicesCount);
					}
					
					Thread.sleep(pageFetchDelayMillis);
					
				} catch (Throwable e) {
					processingErrors.incrementAndGet();
					//meterRegistry.counter("svcMonitoring.checkAndUpdateFleet.processingErrors").increment();
					log.error("Error looping thru fleet on check & update" + e.getMessage(), e);
				}
			}
			
			//meterRegistry.counter("svcMonitoring.checkAndUpdateFleet.runs").increment();
			
			log.trace(String.format("Completed processing. processed: %d, errors: %d",processedDevicesCount, processingErrors.get()));
		});
	}	
	
	private String buildQueryById(final String query, final String last_id) {
		String full_query = "$filter=_id gt '" + last_id + "'";
		if (StringUtils.isNotBlank(query)) {
			full_query += " and (" + query + ")";
		}
		return full_query + "$orderby=_id asc";
	}
}

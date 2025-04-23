package com.softwareag.signalmigration.util;

import java.util.Map;

import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormat;
import org.svenson.JSONParser;

import com.cumulocity.model.JSONBase;
import com.cumulocity.rest.representation.AbstractExtensibleRepresentation;
import com.cumulocity.rest.representation.inventory.ManagedObjectRepresentation;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.fasterxml.jackson.datatype.joda.cfg.JacksonJodaDateFormat;
import com.fasterxml.jackson.datatype.joda.ser.LocalDateSerializer;

public class ManagedObjectUtil {
	
	private final static JSONParser jsonParser = JSONBase.getJSONParser();

	private static ObjectMapper objectMapper;
        
	@SuppressWarnings("unchecked")
	public static Object getNestedProperty(AbstractExtensibleRepresentation rep, String path) {
		Map<String, Object> map = rep.getAttrs();
		
		String[] split = path.split("\\.");
		
		for (int i = 0; i < split.length-1; i++) {
			String segment = split[i];
			if (!map.containsKey(segment)) return null;
			if (!(map.get(segment) instanceof Map)) return null;
			map = (Map<String, Object>) map.get(segment);			
		}
		return map.get(split[split.length-1]);		
	}
	
	public static boolean setNestedProperty(AbstractExtensibleRepresentation rep, String path, Object value) {
		Map<String, Object> map = rep.getAttrs();
		
		String[] split = path.split("\\.");
		
		for (int i = 0; i < split.length-1; i++) {
			String segment = split[i];
			if (!map.containsKey(segment)) return false;
			map = (Map<String, Object>) map.get(segment);			
		}
		
		map.put(split[split.length-1], value);
		return true;		
	}
	
	
	private static ObjectMapper getObjectMapper() {
		if (objectMapper != null) {
			return objectMapper;
		}
		objectMapper = new ObjectMapper();
		SimpleModule customJodaModule = new SimpleModule();
        customJodaModule.addSerializer(LocalDate.class, 
                new LocalDateSerializer(new JacksonJodaDateFormat(DateTimeFormat.forPattern("dd.MM.yyyy")), 1));

        objectMapper.registerModule(new JodaModule());
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        return objectMapper.findAndRegisterModules()
         .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        
	}
	
	public static void writeObjectAsFragment(ManagedObjectRepresentation managedObjectRepresentation, Object object, String fragmentName) {
		
		Map<String, Object> map = getObjectMapper().convertValue(object, new TypeReference<Map<String, Object>>() {});
	
		managedObjectRepresentation.set(map, fragmentName);
	}
	
	public static <V> V readObjectFromFragment(ManagedObjectRepresentation managedObjectRepresentation,	
			Class <V> objectClass, String fragmentName)  {
		@SuppressWarnings("unchecked")
		Map<String, Object> map = (Map<String, Object>) ManagedObjectUtil.getNestedProperty(managedObjectRepresentation, fragmentName);
		
		if (null == map) {
			return null;								
		} 
		
		V object = (V) getObjectMapper().convertValue(map, objectClass);
		return object;
	}
	
	/**
	 * Parse json using the c8y SDK parser; enables parsing 
	 * of AbstractExtensibleRepresentation subclasses
	 * @param <T>
	 * @param json
	 * @param targetType
	 * @return
	 */
	public static <T> T parseC8yRepresenationJSON(final String jsondata, final Class<T> targetType) {
        
        if (jsondata == null) {
            throw new IllegalArgumentException("Converting RealTimeData to json returned null");
        } else {
            final T parsed = jsonParser.parse(targetType, jsondata);
            return parsed;
        }
    }
	
	public static void filterNonUpdateableMnagedObjectFields(ManagedObjectRepresentation moRep) {
		moRep.setId(null);
		moRep.setLastUpdatedDateTime(null);
		moRep.setCreationDateTime(null);
		moRep.setChildAdditions(null);
		moRep.setChildDevices(null);
		moRep.setDeviceParents(null);
		
	}
}

package com.softwareag.signalmigration.service;

import org.apache.logging.log4j.util.Strings;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.cumulocity.model.authentication.CumulocityBasicCredentials;
import com.cumulocity.sdk.client.Platform;
import com.cumulocity.sdk.client.PlatformImpl;

@Component
public class PlatformUtil {
	public static String LOCAL_PLATFORM_URL= "http://cumulocity:8111";
	
	@Value("${C8Y.user:}")
	private String c8yUser;
	@Value("${C8Y.password:}")
	private String c8yPassword;
	@Value("${C8Y.tenant:}")
	private String c8yTenant;

	public Platform resolvePlatform(String platformHost, String platformLoginString) {
		CumulocityBasicCredentials credentials;
		// if platfrom host is LOCAL_PLATFORM_URL, use the env variables C8Y_TENANT, C8Y_USER, C8Y_PASSWORD for authentication 
		if (platformHost.equals(LOCAL_PLATFORM_URL)) {
			if (!Strings.isBlank(platformLoginString)) {
				throw new IllegalArgumentException("When platfrom host is LOCAL_PLATFORM_URL, platformLoginString must be null; LOCAL_PLATFORM_URL: " + LOCAL_PLATFORM_URL);
			}
			if (Strings.isBlank(c8yTenant) 
					|| Strings.isBlank(c8yUser)
					|| Strings.isBlank(c8yPassword)) {
				throw new IllegalArgumentException("When platfrom host is LOCAL_PLATFORM_URL, all of these must be defined : C8Y_TENANT, C8Y_USER, C8Y_PASSWORD");
			}			
			platformLoginString = c8yTenant + '/' + c8yUser + ":" + c8yPassword;
			credentials = CumulocityBasicCredentials.from(platformLoginString);
		} else {
			credentials = CumulocityBasicCredentials.from(platformLoginString);			
		}
		
		return new PlatformImpl(platformHost, credentials);
	}
	
	
}

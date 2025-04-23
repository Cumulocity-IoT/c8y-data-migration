package com.softwareag.signalmigration.config;

import java.time.Duration;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;

@Configuration
public class RetryConfiguration {
	
	@Value("${RetryConfiguration.defaultExternalAPIRetryMaxAttempts:4}")
	private int defaultExternalAPIRetryMaxAttempts;
	
	@Value("${RetryConfiguration.defaultExternalAPIRetryWaitSeconds:40}")
	private long defaultExternalAPIRetryWaitSeconds;
	

	@Bean(name = "defaultExternalAPIRetry")
    RetryRegistry defaultExternalAPIRetry() {
		RetryConfig config = RetryConfig.custom()
				  .maxAttempts(defaultExternalAPIRetryMaxAttempts)
				  .waitDuration(Duration.ofSeconds(defaultExternalAPIRetryWaitSeconds))
				  .build();
		
		RetryRegistry registry = RetryRegistry.of(config);
		
        return registry;
    }
	
	
}

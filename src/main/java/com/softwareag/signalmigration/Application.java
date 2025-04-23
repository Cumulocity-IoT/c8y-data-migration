package com.softwareag.signalmigration;

import org.springframework.boot.SpringApplication;

import com.cumulocity.microservice.autoconfigure.MicroserviceApplication;
import com.cumulocity.microservice.context.annotation.EnableContextSupport;


@MicroserviceApplication
@EnableContextSupport
public class Application {

	public static void main(String[] args) {
		SpringApplication.run(Application.class, args);
	}

}

package com.suhoi.mexcdrainer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.annotation.Bean;
import org.springframework.web.client.RestTemplate;

@SpringBootApplication
@ConfigurationPropertiesScan
public class MexcDrainerApplication {

    public static void main(String[] args) {
        SpringApplication.run(MexcDrainerApplication.class, args);
    }
    }

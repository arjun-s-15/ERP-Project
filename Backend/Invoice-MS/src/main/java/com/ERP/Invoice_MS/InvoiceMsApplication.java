package com.ERP.Invoice_MS;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;

@EnableAsync
@SpringBootApplication
public class InvoiceMsApplication {

	public static void main(String[] args) {
		SpringApplication.run(InvoiceMsApplication.class, args);
	}

}

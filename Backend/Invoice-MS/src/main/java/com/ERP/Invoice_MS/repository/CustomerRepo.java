package com.ERP.Invoice_MS.repository;

import com.ERP.Invoice_MS.entity.CustomerEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

public interface CustomerRepo extends JpaRepository<CustomerEntity, UUID> {
}

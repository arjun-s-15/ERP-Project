package com.ERP.Invoice_MS.repository;

import com.ERP.Invoice_MS.entity.InvoiceEntity;
import com.ERP.Invoice_MS.enums.InvoiceStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import software.amazon.awssdk.services.s3.endpoints.internal.Value;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface InvoiceRepo extends JpaRepository<InvoiceEntity, UUID> {
    Optional<InvoiceEntity> findByInvoiceNumber(String invoiceNumber);
    List<InvoiceEntity> findByCustomerId(UUID customerId);
    List<InvoiceEntity> findByStatus(InvoiceStatus status);
}

package com.ERP.Invoice_MS.repository;

import com.ERP.Invoice_MS.entity.InvoiceEntity;
import com.ERP.Invoice_MS.enums.InvoiceStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import software.amazon.awssdk.services.s3.endpoints.internal.Value;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface InvoiceRepo extends JpaRepository<InvoiceEntity, UUID> {
    Optional<InvoiceEntity> findByInvoiceNumber(String invoiceNumber);
    List<InvoiceEntity> findByCustomerId(UUID customerId);
    List<InvoiceEntity> findByStatus(InvoiceStatus status);
    Optional<Object> findById(Long id);
    @Query("SELECT i FROM InvoiceEntity i LEFT JOIN FETCH i.items WHERE i.id = :id")
    Optional<InvoiceEntity> findByIdWithItems(@Param("id") UUID id);
}

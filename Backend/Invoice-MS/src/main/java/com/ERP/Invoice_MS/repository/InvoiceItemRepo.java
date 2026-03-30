package com.ERP.Invoice_MS.repository;

import com.ERP.Invoice_MS.entity.InvoiceItemEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface InvoiceItemRepo extends JpaRepository<InvoiceItemEntity, UUID> {
    List<InvoiceItemEntity> findByInvoiceId(UUID invoiceId);
}

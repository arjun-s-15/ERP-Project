package com.ERP.Invoice_MS.repository;

import com.ERP.Invoice_MS.entity.PaymentEntity;
import com.ERP.Invoice_MS.enums.PaymentStatus;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface PaymentRepo extends JpaRepository<PaymentEntity, UUID> {
    List<PaymentEntity> findByInvoiceId(UUID invoiceId);
    List<PaymentEntity> findByStatus(PaymentStatus status);

}

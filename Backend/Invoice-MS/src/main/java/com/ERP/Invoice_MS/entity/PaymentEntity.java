package com.ERP.Invoice_MS.entity;

import com.ERP.Invoice_MS.enums.PaymentStatus;
import jakarta.persistence.*;
import lombok.*;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;

@Entity
@Table(name = "payments")
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class PaymentEntity {

    @Id
    @GeneratedValue
    private UUID id;

    @ManyToOne
    @JoinColumn(name = "invoice_id")
    private InvoiceEntity invoice;

    private BigDecimal amount;

    private LocalDateTime paymentDate = LocalDateTime.now();

    private String method; // UPI, CARD

    private String transactionId;

    @Enumerated(EnumType.STRING)
    private PaymentStatus status;
}
package com.ERP.Invoice_MS.entity;

import com.ERP.Invoice_MS.enums.InvoiceStatus;
import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.CreationTimestamp;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

@Entity
@Table(name = "invoices")
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class InvoiceEntity {

    @Id
    @GeneratedValue
    private UUID id;

    @Column(unique = true, nullable = false)
    private String invoiceNumber;

    @ManyToOne
    @JoinColumn(name = "customer_id")
    private CustomerEntity customer = new CustomerEntity();

    private LocalDate issueDate;
    private LocalDate dueDate;

    private BigDecimal subtotal;
    private BigDecimal taxAmount;
    private BigDecimal discount;
    private BigDecimal totalAmount;

    @Enumerated(EnumType.STRING)
    private InvoiceStatus status;

    private String currency = "INR";

    @Column(columnDefinition = "TEXT")
    private String notes;
    @CreationTimestamp
    private LocalDateTime createdAt ;

    @OneToMany(mappedBy = "invoice", cascade = CascadeType.ALL, orphanRemoval = true)
    private List<InvoiceItemEntity> items;
}
package com.ERP.Invoice_MS.entity;

import com.fasterxml.jackson.annotation.JsonIgnore;
import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;


import java.math.BigDecimal;
import java.util.UUID;

@Entity
@Table(name = "invoice_items")
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class InvoiceItemEntity{

    @Id
    @GeneratedValue
    private UUID id;

    @ManyToOne
    @JoinColumn(name = "invoice_id")
    @JsonIgnore
    private InvoiceEntity invoice;

    private String itemName;

    @Column(columnDefinition = "TEXT")
    private String description;

    private Integer quantity;

    private BigDecimal unitPrice;
    private BigDecimal totalPrice;

    private BigDecimal taxRate;
}
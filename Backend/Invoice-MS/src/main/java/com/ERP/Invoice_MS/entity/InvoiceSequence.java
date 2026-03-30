package com.ERP.Invoice_MS.entity;

import jakarta.persistence.*;
import lombok.*;

@Entity
@Table(name = "invoice_sequence")
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class InvoiceSequence {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private int year;

    private int lastNumber;
}
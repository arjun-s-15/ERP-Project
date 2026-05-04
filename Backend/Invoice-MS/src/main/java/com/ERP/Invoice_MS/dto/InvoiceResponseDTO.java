package com.ERP.Invoice_MS.dto;

import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.UUID;

@Data
@Builder
public class InvoiceResponseDTO {

    private UUID id;
    private String invoiceNumber;
    private String customerName;
    private LocalDate issueDate;
    private BigDecimal totalAmount;
    private String status;
}
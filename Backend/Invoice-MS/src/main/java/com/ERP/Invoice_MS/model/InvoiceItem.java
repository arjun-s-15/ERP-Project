package com.ERP.Invoice_MS.model;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;
@Data
@AllArgsConstructor
public class InvoiceItem {
    private String name;
    private int quantity;
    private double price;




}

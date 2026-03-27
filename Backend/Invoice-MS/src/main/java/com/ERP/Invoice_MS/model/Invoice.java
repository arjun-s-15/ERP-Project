package com.ERP.Invoice_MS.model;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data

public class Invoice {
//Invoice	StockCode	Description	Quantity	InvoiceDate	Price	Customer ID	Country
    private String invoiceNumber;
    private String customerName;
    private String product;
    private double amount;
    private double cgst;
    private double sgst;
    private double total;
    private List<InvoiceItem> items = new ArrayList<>();

}

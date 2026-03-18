package com.ERP.Invoice_MS.model;

import lombok.Data;

@Data

public class Invoice {
//Invoice	StockCode	Description	Quantity	InvoiceDate	Price	Customer ID	Country
    private String invoiceNumber;
    private String customerName;
    private String productId;
    private double amount;
    private double cgst;
    private double sgst;
    private double total;


}

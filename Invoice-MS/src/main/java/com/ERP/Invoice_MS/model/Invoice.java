package com.ERP.Invoice_MS.model;

import lombok.Data;

@Data

public class Invoice {
//Invoice	StockCode	Description	Quantity	InvoiceDate	Price	Customer ID	Country
    private String invoiceNumber;
    private String customerName;


}

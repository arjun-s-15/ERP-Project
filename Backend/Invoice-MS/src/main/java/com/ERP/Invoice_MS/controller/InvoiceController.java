package com.ERP.Invoice_MS.controller;

import com.ERP.Invoice_MS.model.Invoice;
import com.ERP.Invoice_MS.model.InvoiceItem;
import com.ERP.Invoice_MS.service.InvoiceService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;

@RestController
@RequestMapping("/invoice")
public class InvoiceController {

    @Autowired
    private InvoiceService invoiceService;

    @GetMapping("/download")
    public ResponseEntity<byte[]> downloadInvoice() throws Exception {

        Invoice invoice = new Invoice();
        invoice.setInvoiceNumber("INV-1001");
        invoice.setCustomerName("Arjun Singh");
        invoice.setProduct("Wallet Topup");
        invoice.setAmount(1000);
        invoice.setCgst(90);
        invoice.setSgst(90);
        invoice.setTotal(1180);

        List<InvoiceItem> items = new ArrayList<>();
        items.add(new InvoiceItem("Wallet Topup", 1, 1000));

        invoice.setItems(items);

        byte[] pdf = invoiceService.generateInvoicePdf(invoice);

        return ResponseEntity.ok()
                .header(HttpHeaders.CONTENT_DISPOSITION,
                        "attachment; filename=invoice.pdf")
                .contentType(MediaType.APPLICATION_PDF)
                .body(pdf);
    }
}

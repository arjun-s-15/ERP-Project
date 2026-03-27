package com.ERP.Invoice_MS.controller;

import com.ERP.Invoice_MS.model.Invoice;
import com.ERP.Invoice_MS.service.InvoiceService;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
@RequestMapping("/invoice")
public class InvoiceViewController {

    private final InvoiceService invoiceService;

    public InvoiceViewController(InvoiceService invoiceService) {
        this.invoiceService = invoiceService;
    }

    @GetMapping("/form")
    public String showForm(Model model) {
        model.addAttribute("invoice", new Invoice());
        return "invoice-form";
    }

    @PostMapping("/generate")
    public ResponseEntity<byte[]> generateInvoice(@ModelAttribute Invoice invoice) throws Exception {

        // Step 1: Generate PDF bytes
        byte[] pdf = invoiceService.generateInvoicePdf(invoice);

        // Step 2: Upload to S3 and get presigned URL (logged in console)
        String presignedUrl = invoiceService.generateInvoiceAndUpload(invoice);
        System.out.println("Invoice uploaded. Presigned URL: " + presignedUrl);

        // Step 3: Return PDF as download
        String invoiceNumber = invoice.getInvoiceNumber() != null
                ? invoice.getInvoiceNumber()
                : "draft";

        return ResponseEntity.ok()
                .header(HttpHeaders.CONTENT_DISPOSITION,
                        "attachment; filename=invoice-" + invoiceNumber + ".pdf")
                .contentType(MediaType.APPLICATION_PDF)
                .body(pdf);
    }
}
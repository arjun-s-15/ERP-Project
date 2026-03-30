package com.ERP.Invoice_MS.controller;

import com.ERP.Invoice_MS.entity.InvoiceEntity;
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
        model.addAttribute("invoice", new InvoiceEntity());
        return "invoice-form";
    }

    @PostMapping("/generate")
    public ResponseEntity<byte[]> generateInvoice(@ModelAttribute InvoiceEntity invoiceEntity) throws Exception {

        // Step 1: Save to DB, calculate totals, upload to S3 (all handled in service)
        String presignedUrl = invoiceService.createInvoiceAndUpload(invoiceEntity);
        System.out.println("Invoice uploaded. Presigned URL: " + presignedUrl);

        // Step 2: Generate PDF from the fully populated entity (number + totals already set)
        byte[] pdf = invoiceService.generateInvoicePdf(invoiceEntity);

        // Step 3: Return PDF as download
        String invoiceNumber = invoiceEntity.getInvoiceNumber() != null
                ? invoiceEntity.getInvoiceNumber()
                : "draft";

        return ResponseEntity.ok()
                .header(HttpHeaders.CONTENT_DISPOSITION,
                        "attachment; filename=invoice-" + invoiceNumber + ".pdf")
                .contentType(MediaType.APPLICATION_PDF)
                .body(pdf);
    }
}
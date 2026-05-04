package com.ERP.Invoice_MS.controller;

import com.ERP.Invoice_MS.entity.InvoiceEntity;
import com.ERP.Invoice_MS.enums.InvoiceStatus;
import com.ERP.Invoice_MS.service.InvoiceService;
import com.ERP.Invoice_MS.service.S3UrlService;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/invoice")
public class InvoiceController {
    private final S3UrlService s3UrlService;
    private final InvoiceService invoiceService;

    public InvoiceController(S3UrlService s3UrlService, InvoiceService invoiceService) {
        this.s3UrlService = s3UrlService;
        this.invoiceService = invoiceService;
    }
    @PatchMapping("/{id}/status")
    public InvoiceEntity updateInvoiceStatus(
            @PathVariable UUID id,
            @RequestParam InvoiceStatus status
    ) {
        return invoiceService.updateStatus(id, status);
    }
    @GetMapping
    public List<Map<String, Object>> getAllInvoices() {
        return invoiceService.getAllInvoices();
    }
    @GetMapping("/{id}")
    public InvoiceEntity getInvoice(@PathVariable UUID id) {
        return invoiceService.getInvoiceById(id);
    }
    @GetMapping("/{id}/pdf")
    public ResponseEntity<byte[]> downloadInvoice(@PathVariable UUID id) throws Exception {

        InvoiceEntity invoice = invoiceService.getInvoiceById(id);
        byte[] pdf = invoiceService.generateInvoicePdf(invoice);

        return ResponseEntity.ok()
                .header(HttpHeaders.CONTENT_DISPOSITION,
                        "attachment; filename=invoice-" + invoice.getInvoiceNumber() + ".pdf")
                .contentType(MediaType.APPLICATION_PDF)
                .body(pdf);
    }

    @PostMapping("/generate")
    public ResponseEntity<byte[]> generateInvoice(@RequestBody InvoiceEntity invoiceEntity) throws Exception {

        // Step 1: Business logic
        String presignedUrl = invoiceService.createInvoiceAndUpload(invoiceEntity);
        System.out.println("Invoice uploaded: " + presignedUrl);

        // Step 2: Generate PDF
        byte[] pdf = invoiceService.generateInvoicePdf(invoiceEntity);

        String invoiceNumber = invoiceEntity.getInvoiceNumber() != null
                ? invoiceEntity.getInvoiceNumber()
                : "draft";

        // Step 3: Send PDF to React
        return ResponseEntity.ok()
                .header(HttpHeaders.CONTENT_DISPOSITION,
                        "attachment; filename=invoice-" + invoiceNumber + ".pdf")
                .contentType(MediaType.APPLICATION_PDF)
                .body(pdf);
    }
    @DeleteMapping("/{id}")
    public ResponseEntity<Void> deleteInvoice(@PathVariable UUID id) {
        invoiceService.deleteInvoice(id);
        return ResponseEntity.noContent().build(); // 204 No Content
    }
    @GetMapping("/{id}/url")
    public ResponseEntity<Map<String, String>> getInvoiceUrl(@PathVariable UUID id) {
        InvoiceEntity invoice = invoiceService.getInvoiceById(id);
        String key = "invoices/" + invoice.getInvoiceNumber() + ".pdf";
        String url = s3UrlService.generatePresignedUrl(key);
        return ResponseEntity.ok(Map.of("url", url));
    }
}
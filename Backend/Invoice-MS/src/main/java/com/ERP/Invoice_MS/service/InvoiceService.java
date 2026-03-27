package com.ERP.Invoice_MS.service;

import com.ERP.Invoice_MS.model.Invoice;
import com.openhtmltopdf.pdfboxout.PdfRendererBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.thymeleaf.TemplateEngine;
import org.thymeleaf.context.Context;

import java.io.ByteArrayOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

@Service
public class InvoiceService {

    private final TemplateEngine templateEngine;
    private final S3UploadService s3UploadService;
    private final S3UrlService s3UrlService;

    public InvoiceService(TemplateEngine templateEngine,
                          S3UploadService s3UploadService,
                          S3UrlService s3UrlService) {
        this.templateEngine = templateEngine;
        this.s3UploadService = s3UploadService;
        this.s3UrlService = s3UrlService;
    }
    public String generateInvoiceAndUpload(Invoice invoice) throws Exception {

        byte[] pdfBytes = generateInvoicePdf(invoice);

        String invoiceNumber = generateInvoiceNumber();

        String key = "invoices/" + invoiceNumber + ".pdf";

        Path tempFile = Files.createTempFile("invoice-", ".pdf");
        try {
            Files.write(tempFile, pdfBytes);
            s3UploadService.uploadFile(key, tempFile);
        } finally {
            Files.deleteIfExists(tempFile);
        }

        return s3UrlService.generatePresignedUrl(key);
    }

    public byte[] generateInvoicePdf(Invoice invoice) throws Exception {

        double subtotal = 0;

        if (invoice.getItems() != null) {
            subtotal = invoice.getItems().stream()
                    .mapToDouble(i -> i.getPrice() * i.getQuantity())
                    .sum();
        }

        double total = subtotal + invoice.getCgst() + invoice.getSgst();
        invoice.setTotal(total);

        Context context = new Context();
        context.setVariable("invoice", invoice);

        String html = templateEngine.process("invoice", context);

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        PdfRendererBuilder builder = new PdfRendererBuilder();

        builder.withHtmlContent(html, null);
        builder.toStream(outputStream);
        builder.run();

        return outputStream.toByteArray();
    }

    private String generateInvoiceNumber() {

        String date = LocalDate.now()
                .format(DateTimeFormatter.ofPattern("yyyyMM"));

        String unique = UUID.randomUUID().toString().substring(0, 8).toUpperCase();

        return "INV-" + date + "-" + unique;
    }
}
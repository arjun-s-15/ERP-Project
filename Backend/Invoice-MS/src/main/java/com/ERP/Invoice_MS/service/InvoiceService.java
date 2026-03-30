package com.ERP.Invoice_MS.service;

import com.ERP.Invoice_MS.entity.CustomerEntity;
import com.ERP.Invoice_MS.entity.InvoiceEntity;
import com.ERP.Invoice_MS.entity.InvoiceItemEntity;
import com.ERP.Invoice_MS.repository.CustomerRepo;
import com.ERP.Invoice_MS.repository.InvoiceRepo;
import com.openhtmltopdf.pdfboxout.PdfRendererBuilder;
import org.springframework.stereotype.Service;
import org.thymeleaf.TemplateEngine;
import org.thymeleaf.context.Context;

import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

@Service
public class InvoiceService {

    private final CustomerRepo customerRepo;
    private final TemplateEngine templateEngine;
    private final S3UploadService s3UploadService;
    private final S3UrlService s3UrlService;
    private final InvoiceRepo invoiceRepository;
    private final InvoiceNumberGenerator invoiceNumberGenerator;

    public InvoiceService(CustomerRepo customerRepo,
                          TemplateEngine templateEngine,
                          S3UploadService s3UploadService,
                          S3UrlService s3UrlService,
                          InvoiceRepo invoiceRepository,
                          InvoiceNumberGenerator invoiceNumberGenerator) {
        this.customerRepo = customerRepo;
        this.templateEngine = templateEngine;
        this.s3UploadService = s3UploadService;
        this.s3UrlService = s3UrlService;
        this.invoiceRepository = invoiceRepository;
        this.invoiceNumberGenerator = invoiceNumberGenerator;
    }

    public String createInvoiceAndUpload(InvoiceEntity invoiceEntity) throws Exception {

        // Debug — remove after confirming binding works
        System.out.println("Customer received: " + invoiceEntity.getCustomer());
        if (invoiceEntity.getCustomer() != null) {
            System.out.println("Customer name: " + invoiceEntity.getCustomer().getName());
        }

        // 1. Save customer first so invoice FK is satisfied
        CustomerEntity customer = invoiceEntity.getCustomer();
        if (customer != null && hasAnyValue(customer)) {
            CustomerEntity savedCustomer = customerRepo.save(customer);
            invoiceEntity.setCustomer(savedCustomer);
        } else {
            invoiceEntity.setCustomer(null);
        }

        // 2. Generate invoice number
        String invoiceNumber = invoiceNumberGenerator.generateInvoiceNumber();
        invoiceEntity.setInvoiceNumber(invoiceNumber);

        // 3. Calculate totals
        calculateTotals(invoiceEntity);

        // 4. Save invoice to DB
        InvoiceEntity savedInvoice = invoiceRepository.save(invoiceEntity);

        // 5. Generate PDF
        byte[] pdfBytes = generateInvoicePdf(savedInvoice);

        // 6. Upload to S3
        String key = "invoices/" + invoiceNumber + ".pdf";
        Path tempFile = Files.createTempFile("invoice-", ".pdf");
        try {
            Files.write(tempFile, pdfBytes);
            s3UploadService.uploadFile(key, tempFile);
        } finally {
            Files.deleteIfExists(tempFile);
        }

        // 7. Return presigned URL
        return s3UrlService.generatePresignedUrl(key);
    }

    // Returns true if at least one customer field has a value
    private boolean hasAnyValue(CustomerEntity customer) {
        return (customer.getName()    != null && !customer.getName().isBlank())
                || (customer.getEmail()   != null && !customer.getEmail().isBlank())
                || (customer.getPhone()   != null && !customer.getPhone().isBlank())
                || (customer.getGstin()   != null && !customer.getGstin().isBlank())
                || (customer.getAddress() != null && !customer.getAddress().isBlank());
    }

    private void calculateTotals(InvoiceEntity invoice) {
        List<InvoiceItemEntity> items = invoice.getItems();

        BigDecimal subtotal = BigDecimal.ZERO;
        BigDecimal taxAmount = BigDecimal.ZERO;

        if (items != null) {
            for (InvoiceItemEntity item : items) {
                BigDecimal itemTotal = item.getUnitPrice()
                        .multiply(BigDecimal.valueOf(item.getQuantity()));
                item.setTotalPrice(itemTotal);

                BigDecimal itemTax = BigDecimal.ZERO;
                if (item.getTaxRate() != null) {
                    itemTax = itemTotal.multiply(item.getTaxRate())
                            .divide(BigDecimal.valueOf(100), 2, RoundingMode.HALF_UP);
                }

                subtotal = subtotal.add(itemTotal);
                taxAmount = taxAmount.add(itemTax);
            }
        }

        BigDecimal discount = invoice.getDiscount() != null
                ? invoice.getDiscount()
                : BigDecimal.ZERO;

        BigDecimal totalAmount = subtotal.add(taxAmount).subtract(discount);

        invoice.setSubtotal(subtotal);
        invoice.setTaxAmount(taxAmount);
        invoice.setTotalAmount(totalAmount);
    }

    public byte[] generateInvoicePdf(InvoiceEntity invoiceEntity) throws Exception {
        Context context = new Context();
        context.setVariable("invoice", invoiceEntity);

        String html = templateEngine.process("invoice", context);

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        PdfRendererBuilder builder = new PdfRendererBuilder();
        builder.withHtmlContent(html, null);
        builder.toStream(outputStream);
        builder.run();

        return outputStream.toByteArray();
    }
}
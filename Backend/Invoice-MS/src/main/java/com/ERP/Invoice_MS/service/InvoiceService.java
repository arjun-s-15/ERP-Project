package com.ERP.Invoice_MS.service;

import com.ERP.Invoice_MS.entity.CustomerEntity;
import com.ERP.Invoice_MS.entity.InvoiceEntity;
import com.ERP.Invoice_MS.entity.InvoiceItemEntity;
import com.ERP.Invoice_MS.enums.InvoiceStatus;
import com.ERP.Invoice_MS.repository.CustomerRepo;
import com.ERP.Invoice_MS.repository.InvoiceRepo;
import com.ERP.Invoice_MS.util.InvoiceStatusValidator;
import com.openhtmltopdf.pdfboxout.PdfRendererBuilder;
import org.springframework.stereotype.Service;
import org.thymeleaf.TemplateEngine;
import org.thymeleaf.context.Context;

import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Service
public class InvoiceService {

    private final CustomerRepo customerRepo;
    private final TemplateEngine templateEngine;
    private final S3UploadService s3UploadService;
    private final S3UrlService s3UrlService;
    private final InvoiceRepo invoiceRepository;
    private final InvoiceNumberGenerator invoiceNumberGenerator;
    private final EmailService emailService;

    public InvoiceService(CustomerRepo customerRepo,
                          TemplateEngine templateEngine,
                          S3UploadService s3UploadService,
                          S3UrlService s3UrlService,
                          InvoiceRepo invoiceRepository,
                          InvoiceNumberGenerator invoiceNumberGenerator,
                          EmailService emailService) {
        this.customerRepo = customerRepo;
        this.templateEngine = templateEngine;
        this.s3UploadService = s3UploadService;
        this.s3UrlService = s3UrlService;
        this.invoiceRepository = invoiceRepository;
        this.invoiceNumberGenerator = invoiceNumberGenerator;
        this.emailService = emailService;
    }
    private String resolveStatus(InvoiceEntity inv) {
        if (inv.getStatus() == null) return "DRAFT";

        if (inv.getStatus() == InvoiceStatus.SENT &&
                inv.getDueDate() != null &&
                inv.getDueDate().isBefore(LocalDate.now())) {

            return "OVERDUE"; // 🔥 computed, not stored
        }

        return inv.getStatus().name();
    }
    public String createInvoiceAndUpload(InvoiceEntity invoiceEntity) throws Exception {
        if (invoiceEntity.getItems() != null) {
            for (InvoiceItemEntity item : invoiceEntity.getItems()) {
                item.setInvoice(invoiceEntity);
            }
        }
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

        if (invoiceEntity.getStatus() == null){
            invoiceEntity.setStatus(InvoiceStatus.DRAFT);
        }

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
        String url =  s3UrlService.generatePresignedUrl(key);

        //8. Send Email
        if (invoiceEntity.getCustomer() != null
                && invoiceEntity.getCustomer().getEmail() != null) {

            emailService.sendInvoiceEmail(
                    invoiceEntity.getCustomer().getEmail(),
                    invoiceNumber,
                    url
            );
        }

        return url;
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
        builder.withHtmlContent(html, "file:/");
        builder.toStream(outputStream);
        builder.run();

        return outputStream.toByteArray();
    }
    public List<Map<String, Object>> getAllInvoices() {
        return invoiceRepository.findAll().stream()
                .map(inv -> {
                    Map<String, Object> res = new HashMap<>();

                    res.put("id", inv.getId());
                    res.put("invoiceNumber", inv.getInvoiceNumber());
                    res.put("customer", inv.getCustomer() != null ? inv.getCustomer().getName() : "N/A");
                    res.put("issueDate", inv.getIssueDate());
                    res.put("totalAmount", inv.getTotalAmount());

                    // ✅ USE DERIVED STATUS
                    res.put("status", resolveStatus(inv));

                    return res;
                })
                .toList();
    }
    public InvoiceEntity getInvoiceById(UUID id) {
        return (InvoiceEntity) invoiceRepository.findById(id)
                .orElseThrow(() -> new RuntimeException("Invoice not found"));
    }

    public InvoiceEntity updateStatus(UUID id, InvoiceStatus newStatus) {
        InvoiceEntity invoice = invoiceRepository.findById(id)
                .orElseThrow(() -> new RuntimeException("Invoice not found"));

        InvoiceStatus current = invoice.getStatus();

        if (!InvoiceStatusValidator.canTransition(current, newStatus)) {
            throw new RuntimeException(
                    "Invalid status transition: " + current + " → " + newStatus
            );
        }

        invoice.setStatus(newStatus);

        // ✅ SAVE FIRST
        InvoiceEntity updatedInvoice = invoiceRepository.save(invoice);

        // ✅ ADD EMAIL LOGIC HERE (ONLY WHEN SENT)
        if (newStatus == InvoiceStatus.SENT &&
                invoice.getCustomer() != null &&
                invoice.getCustomer().getEmail() != null) {

            String key = "invoices/" + invoice.getInvoiceNumber() + ".pdf";

            String url = s3UrlService.generatePresignedUrl(key);

            emailService.sendInvoiceEmail(
                    invoice.getCustomer().getEmail(),
                    invoice.getInvoiceNumber(),
                    url
            );
        }

        return updatedInvoice;
    }

    public void deleteInvoice(UUID id) {
        InvoiceEntity invoice = invoiceRepository.findById(id)
                .orElseThrow(() -> new RuntimeException("Invoice not found"));

        // Optional: delete from S3 too
        String key = "invoices/" + invoice.getInvoiceNumber() + ".pdf";
        invoiceRepository.deleteById(id);
    }

}
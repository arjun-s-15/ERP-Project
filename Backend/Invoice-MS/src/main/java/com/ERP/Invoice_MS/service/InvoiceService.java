package com.ERP.Invoice_MS.service;

import com.ERP.Invoice_MS.model.Invoice;
import com.openhtmltopdf.pdfboxout.PdfRendererBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.thymeleaf.TemplateEngine;
import org.thymeleaf.context.Context;

import java.io.ByteArrayOutputStream;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Random;

@Service
public class InvoiceService {

    @Autowired
    private TemplateEngine templateEngine;

    public byte[] generateInvoicePdf(Invoice invoice) throws Exception {

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
    public String generateInvoiceNumber() {

        String prefix = "INV";
        String date = LocalDate.now()
                .format(DateTimeFormatter.ofPattern("yyyyMM"));

        int sequence = new Random().nextInt(9999);

        return prefix + "-" + date + "-" + String.format("%04d", sequence);
    }
}
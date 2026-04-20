package com.ERP.Invoice_MS.service;

import jakarta.mail.internet.MimeMessage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

@Service
public class EmailService {

    @Autowired
    private JavaMailSender mailSender;

    @Async
    public void sendInvoiceEmail(String to, String invoiceNumber, String url) {
        try {
            MimeMessage message = mailSender.createMimeMessage();
            MimeMessageHelper helper = new MimeMessageHelper(message, true);

            helper.setTo(to);
            helper.setSubject("Invoice #" + invoiceNumber);

            String content = """
                    Dear Customer,

                    Your invoice has been generated successfully.

                    Invoice Number: %s

                    Download your invoice here:
                    %s

                    Thanks,
                    ERP Team
                    """.formatted(invoiceNumber, url);

            helper.setText(content);

            mailSender.send(message);

        } catch (Exception e) {
            System.out.println("Email failed: " + e.getMessage());
        }
    }
}

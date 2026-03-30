package com.ERP.Invoice_MS.service;

import com.ERP.Invoice_MS.repository.InvoiceSequenceRepo;
import com.ERP.Invoice_MS.entity.InvoiceSequence;
import com.ERP.Invoice_MS.repository.InvoiceSequenceRepo;
import jakarta.transaction.Transactional;
import org.springframework.stereotype.Service;

import java.time.Year;

@Service
public class InvoiceNumberGenerator {

    private final InvoiceSequenceRepo repository;

    public InvoiceNumberGenerator(InvoiceSequenceRepo repository) {
        this.repository = repository;
    }

    @Transactional
    public synchronized String generateInvoiceNumber() {

        int currentYear = Year.now().getValue();

        InvoiceSequence sequence = repository.findByYear(currentYear)
                .orElseGet(() -> {
                    InvoiceSequence newSeq = new InvoiceSequence();
                    newSeq.setYear(currentYear);
                    newSeq.setLastNumber(0);
                    return repository.save(newSeq);
                });

        int nextNumber = sequence.getLastNumber() + 1;
        sequence.setLastNumber(nextNumber);

        repository.save(sequence);

        return String.format("INV-%d-%05d", currentYear, nextNumber);
    }
}
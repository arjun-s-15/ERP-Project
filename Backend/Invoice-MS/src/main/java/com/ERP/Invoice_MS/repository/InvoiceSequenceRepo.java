package com.ERP.Invoice_MS.repository;

import com.ERP.Invoice_MS.entity.InvoiceSequence;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface InvoiceSequenceRepo extends JpaRepository<InvoiceSequence, Long> {

    Optional<InvoiceSequence> findByYear(int year);
}
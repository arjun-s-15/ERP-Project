package com.ERP.Invoice_MS.util;

import com.ERP.Invoice_MS.enums.InvoiceStatus;

import java.util.Map;
import java.util.Set;

public class InvoiceStatusValidator {

    private static final Map<InvoiceStatus, Set<InvoiceStatus>> allowedTransitions = Map.of(
            InvoiceStatus.DRAFT, Set.of(InvoiceStatus.SENT),

            // SENT → PAID OR back to DRAFT (optional but useful in real apps)
            InvoiceStatus.SENT, Set.of(InvoiceStatus.PAID, InvoiceStatus.DRAFT),

            InvoiceStatus.OVERDUE, Set.of(InvoiceStatus.PAID),

            // PAID is final
            InvoiceStatus.PAID, Set.of()
    );

    public static boolean canTransition(InvoiceStatus from, InvoiceStatus to) {

        // ✅ FIX 1: Handle null safely (your crash fix)
        if (from == null) {
            return to == InvoiceStatus.DRAFT;
        }

        // ❗ OVERDUE is computed, but still allow transition logic
        return allowedTransitions
                .getOrDefault(from, Set.of())
                .contains(to);
    }
}
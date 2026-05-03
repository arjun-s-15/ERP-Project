package org.ERP.Inventory.dto.response;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class InventoryResponse {
    private boolean success;
    private String  message;
    private Long    transactionId;
    private Integer availableQuantity;
    private Integer reservedQuantity;
}
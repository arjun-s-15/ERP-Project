package org.ERP.Inventory.dto.request;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Data;

@Data
public class DeductRequest {
    @NotNull  private Long productId;
    @NotNull  private Long warehouseId;
    @Min(1)   private int quantity;
    @NotBlank private String referenceId;
}
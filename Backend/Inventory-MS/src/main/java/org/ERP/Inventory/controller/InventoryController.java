package org.ERP.Inventory.controller;



import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.ERP.Inventory.dto.request.AddStockRequest;
import org.ERP.Inventory.dto.request.DeductRequest;
import org.ERP.Inventory.dto.request.ReleaseRequest;
import org.ERP.Inventory.dto.request.ReserveRequest;
import org.ERP.Inventory.dto.response.InventoryResponse;
import org.ERP.Inventory.service.InventoryService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/inventory")
@RequiredArgsConstructor
public class InventoryController {

    private final InventoryService inventoryService;

    // GET /inventory?productId=1&warehouseId=1
    @GetMapping
    public ResponseEntity<InventoryResponse> getStock(
            @RequestParam Long productId,
            @RequestParam Long warehouseId) {

        return ResponseEntity.ok(inventoryService.getStock(productId, warehouseId));
    }

    // POST /inventory/add
    @PostMapping("/add")
    public ResponseEntity<InventoryResponse> addStock(
            @RequestBody @Valid AddStockRequest request) {

        return ResponseEntity.status(HttpStatus.CREATED)
                .body(inventoryService.addStock(request));
    }

    // POST /inventory/reserve
    @PostMapping("/reserve")
    public ResponseEntity<InventoryResponse> reserve(
            @RequestBody @Valid ReserveRequest request) {

        return ResponseEntity.ok(inventoryService.reserve(request));
    }

    // POST /inventory/release
    @PostMapping("/release")
    public ResponseEntity<InventoryResponse> release(
            @RequestBody @Valid ReleaseRequest request) {

        return ResponseEntity.ok(inventoryService.release(request));
    }

    // POST /inventory/deduct
    @PostMapping("/deduct")
    public ResponseEntity<InventoryResponse> deduct(
            @RequestBody @Valid DeductRequest request) {

        return ResponseEntity.ok(inventoryService.deduct(request));
    }
}

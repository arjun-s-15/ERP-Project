package org.ERP.Inventory.service;

import lombok.RequiredArgsConstructor;
import org.ERP.Inventory.dto.request.AddStockRequest;
import org.ERP.Inventory.dto.request.DeductRequest;
import org.ERP.Inventory.dto.request.ReleaseRequest;
import org.ERP.Inventory.dto.request.ReserveRequest;
import org.ERP.Inventory.dto.response.InventoryResponse;
import org.ERP.Inventory.entity.Inventory;
import org.ERP.Inventory.entity.InventoryTransaction;
import org.ERP.Inventory.entity.InventoryTransaction.TransactionType;
import org.ERP.Inventory.exception.InsufficientStockException;
import org.ERP.Inventory.exception.ResourceNotFoundException;
import org.ERP.Inventory.repository.InventoryRepository;
import org.ERP.Inventory.repository.InventoryTransactionRepository;
import org.springframework.orm.ObjectOptimisticLockingFailureException;


import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
public class InventoryService {

    private final InventoryRepository inventoryRepo;
    private final InventoryTransactionRepository txnRepo;

    // ─── ADD ────────────────────────────────────────────────────────────────
    @Retryable(retryFor = ObjectOptimisticLockingFailureException.class,
            maxAttempts = 3, backoff = @Backoff(delay = 100))
    @Transactional
    public InventoryResponse addStock(AddStockRequest req) {
        Inventory inv = findInventory(req.getProductId(), req.getWarehouseId());

        inv.setAvailableQuantity(inv.getAvailableQuantity() + req.getQuantity());
        inventoryRepo.save(inv);

        InventoryTransaction txn = logTransaction(
                inv, TransactionType.ADD, req.getQuantity(), req.getReferenceId());

        return buildResponse("Stock added successfully", inv, txn.getId());
    }

    // ─── RESERVE ────────────────────────────────────────────────────────────
    @Retryable(retryFor = ObjectOptimisticLockingFailureException.class,
            maxAttempts = 3, backoff = @Backoff(delay = 100))
    @Transactional
    public InventoryResponse reserve(ReserveRequest req) {
        Inventory inv = findInventory(req.getProductId(), req.getWarehouseId());

        if (inv.getAvailableQuantity() < req.getQuantity()) {
            throw new InsufficientStockException(
                    "Insufficient stock. Available: " + inv.getAvailableQuantity()
                            + ", Requested: " + req.getQuantity());
        }

        inv.setAvailableQuantity(inv.getAvailableQuantity() - req.getQuantity());
        inv.setReservedQuantity (inv.getReservedQuantity()  + req.getQuantity());
        inventoryRepo.save(inv);  // @Version handles optimistic locking

        InventoryTransaction txn = logTransaction(
                inv, TransactionType.RESERVE, req.getQuantity(), req.getReferenceId());

        return buildResponse("Stock reserved successfully", inv, txn.getId());
    }

    // ─── RELEASE ────────────────────────────────────────────────────────────
    @Retryable(retryFor = ObjectOptimisticLockingFailureException.class,
            maxAttempts = 3, backoff = @Backoff(delay = 100))
    @Transactional
    public InventoryResponse release(ReleaseRequest req) {
        Inventory inv = findInventory(req.getProductId(), req.getWarehouseId());

        if (inv.getReservedQuantity() < req.getQuantity()) {
            throw new InsufficientStockException(
                    "Cannot release more than reserved. Reserved: " + inv.getReservedQuantity());
        }

        inv.setReservedQuantity (inv.getReservedQuantity()  - req.getQuantity());
        inv.setAvailableQuantity(inv.getAvailableQuantity() + req.getQuantity());
        inventoryRepo.save(inv);

        InventoryTransaction txn = logTransaction(
                inv, TransactionType.RELEASE, req.getQuantity(), req.getReferenceId());

        return buildResponse("Stock released successfully", inv, txn.getId());
    }

    // ─── DEDUCT ─────────────────────────────────────────────────────────────
    @Retryable(retryFor = ObjectOptimisticLockingFailureException.class,
            maxAttempts = 3, backoff = @Backoff(delay = 100))
    @Transactional
    public InventoryResponse deduct(DeductRequest req) {
        Inventory inv = findInventory(req.getProductId(), req.getWarehouseId());

        if (inv.getReservedQuantity() < req.getQuantity()) {
            throw new InsufficientStockException(
                    "Cannot deduct more than reserved. Reserved: " + inv.getReservedQuantity());
        }

        inv.setReservedQuantity(inv.getReservedQuantity() - req.getQuantity());
        inventoryRepo.save(inv);

        InventoryTransaction txn = logTransaction(
                inv, TransactionType.DEDUCT, req.getQuantity(), req.getReferenceId());

        return buildResponse("Stock deducted successfully", inv, txn.getId());
    }

    // ─── GET ─────────────────────────────────────────────────────────────────
    @Transactional(readOnly = true)
    public InventoryResponse getStock(Long productId, Long warehouseId) {
        Inventory inv = findInventory(productId, warehouseId);
        return buildResponse("OK", inv, null);
    }

    // ─── Helpers ─────────────────────────────────────────────────────────────
    private Inventory findInventory(Long productId, Long warehouseId) {
        return inventoryRepo.findByProductIdAndWarehouseId(productId, warehouseId)
                .orElseThrow(() -> new ResourceNotFoundException(
                        "Inventory not found for productId=" + productId
                                + ", warehouseId=" + warehouseId));
    }

    private InventoryTransaction logTransaction(
            Inventory inv, TransactionType type, int qty, String referenceId) {

        return txnRepo.save(InventoryTransaction.builder()
                .productId(inv.getProductId())
                .warehouseId(inv.getWarehouseId())
                .type(type)
                .quantity(qty)
                .referenceId(referenceId)
                .build());
    }

    private InventoryResponse buildResponse(String message, Inventory inv, Long txnId) {
        return InventoryResponse.builder()
                .success(true)
                .message(message)
                .transactionId(txnId)
                .availableQuantity(inv.getAvailableQuantity())
                .reservedQuantity(inv.getReservedQuantity())
                .build();
    }
}
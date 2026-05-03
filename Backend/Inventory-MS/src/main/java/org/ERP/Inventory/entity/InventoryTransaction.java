package org.ERP.Inventory.entity;

import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.CreationTimestamp;
import java.time.LocalDateTime;

@Entity
@Table(
        name = "inventory_transaction",
        indexes = {
                @Index(name = "idx_txn_product_id",   columnList = "product_id"),
                @Index(name = "idx_txn_warehouse_id", columnList = "warehouse_id"),
                @Index(name = "idx_txn_reference_id", columnList = "reference_id"),
                @Index(name = "idx_txn_created_at",   columnList = "created_at")
        }
)
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class InventoryTransaction {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "product_id", nullable = false)
    private Long productId;

    @Column(name = "warehouse_id", nullable = false)
    private Long warehouseId;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 50)
    private TransactionType type;

    @Column(nullable = false)
    private Integer quantity;

    @Column(name = "reference_id", length = 100)
    private String referenceId;   // order_id, PO number etc.

    @CreationTimestamp
    @Column(name = "created_at", updatable = false)
    private LocalDateTime createdAt;

    public enum TransactionType {
        ADD, RESERVE, RELEASE, DEDUCT
    }
}
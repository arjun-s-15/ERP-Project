package org.ERP.Inventory.entity;

import jakarta.persistence.*;
import jakarta.validation.constraints.Min;
import lombok.*;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;
import java.time.LocalDateTime;

@Entity
@Table(
        name = "inventory",
        uniqueConstraints = {
                @UniqueConstraint(
                        name = "uq_product_warehouse",
                        columnNames = {"product_id", "warehouse_id"}
                )
        },
        indexes = {
                @Index(name = "idx_product_id",   columnList = "product_id"),
                @Index(name = "idx_warehouse_id", columnList = "warehouse_id")
        }
)
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Inventory {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "product_id", nullable = false)
    private Long productId;

    @Column(name = "warehouse_id", nullable = false)
    private Long warehouseId;

    @Min(0)
    @Column(nullable = false)
    private Integer availableQuantity;

    @Min(0)
    @Column(nullable = false)
    private Integer reservedQuantity;

    @Version
    private Integer version;  // JPA handles optimistic locking automatically

    @CreationTimestamp
    @Column(updatable = false)
    private LocalDateTime createdAt;

    @UpdateTimestamp
    private LocalDateTime updatedAt;
}
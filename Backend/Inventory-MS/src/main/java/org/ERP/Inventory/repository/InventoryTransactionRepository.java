package org.ERP.Inventory.repository;

import org.ERP.Inventory.entity.InventoryTransaction;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import java.util.List;

@Repository
public interface InventoryTransactionRepository extends JpaRepository<InventoryTransaction, Long> {

    List<InventoryTransaction> findByProductIdAndWarehouseId(Long productId, Long warehouseId);

    List<InventoryTransaction> findByReferenceId(String referenceId);
}
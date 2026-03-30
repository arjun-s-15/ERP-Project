package com.ERP.Invoice_MS.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Generated;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.CreationTimestamp;

import java.time.LocalDateTime;
import java.util.UUID;

@Entity
@Table(name = "customers")
@Data
@NoArgsConstructor
@AllArgsConstructor

public class CustomerEntity {
    @Id
    @GeneratedValue
    private UUID id;

    private String name;
    private String email;
    private String phone;

    @Column(columnDefinition = "TEXT")
    private String address;

    private String gstin;
    @CreationTimestamp
    private LocalDateTime createdAt ;

}

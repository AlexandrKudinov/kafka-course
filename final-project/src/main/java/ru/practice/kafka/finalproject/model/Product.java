package ru.practice.kafka.finalproject.model;

import lombok.Data;

import java.math.BigDecimal;
import java.util.List;

@Data
public class Product {
    private String productId;
    private String name;
    private String description;
    private Price price;
    private String category;
    private String brand;
    private Stock stock;
    private String sku;
    private List<String> tags;
    private String createdAt;
    private String updatedAt;
    private String index;
    private String storeId;

    @Data
    public static class Price {
        private BigDecimal amount;
        private String currency;
    }

    @Data
    public static class Stock {
        private int available;
        private int reserved;
    }
}

package com.suhoi.mexcdrainer.dto;

import lombok.Data;
import java.util.List;

@Data
public class Depth {
    private long lastUpdateId;
    private List<String[]> bids; // [price, qty]
    private List<String[]> asks;
}

package com.suhoi.mexcdrainer.dto;

import lombok.Data;
import java.util.List;

@Data
public class AccountInfo {
    @Data public static class Balance { private String asset; private String free; private String locked; }
    private long updateTime;
    private List<Balance> balances;
}

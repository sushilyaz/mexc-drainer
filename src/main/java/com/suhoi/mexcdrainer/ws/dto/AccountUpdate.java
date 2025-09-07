package com.suhoi.mexcdrainer.ws.dto;

import lombok.Builder;
import lombok.Value;

import java.math.BigDecimal;

/** Апдейт балансов (private.account). */
@Value
@Builder
public class AccountUpdate {
    String asset;             // vcoinName
    BigDecimal balance;       // balanceAmount
    BigDecimal frozen;        // frozenAmount
    long time;                // time из события
    long sendTime;            // sendTime
}

package com.suhoi.mexcdrainer.model;

import lombok.Data;

import java.math.BigDecimal;

@Data
public class DrainSession {

    public enum State {
        IDLE,
        A_MKT_BUY_DONE,
        A_SELL_PLACED,
        B_MKT_BUY_SENT,
        A_SELL_FILLED,
        A_BUY_PLACED,
        B_MKT_SELL_SENT,
        A_BUY_FILLED,
        AUTO_PAUSE
    }

    public enum AutoPauseReason {
        MANUAL,
        FRONT_RUN,             // вклинивание ММ
        TIMEOUT,               // таймаут ожидания FILLED
        PARTIAL_MISMATCH,      // факты/ожидания не сошлись
        SPREAD_TOO_THIN,       // спред слишком мал
        INSUFFICIENT_BALANCE,  // не хватает средств для шага
        UNKNOWN
    }

    private State state = State.IDLE;
    private AutoPauseReason reason = null;
    private String reasonDetails = null;

    private String symbol;
    private int cycleIndex;

    public BigDecimal qtyA;           // «рабочее» кол-во базового на A на входе цикла
    public String sellOrderId;
    public String buyOrderId;

    public BigDecimal pSell;          // выставленная цена SELL (нижняя кромка)
    public BigDecimal pBuy;           // выставленная цена BUY  (верхняя кромка)

    public BigDecimal lastSpentB;     // сколько USDT реально списали с B при MARKET BUY
    public BigDecimal lastCummA;      // сколько USDT реально пришло на A при SELL

    // счётчики «перестановок» против вклинивания
    public int requotesSell = 0;
    public int requotesBuy  = 0;

    // таймстемпы для диагностики
    public long tCreated = System.currentTimeMillis();
    public long tLastUpdate = System.currentTimeMillis();

    public void setState(State s) {
        this.state = s;
        this.tLastUpdate = System.currentTimeMillis();
    }

    public void autoPause(AutoPauseReason r, String details) {
        this.state = State.AUTO_PAUSE;
        this.reason = r;
        this.reasonDetails = details;
        this.tLastUpdate = System.currentTimeMillis();
    }
}


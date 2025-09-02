package com.suhoi.mexcdrainer.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.Instant;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RangeState {
    private Long chatId;

    private String symbol;                 // ANTUSDT
    private BigDecimal rangeLow;           // LOW
    private BigDecimal rangeHigh;          // HIGH

    private BigDecimal targetUsdt;         // цель перелива
    private BigDecimal drainedUsdt;        // уже «перелито»

    private int step;                      // кол-во завершённых шагов
    private boolean paused;                // ручная или авто-пауза
    private boolean running;               // есть активный цикл

    // фаза и служебки для resume/reconcile:
    private RangePhase phase;              // где «замерзли»: A_SELL_PLACED / A_BUY_PLACED / IDLE
    private String aSellOrderId;           // последняя лимитка A SELL
    private String aBuyOrderId;            // последняя лимитка A BUY
    private BigDecimal lastPlannedQty;     // плановая qty для текущей итерации

    private PauseReason pausedReason;      // причина паузы (если paused=true)
    private String pausedDetails;          // кратко, что случилось

    private Instant updatedAt;             // диагностика
}

package com.suhoi.mexcdrainer.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Простейшее, "слепое" состояние диапазонного перелива на чат.
 * НИКАКОГО reconciliation — только то, что нужно для /stop и /continue.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RangeState {
    private String symbol;                 // Монета (например, ANTUSDT)
    private BigDecimal rangeLow;           // Текущий нижний предел
    private BigDecimal rangeHigh;          // Текущий верхний предел
    private BigDecimal targetUsdt;         // Изначальная цель перелива в USDT
    private BigDecimal drainedUsdt;        // Сколько уже перелили (накопительно)
    private int step;                      // Сколько шагов сделали (для информации)
    private boolean paused;                // Пауза по /stop
    private boolean running;               // В процессе (рабочий поток жив)
    private Instant updatedAt;             // Метка обновления (просто для логов/диагностики)
}

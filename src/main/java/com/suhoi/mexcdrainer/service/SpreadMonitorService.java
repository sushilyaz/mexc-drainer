package com.suhoi.mexcdrainer.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.function.Consumer;

import static java.math.BigDecimal.ZERO;

@Slf4j
@Service
@RequiredArgsConstructor
public class SpreadMonitorService {

    private static final String API_BASE = "https://api.mexc.com";
    private static final String TICKER_BOOK = "/api/v3/ticker/bookTicker";
    // ставь true, чтобы видеть снимок спреда на каждом тике
    private static final boolean LOG_TICK_SPREAD = true;

    private static final int DEPTH_LIMIT = 20;

    private final RestTemplate rest = new RestTemplate();
    private final ObjectMapper om = new ObjectMapper();

    private final MexcTradeService mexc;

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(
            1, r -> {
                Thread t = new Thread(r, "spread-monitor");
                t.setDaemon(true);
                return t;
            });

    @Getter
    private final Map<String, MonitorHandle> active = new ConcurrentHashMap<>();

    @Value
    public static class SpreadSnapshot {
        BigDecimal bid;
        BigDecimal ask;
        BigDecimal spread; // ask - bid
        boolean    exSelf;
    }

    @Value
    public static class MonitorConfig {
        String symbol;
        BigDecimal rangeLow;
        BigDecimal rangeHigh;
        Duration  period;
        int       tickSafety;
        boolean   excludeSelf;
        Long      chatId;       // для excludeSelf и для ключа монитора
    }

    public static class MonitorHandle implements AutoCloseable {
        private final String key;
        private final ScheduledFuture<?> fut;
        private final Runnable onStop;
        private volatile boolean stopped = false;

        MonitorHandle(String key, ScheduledFuture<?> fut, Runnable onStop) {
            this.key = key;
            this.fut = fut;
            this.onStop = onStop;
        }
        @Override
        public void close() {
            if (stopped) return;
            stopped = true;
            if (fut != null) fut.cancel(false);
            if (onStop != null) onStop.run();
        }
    }

    public MonitorHandle startMonitor(MonitorConfig cfg, Consumer<SpreadSnapshot> onStopCallback) {
        final String key = cfg.getSymbol() + "#" + cfg.getChatId();
        stopMonitor(cfg.getSymbol(), cfg.getChatId());

        final java.util.concurrent.atomic.AtomicBoolean wasInside = new java.util.concurrent.atomic.AtomicBoolean(false);

        long periodMs = Math.max(80, cfg.getPeriod().toMillis());
        ScheduledFuture<?> fut = scheduler.scheduleAtFixedRate(() -> {
            try {
                SpreadSnapshot s = cfg.isExcludeSelf()
                        ? fetchTopExSelf(cfg.getSymbol(), cfg.getChatId())
                        : fetchTopRaw(cfg.getSymbol());
                if (LOG_TICK_SPREAD && log.isDebugEnabled()) {
                    // Удобный однострочник с текущим снимком
                    log.debug("SPREAD[{}|{}|{}] bid={} ask={} spread={} | range=[{}..{}]",
                            cfg.getSymbol(), cfg.getChatId(), (cfg.isExcludeSelf() ? "EX-SELF" : "RAW"),
                            s.getBid().stripTrailingZeros().toPlainString(),
                            s.getAsk().stripTrailingZeros().toPlainString(),
                            s.getSpread().stripTrailingZeros().toPlainString(),
                            cfg.getRangeLow().stripTrailingZeros().toPlainString(),
                            cfg.getRangeHigh().stripTrailingZeros().toPlainString());
                }
                // DIAG: параллельно снимем RAW для сравнения
                if (LOG_TICK_SPREAD && log.isDebugEnabled()) {
                    try {
                        SpreadSnapshot raw = fetchTopRaw(cfg.getSymbol());
                        log.debug("SPREAD-RAW[{}|{}] bid={} ask={} spread={}",
                                cfg.getSymbol(), cfg.getChatId(),
                                raw.getBid().stripTrailingZeros().toPlainString(),
                                raw.getAsk().stripTrailingZeros().toPlainString(),
                                raw.getSpread().stripTrailingZeros().toPlainString());
                    } catch (Exception ignore) { }
                }

                boolean violate = violatesRange(cfg, s);
                if (!violate) {
                    wasInside.set(true); // запомнили, что хотя бы раз были внутри
                } else if (wasInside.get()) {
                    // только переход inside -> outside считается триггером
                    Optional.ofNullable(onStopCallback).ifPresent(cb -> {
                        try { cb.accept(s); } catch (Exception ignore) {}
                    });
                    stopMonitor(cfg.getSymbol(), cfg.getChatId());
                }
            } catch (Exception e) {
                log.error("SpreadMonitor error [{}|{}]: {}", cfg.getSymbol(), cfg.getChatId(), e.getMessage());
            }
        }, 0, periodMs, TimeUnit.MILLISECONDS);

        MonitorHandle handle = new MonitorHandle(key, fut, () -> active.remove(key));
        active.put(key, handle);
        return handle;
    }


    public void stopMonitor(String symbol, Long chatId) {
        String key = symbol + "#" + chatId;
        MonitorHandle h = active.remove(key);
        if (h != null) {
            try { h.close(); } catch (Exception ignore) {}
            log.info("⏹ [{}|{}] Остановил мониторинг спреда", symbol, chatId);
        }
    }

    // ===== Источники котировок =====

    private SpreadSnapshot fetchTopRaw(String symbol) {
        ResponseEntity<String> r = rest.getForEntity(API_BASE + TICKER_BOOK + "?symbol=" + symbol, String.class);
        JsonNode j;
        try { j = om.readTree(Objects.requireNonNull(r.getBody())); }
        catch (Exception e) { throw new RuntimeException("bad json: " + e.getMessage(), e); }

        BigDecimal bid = new BigDecimal(j.path("bidPrice").asText("0"));
        BigDecimal ask = new BigDecimal(j.path("askPrice").asText("0"));
        if (bid.signum() <= 0 && ask.signum() > 0) bid = ask;
        if (ask.signum() <= 0 && bid.signum() > 0) ask = bid;
        if (bid.signum() <= 0 && ask.signum() <= 0) {
            return new SpreadSnapshot(ZERO, ZERO, ZERO, false);
        }
        return new SpreadSnapshot(bid, ask, ask.subtract(bid), false);
    }

    private SpreadSnapshot fetchTopExSelf(String symbol, Long chatId) {
        try {
            MexcTradeService.TopOfBook tob = mexc.topExcludingSelf(symbol, chatId, DEPTH_LIMIT);
            BigDecimal bid = tob.bid();
            BigDecimal ask = tob.ask();
            if (bid.signum() <= 0 && ask.signum() > 0) bid = ask;
            if (ask.signum() <= 0 && bid.signum() > 0) ask = bid;
            if (bid.signum() <= 0 && ask.signum() <= 0) {
                return new SpreadSnapshot(ZERO, ZERO, ZERO, true);
            }
            return new SpreadSnapshot(bid, ask, ask.subtract(bid), true);
        } catch (Exception e) {
            log.warn("fetchTopExSelf[{}|{}] error: {}", symbol, chatId, e.getMessage());
            SpreadSnapshot raw = fetchTopRaw(symbol);
            return new SpreadSnapshot(raw.getBid(), raw.getAsk(), raw.getSpread(), true);
        }
    }

    // TRUE -> надо остановиться (вилка НЕ целиком внутри [bid; ask] с запасом pad)
    private boolean violatesRange(MonitorConfig cfg, SpreadSnapshot s) {
        // пустой/невалидный стакан — стопаемся
        if (s.getBid().signum() <= 0 || s.getAsk().signum() <= 0) return true;
        if (s.getAsk().compareTo(s.getBid()) <= 0) return true;

        // небольшой «зазор» от краёв спреда
        BigDecimal pad = s.getSpread()
                .divide(new java.math.BigDecimal("500"), 12, java.math.RoundingMode.HALF_UP)
                .multiply(java.math.BigDecimal.valueOf(Math.max(0, Math.min(3, cfg.getTickSafety()))));

        // Правильная геометрия: [LOW..HIGH] внутри [bid+pad .. ask-pad]
        boolean inside =
                cfg.getRangeLow().compareTo(s.getBid().add(pad)) >= 0 &&
                        cfg.getRangeHigh().compareTo(s.getAsk().subtract(pad)) <= 0;

        return !inside;
    }

}

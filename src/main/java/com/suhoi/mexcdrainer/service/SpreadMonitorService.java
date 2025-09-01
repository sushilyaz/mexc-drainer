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

/**
 * Монитор спреда для заданного диапазона.
 * Поддерживает два режима:
 *  - RAW: обычный топ стакана (bookTicker)
 *  - EX-SELF: топ стакана за вычетом собственных лимиток Аккаунта A (через depth + openOrders)
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class SpreadMonitorService {

    private static final String API_BASE = "https://api.mexc.com";
    private static final String TICKER_BOOK = "/api/v3/ticker/bookTicker";

    /** Сколько уровней глубины берём для EX-SELF оценки (REST). */
    private static final int DEPTH_LIMIT = 20;

    private final RestTemplate rest = new RestTemplate();
    private final ObjectMapper om = new ObjectMapper();

    /** Нужен для EX-SELF режима (depth + openOrders + расчёт top без собственных заявок). */
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
        boolean    exSelf; // true если bid/ask посчитаны без собственных лимиток
    }

    @Value
    public static class MonitorConfig {
        String symbol;
        BigDecimal rangeLow;    // включительно
        BigDecimal rangeHigh;   // включительно
        Duration  period;       // период опроса
        int       tickSafety;   // "тик-запас" (0..3)
        boolean   excludeSelf;  // считать спред «без своих» лимиток?
        Long      chatId;       // чей A-аккаунт исключать (для openOrders)
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
            if (fut != null) fut.cancel(true);
            if (onStop != null) onStop.run();
        }
    }

    /**
     * Запустить мониторинг. При выходе диапазона из спреда — вызовет onStopCallback и самостопнется.
     */
    public MonitorHandle startMonitor(MonitorConfig cfg, Consumer<SpreadSnapshot> onStopCallback) {
        String key = cfg.getSymbol();
        stopMonitor(key); // если уже был

        long periodMs = Math.max(50, cfg.getPeriod().toMillis());
        ScheduledFuture<?> fut = scheduler.scheduleAtFixedRate(() -> {
            try {
                SpreadSnapshot s = cfg.isExcludeSelf()
                        ? fetchTopExSelf(cfg.getSymbol(), cfg.getChatId())
                        : fetchTopRaw(cfg.getSymbol());

                if (violatesRange(cfg, s)) {
                    log.warn("🛑 [{}] Диапазон {}–{} ВЫШЕЛ из {}спреда: bid={} ask={} spread={}",
                            cfg.getSymbol(),
                            cfg.getRangeLow().toPlainString(), cfg.getRangeHigh().toPlainString(),
                            s.isExSelf() ? "EX-SELF " : "",
                            s.getBid().toPlainString(), s.getAsk().toPlainString(), s.getSpread().toPlainString());
                    Optional.ofNullable(onStopCallback).ifPresent(cb -> {
                        try { cb.accept(s); } catch (Exception ignore) {}
                    });
                    stopMonitor(key);
                }
            } catch (Exception e) {
                log.error("SpreadMonitor error [{}]: {}", cfg.getSymbol(), e.getMessage());
            }
        }, 0, periodMs, TimeUnit.MILLISECONDS);

        MonitorHandle handle = new MonitorHandle(key, fut, () -> active.remove(key));
        active.put(key, handle);

        log.info("▶️ [{}] Запустил мониторинг спреда: диапазон={}–{}, период={}ms, режим={}",
                cfg.getSymbol(),
                cfg.getRangeLow().toPlainString(), cfg.getRangeHigh().toPlainString(),
                periodMs,
                cfg.isExcludeSelf() ? "EX-SELF" : "RAW");
        return handle;
    }

    public void stopMonitor(String symbol) {
        MonitorHandle h = active.remove(symbol);
        if (h != null) {
            try { h.close(); } catch (Exception ignore) {}
            log.info("⏹ [{}] Остановил мониторинг спреда", symbol);
        }
    }

    // ===== Источники котировок =====

    /** Обычный топ стакана (bookTicker). */
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
            // мёртвый стакан — считаем spread=0, чтобы сразу остановиться
            return new SpreadSnapshot(ZERO, ZERO, ZERO, false);
        }
        return new SpreadSnapshot(bid, ask, ask.subtract(bid), false);
    }

    /**
     * Топ стакана «без своих лимиток» аккаунта A:
     * depth(limit=DEPTH_LIMIT) - openOrders(A) → вычитаем свои остатки на ценах → ищем первый уровень с netQty>0.
     */
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
            log.warn("fetchTopExSelf[{}] error: {}", symbol, e.getMessage());
            // фолбэк на RAW, чтобы не глохнуть
            SpreadSnapshot raw = fetchTopRaw(symbol);
            return new SpreadSnapshot(raw.getBid(), raw.getAsk(), raw.getSpread(), true);
        }
    }

    // ===== Правило остановки =====

    /** TRUE → надо остановиться (диапазон больше не полностью внутри спреда). */
    private boolean violatesRange(MonitorConfig cfg, SpreadSnapshot s) {
        if (s.getSpread().signum() <= 0) return true;

        // Условие 1: спред шире диапазона
        BigDecimal bandWidth = cfg.getRangeHigh().subtract(cfg.getRangeLow());
        if (s.getSpread().compareTo(bandWidth) < 0) return true;

        // Условие 2: диапазон полностью внутри [bid; ask], с "тик-запасом" по краям
        // tickSafety реализуем как долю спреда, чтобы не лезть в тик-логику здесь
        BigDecimal pad = s.getSpread().divide(BigDecimal.valueOf(500L), 12, java.math.RoundingMode.HALF_UP)
                .multiply(BigDecimal.valueOf(Math.max(0, Math.min(3, cfg.getTickSafety())))); // ~0.2% спреда на "тик"

        boolean inside = s.getBid().subtract(pad).compareTo(cfg.getRangeLow()) <= 0
                && s.getAsk().add(pad).compareTo(cfg.getRangeHigh()) >= 0;

        return !inside;
    }
}

package com.suhoi.mexcdrainer.ws.user;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.InvalidProtocolBufferException;
import com.mxc.push.common.protobuf.PrivateOrdersV3Api;
import com.mxc.push.common.protobuf.PrivateDealsV3Api;
import com.mxc.push.common.protobuf.PrivateAccountV3Api;
import com.mxc.push.common.protobuf.PushDataV3ApiWrapper;
import com.suhoi.mexcdrainer.util.MemoryDb;
import com.suhoi.mexcdrainer.ws.WsConnectionManager;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;
import org.springframework.util.StringUtils;

import java.net.URI;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;

/**
 * Подключение к приватным ws по listenKey.
 * Подписка на:
 *  - spot@private.orders.v3.api.pb    — статусы ордеров (ключ для ожиданий FILLED)
 *  - spot@private.deals.v3.api.pb     — сделки (для avg/qty/spent и верификации)
 *  - spot@private.account.v3.api.pb   — изменения балансов (USDT/base)
 */
@Slf4j
@RequiredArgsConstructor
public class UserDataWsService implements AutoCloseable {

    private final ObjectMapper om;
    private final ListenKeyClient listenKeyClient;

    private final ScheduledExecutorService ses =
            Executors.newSingleThreadScheduledExecutor(new CustomizableThreadFactory("mexc-userws-"));

    private volatile WsConnectionManager ws;
    private volatile String activeListenKey;
    private volatile String activeApiKey;
    private volatile String activeSecret;

    // Ожидалки состояний ордеров
    private final ConcurrentMap<String, CompletableFuture<OrderUpdate>> orderFutures = new ConcurrentHashMap<>();

    // Кеш балансoв по asset
    private final ConcurrentMap<String, String> lastBalances = new ConcurrentHashMap<>();

    // Текущие подписки (каналы)
    private static final Set<String> CHANNELS = Set.of(
            "spot@private.orders.v3.api.pb",
            "spot@private.deals.v3.api.pb",
            "spot@private.account.v3.api.pb"
    );

    public void start(String apiKey, String secret, String wsBaseUrl,
                      int pingMs, int wsPingFrameMs,
                      int reconnectBase, int reconnectMax,
                      int listenKeyKeepAliveMinutes) {
        this.activeApiKey = apiKey;
        this.activeSecret = secret;

        // 1) генерим listenKey
        this.activeListenKey = listenKeyClient.createListenKey(apiKey, secret);
        // 2) ws connect
        URI uri = URI.create(wsBaseUrl + "?listenKey=" + activeListenKey);
        this.ws = WsConnectionManager.builder()
                .uri(uri)
                .textHandler(this::onText)
                .binaryHandler(this::onBinary)
                .openHandler(() -> {
                    log.info("UserWS opened. Subscribing channels...");
                    CHANNELS.forEach(ch -> {
                        ws.rememberSubscription(ch);
                        ws.sendText("""
                            {"method":"SUBSCRIPTION","params":["%s"]}
                            """.formatted(ch));
                    });
                })
                .closeHandler((code, reason) -> {
                    // Переподключение
                    reconnectLoop(wsBaseUrl, pingMs, wsPingFrameMs, reconnectBase, reconnectMax);
                })
                .errorHandler(err -> log.warn("UserWS error: {}", err.toString()))
                .pingIntervalMs(pingMs)
                .wsPingFrameIntervalMs(wsPingFrameMs)
                .build();
        this.ws.connectWithRetry(reconnectBase, reconnectMax);

        // 3) плановый keepalive listenKey
        ses.scheduleAtFixedRate(() -> {
            try {
                if (StringUtils.hasText(activeListenKey)) {
                    listenKeyClient.keepAlive(activeApiKey, activeSecret, activeListenKey);
                }
            } catch (Exception e) {
                log.warn("listenKey keepalive error: {}", e.getMessage());
            }
        }, listenKeyKeepAliveMinutes, listenKeyKeepAliveMinutes, TimeUnit.MINUTES);
    }

    private void reconnectLoop(String wsBaseUrl, int pingMs, int wsPingFrameMs, int base, int max) {
        if (!StringUtils.hasText(activeApiKey)) return;
        while (true) {
            try {
                // Получаем новый listenKey на коннект (старый можно не закрывать при аварийном дисконнекте)
                this.activeListenKey = listenKeyClient.createListenKey(activeApiKey, activeSecret);
                URI uri = URI.create(wsBaseUrl + "?listenKey=" + activeListenKey);
                int finalBase = base;
                this.ws = WsConnectionManager.builder()
                        .uri(uri)
                        .textHandler(this::onText)
                        .binaryHandler(this::onBinary)
                        .openHandler(() -> {
                            CHANNELS.forEach(ch -> {
                                ws.rememberSubscription(ch);
                                ws.sendText("""
                                    {"method":"SUBSCRIPTION","params":["%s"]}
                                    """.formatted(ch));
                            });
                        })
                        .closeHandler((c, r) -> reconnectLoop(wsBaseUrl, pingMs, wsPingFrameMs, finalBase, max))
                        .errorHandler(err -> log.warn("UserWS error: {}", err.toString()))
                        .pingIntervalMs(pingMs)
                        .wsPingFrameIntervalMs(wsPingFrameMs)
                        .build();
                this.ws.connectWithRetry(base, max);
                return;
            } catch (Exception e) {
                log.warn("reconnect userWS failed: {}", e.getMessage());
                sleep(Math.min(max, base *= 2));
            }
        }
    }

    // ==== Публичные API ====

    /** Ожидание конечного статуса ордера по orderId. Возвращает апдейт (status, avgPrice, executedQty ...). */
    public OrderUpdate awaitOrderFinal(String orderId, Duration timeout) {
        CompletableFuture<OrderUpdate> fut = new CompletableFuture<>();
        orderFutures.put(orderId, fut);
        try {
            return fut.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException te) {
            fut.completeExceptionally(te);
            throw new RuntimeException("Timeout waiting order " + orderId);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            orderFutures.remove(orderId);
        }
    }

    public String getLastBalance(String asset) {
        return lastBalances.get(asset);
    }

    @Override public void close() {
        try { if (ws != null) ws.close(); } catch (Exception ignore) {}
        try { ses.shutdownNow(); } catch (Exception ignore) {}
        try {
            if (StringUtils.hasText(activeListenKey)) {
                listenKeyClient.close(activeApiKey, activeSecret, activeListenKey);
            }
        } catch (Exception e) {
            log.warn("listenKey close error: {}", e.getMessage());
        }
    }

    // ==== Handlers ====

    private void onText(String t) {
        // Приходят ответы на SUBSCRIPTION/UNSUBSCRIPTION/PONG в JSON
        log.debug("UserWS TEXT: {}", t);
    }

    private void onBinary(byte[] bytes) {
        try {
            PushDataV3ApiWrapper w = PushDataV3ApiWrapper.parseFrom(bytes);
            String ch = w.getChannel();
            if (ch.contains("private.orders")) {
                PrivateOrdersV3Api po = w.getPrivateOrders();
                handleOrder(po, w.getSymbol(), w.getSendTime());
            } else if (ch.contains("private.deals")) {
                PrivateDealsV3Api pd = w.getPrivateDeals();
                handleDeal(pd, w.getSymbol(), w.getSendTime());
            } else if (ch.contains("private.account")) {
                PrivateAccountV3Api pa = w.getPrivateAccount();
                handleAccount(pa, w.getSendTime());
            }
        } catch (InvalidProtocolBufferException e) {
            log.warn("UserWS binary parse error: {}", e.getMessage());
        }
    }

    private void handleOrder(PrivateOrdersV3Api po, String symbol, long sendTime) {
        // status: 1=NEW, 2=FILLED, 3=PARTIALLY_FILLED, 4=CANCELED, 5=PARTIALLY_CANCELED
        String orderId = po.getClientId(); // см. доку: clientId ~ orderId
        int status = po.getStatus();
        var upd = new OrderUpdate(orderId, symbol, status,
                po.getAvgPrice(), po.getCumulativeQuantity(), po.getCumulativeAmount(), sendTime);

        log.debug("ORD-UPDATE {} s={} avg={} cumQty={} cumAmt={}",
                orderId, status, upd.avgPrice, upd.cumQty, upd.cumAmt);

        // если финальный — завершаем ожидалки
        if (status == 2 || status == 4 || status == 5) {
            complete(orderId, upd);
        }
    }

    private void handleDeal(PrivateDealsV3Api pd, String symbol, long sendTime) {
        // Можно дополнительно сшивать сделки с ордером, если нужно
        log.debug("DEAL {} p={} q={} amount={}", symbol, pd.getPrice(), pd.getQuantity(), pd.getAmount());
    }

    private void handleAccount(PrivateAccountV3Api pa, long sendTime) {
        lastBalances.put(pa.getVcoinName(), pa.getBalanceAmount());
    }

    private void complete(String orderId, OrderUpdate upd) {
        CompletableFuture<OrderUpdate> fut = orderFutures.remove(orderId);
        if (fut != null) fut.complete(upd);
    }

    private static void sleep(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
    }

    @Value
    public static class OrderUpdate {
        String orderId;
        String symbol;
        int status;
        String avgPrice;
        String cumQty;
        String cumAmt;
        long sendTime;
    }
}

package com.suhoi.mexcdrainer.ws.user;

import com.mxc.push.common.protobuf.PrivateAccountV3Api;
import com.mxc.push.common.protobuf.PrivateDealsV3Api;
import com.mxc.push.common.protobuf.PrivateOrdersV3Api;
import com.suhoi.mexcdrainer.config.WsMarketProperties;
import com.suhoi.mexcdrainer.config.WsUserProperties;
import com.suhoi.mexcdrainer.ws.ListenKeyClient;
import com.suhoi.mexcdrainer.ws.MexcWsClient;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.*;

/**
 * Менеджер приватных WS-подключений по apiKey.
 * - Создаёт listenKey, поднимает отдельный MexcWsClient с endpoint?listenKey=...
 * - Подписывает на private.account / private.orders / private.deals
 * - Проксирует события в трекеры ордеров/балансов и реестр собственных заявок.
 */
@Slf4j
public class UserWsManager {

    private final WsMarketProperties baseWsProps;
    private final WsUserProperties userProps;
    private final ListenKeyClient listenKeyClient;
    private final OrderStateTracker orderStateTracker;
    private final BalanceTracker balanceTracker;
    private final OwnOrdersRegistry ownOrdersRegistry;

    public UserWsManager(WsMarketProperties baseWsProps,
                         WsUserProperties userProps,
                         ListenKeyClient listenKeyClient,
                         OrderStateTracker orderStateTracker,
                         BalanceTracker balanceTracker,
                         OwnOrdersRegistry ownOrdersRegistry) {
        this.baseWsProps = baseWsProps;
        this.userProps = userProps;
        this.listenKeyClient = listenKeyClient;
        this.orderStateTracker = orderStateTracker;
        this.balanceTracker = balanceTracker;
        this.ownOrdersRegistry = ownOrdersRegistry;
    }

    /** Поддерживаемые соединения по apiKey. */
    private final Map<String, UserConn> conns = new ConcurrentHashMap<>();
    /** Храним Listener, чтобы перевесить его при ротации. */
    private final Map<String, MexcWsClient.Listener> listenersByApiKey = new ConcurrentHashMap<>();

    public void start() {
        log.info("[WS_USER] manager started");
    }

    public void stop() {
        for (UserConn c : conns.values()) {
            safeClose(c);
        }
        conns.clear();
        listenersByApiKey.clear();
        log.info("[WS_USER] manager stopped");
    }

    /** Гарантирует активный user-WS для заданной пары ключей. */
    public void ensure(String apiKey, String secret) {
        Objects.requireNonNull(apiKey, "apiKey required");
        Objects.requireNonNull(secret, "secret required");
        conns.computeIfAbsent(apiKey, k -> open(apiKey, secret));
    }

    /* ======================== внутренняя кухня ======================== */

    private UserConn open(String apiKey, String secret) {
        // 1) listenKey
        String lk = listenKeyClient.createListenKey(apiKey, secret);

        // 2) сам WS-клиент (под приватный поток)
        String endpointWithKey = baseWsProps.getEndpoint() + "?listenKey=" + lk;
        MexcWsClient client = new MexcWsClient(
                endpointWithKey,
                baseWsProps.getPingPeriodSec(),
                baseWsProps.getRotateAfterHours(),
                baseWsProps.getReconnectBackoffBaseSec(),
                baseWsProps.getReconnectBackoffMaxSec()
        );

        // 3) listener
        MexcWsClient.Listener listener = createUserListener(apiKey);
        client.addListener(listener);
        listenersByApiKey.put(apiKey, listener);

        // 4) connect + subs
        client.connect();
        client.subscribe(
                "spot@private.account.v3.api.pb",
                "spot@private.deals.v3.api.pb",
                "spot@private.orders.v3.api.pb"
        );

        // 5) планировщик keepalive
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "user-" + hide(apiKey));
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleAtFixedRate(() -> safeKeepAlive(apiKey), userProps.getKeepAliveMinutes(),
                userProps.getKeepAliveMinutes(), TimeUnit.MINUTES);

        log.info("[WS_USER] started for {}", hide(apiKey));
        return new UserConn(apiKey, secret, lk, client, scheduler);
    }

    private MexcWsClient.Listener createUserListener(String apiKey) {
        return new MexcWsClient.Listener() {
            @Override public void onOpen() { log.info("✅ WS_USER[{}] OPEN", hide(apiKey)); }

            @Override
            public void onPrivateAccount(PrivateAccountV3Api acc, long ts) {
                String asset = acc.getVcoinName();
                BigDecimal bal = bd(acc.getBalanceAmount());
                BigDecimal frozen = bd(acc.getFrozenAmount());
                balanceTracker.update(apiKey, asset, bal, frozen, ts);
                log.debug("[WS_USER][ACC] {} {} bal={} frozen={} t={}", hide(apiKey), asset, bal, frozen, ts);
            }

            @Override
            public void onPrivateDeals(String symbol, PrivateDealsV3Api d, long ts) {
                String exchOrderId = d.getOrderId(); // соответствует PrivateOrdersV3Api.id
                // Если захочешь учитывать поштучные филлы:
                // BigDecimal fillQty = bd(d.getQuantity());
                // Добавь в OwnOrdersRegistry метод incrementExecution(...) и дергай его здесь.
                log.debug("[WS_USER][DEAL] {} {} orderId={} (clientId={}) px={} qty={} amt={} fee={} {} maker={}",
                        hide(apiKey), symbol, exchOrderId, d.getClientOrderId(), d.getPrice(), d.getQuantity(),
                        d.getAmount(), d.getFeeAmount(), d.getFeeCurrency(), d.getIsMaker());
            }


            @Override
            public void onPrivateOrders(String symbol, PrivateOrdersV3Api o, long ts) {
                String statusTxt = switch (o.getStatus()) {
                    case 1 -> "NEW";
                    case 2 -> "FILLED";
                    case 3 -> "PARTIALLY_FILLED";
                    case 4 -> "CANCELED";
                    case 5 -> "PARTIALLY_CANCELED";
                    default -> "UNKNOWN";
                };

                // ✅ биржевой ID (главный ключ)
                String exchOrderId = o.getId();
                // клиентский ID — дополнительный
                String clientId    = o.getClientId();

                BigDecimal avg   = bd(o.getAvgPrice());
                BigDecimal cumQ  = bd(o.getCumulativeQuantity());
                BigDecimal cumUS = bd(o.getCumulativeAmount());

                orderStateTracker.onOrderEvent(new OrderStateTracker.OrderState(
                        apiKey, exchOrderId, clientId, symbol, statusTxt,
                        cumQ, cumUS, avg, ts
                ));

                boolean finalState = "FILLED".equals(statusTxt) || "CANCELED".equals(statusTxt) || "REJECTED".equals(statusTxt);
                ownOrdersRegistry.markExecution(apiKey, exchOrderId, cumQ, finalState);

                log.info("[WS_USER][ORDER] {} {} id={} (clientId={}) type={} side={} px={} qty={} filled={} status={}",
                        hide(apiKey), symbol, exchOrderId, clientId, o.getOrderType(), o.getTradeType(),
                        o.getPrice(), o.getQuantity(), cumQ, statusTxt);
            }
        };
    }

    private void safeKeepAlive(String apiKey) {
        var conn = conns.get(apiKey);
        if (conn == null) return;
        try {
            listenKeyClient.keepAlive(conn.getApiKey(), conn.getSecret(), conn.getListenKey());
            log.debug("[WS_USER] keepalive OK for {}", hide(apiKey));
        } catch (Exception e) {
            log.warn("[WS_USER] keepalive failed for {}: {}", hide(apiKey), e.toString());
            // Ротация listenKey и переподъём WS
            try {
                String newKey = listenKeyClient.createListenKey(conn.getApiKey(), conn.getSecret());
                MexcWsClient old = conn.getClient();
                if (old != null) old.close();

                String endpointWithKey = baseWsProps.getEndpoint() + "?listenKey=" + newKey;
                MexcWsClient client = new MexcWsClient(
                        endpointWithKey,
                        baseWsProps.getPingPeriodSec(),
                        baseWsProps.getRotateAfterHours(),
                        baseWsProps.getReconnectBackoffBaseSec(),
                        baseWsProps.getReconnectBackoffMaxSec()
                );
                // перевешиваем listener (берём наш сохранённый)
                MexcWsClient.Listener listener = listenersByApiKey.computeIfAbsent(apiKey, this::createUserListener);
                client.addListener(listener);
                client.connect();
                client.subscribe(
                        "spot@private.account.v3.api.pb",
                        "spot@private.deals.v3.api.pb",
                        "spot@private.orders.v3.api.pb"
                );

                conn.setListenKey(newKey);
                conn.setClient(client);
                log.info("[WS_USER] rotated to new listenKey for {}", hide(apiKey));
            } catch (Exception ex) {
                log.error("[WS_USER] failed to rotate listenKey for {}: {}", hide(apiKey), ex.toString());
            }
        }
    }

    public void registerPlaced(String apiKey, String symbol, String orderId, String clientId,
                               OwnOrdersRegistry.Side side, BigDecimal price, BigDecimal qty) {
        orderStateTracker.registerPlaced(apiKey, orderId, clientId, symbol);
        ownOrdersRegistry.register(apiKey, orderId, symbol, side, price, qty);
    }

    public OrderStateTracker getOrderStateTracker() { return orderStateTracker; }
    public BalanceTracker getBalanceTracker() { return balanceTracker; }
    public OwnOrdersRegistry getOwnOrdersRegistry() { return ownOrdersRegistry; }

    public void close(String apiKey) {
        var c = conns.remove(apiKey);
        if (c != null) safeClose(c);
    }

    private void safeClose(UserConn c) {
        try { if (c.getScheduler() != null) c.getScheduler().shutdownNow(); } catch (Exception ignore) {}
        try { listenKeyClient.close(c.getApiKey(), c.getSecret(), c.getListenKey()); } catch (Exception e) {
            log.debug("[WS_USER] close listenKey err: {}", e.toString());
        }
        try { if (c.getClient() != null) c.getClient().close(); } catch (Exception ignore) {}
        log.info("[WS_USER] closed for {}", hide(c.getApiKey()));
    }

    private static BigDecimal bd(String s) {
        try { return new BigDecimal(s); } catch (Exception e) { return BigDecimal.ZERO; }
    }

    private static String hide(String apiKey) {
        if (apiKey == null || apiKey.length() < 6) return "***";
        return apiKey.substring(0, 3) + "…" + apiKey.substring(apiKey.length() - 3);
    }

    /* ===== простая DTO без final-полей, чтобы можно было менять listenKey/client ===== */
    private static class UserConn {
        private final String apiKey;
        private final String secret;
        private volatile String listenKey;
        private volatile MexcWsClient client;
        private final ScheduledExecutorService scheduler;

        public UserConn(String apiKey, String secret, String listenKey, MexcWsClient client, ScheduledExecutorService scheduler) {
            this.apiKey = apiKey;
            this.secret = secret;
            this.listenKey = listenKey;
            this.client = client;
            this.scheduler = scheduler;
        }
        public String getApiKey() { return apiKey; }
        public String getSecret() { return secret; }
        public String getListenKey() { return listenKey; }
        public MexcWsClient getClient() { return client; }
        public ScheduledExecutorService getScheduler() { return scheduler; }
        public void setListenKey(String listenKey) { this.listenKey = listenKey; }
        public void setClient(MexcWsClient client) { this.client = client; }
    }
}

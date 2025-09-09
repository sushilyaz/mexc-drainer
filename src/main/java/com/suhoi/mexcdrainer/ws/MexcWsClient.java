package com.suhoi.mexcdrainer.ws;

import com.google.protobuf.InvalidProtocolBufferException;
import lombok.extern.slf4j.Slf4j;
import com.mxc.push.common.protobuf.*;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * Лёгкий WS-клиент под MEXC v3 protobuf.
 * - Поддерживает SUB/UNSUB, авто-реконнект с backoff, PING, ротацию < 24h.
 * - Парсит бинарные события PushDataV3ApiWrapper и раздаёт по Listener.
 * - Предусмотрен flush отложенных подписок после коннекта.
 */
@Slf4j
public final class MexcWsClient implements WebSocket.Listener {

    public interface Listener {
        default void onOpen() {}
        default void onAck(String json) {}
        default void onPong() {}
        default void onError(Throwable t) {}
        default void onClosed(int code, String reason) {}

        // Market
        default void onDeals(String symbol, PublicAggreDealsV3Api deals, long sendTime) {}
        default void onDepthInc(String symbol, PublicIncreaseDepthsV3Api inc, long sendTime) {}
        default void onLimitDepth(String symbol, PublicLimitDepthsV3Api depth, long sendTime) {}
        default void onBookTicker(String symbol, PublicAggreBookTickerV3Api bt, long sendTime) {}
        default void onMiniTicker(String symbol, PublicMiniTickerV3Api mt, long sendTime) {}
        default void onAggreDepths(String symbol, PublicAggreDepthsV3Api ag, long sendTime) {} // NEW

        // User
        default void onPrivateAccount(PrivateAccountV3Api acc, long sendTime) {}
        default void onPrivateOrders(String symbol, PrivateOrdersV3Api orders, long sendTime) {}
        default void onPrivateDeals(String symbol, PrivateDealsV3Api deals, long sendTime) {}
    }

    private final String endpoint;
    private final int pingSec;
    private final int rotateHours;
    private final int backoffBaseSec;
    private final int backoffMaxSec;

    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
    private final CopyOnWriteArrayList<Listener> listeners = new CopyOnWriteArrayList<>();
    private final ScheduledExecutorService sched = Executors.newScheduledThreadPool(2, r -> {
        Thread t = new Thread(r, "mexc-ws"); t.setDaemon(true); return t;
    });

    private final Set<String> subs = ConcurrentHashMap.newKeySet();
    private final AtomicBoolean reconnecting = new AtomicBoolean(false);
    private volatile WebSocket ws;

    public MexcWsClient(String endpoint, int pingSec, int rotateHours, int backoffBaseSec, int backoffMaxSec) {
        this.endpoint = endpoint;
        this.pingSec = pingSec;
        this.rotateHours = rotateHours;
        this.backoffBaseSec = backoffBaseSec;
        this.backoffMaxSec = backoffMaxSec;
    }

    public void addListener(Listener l) { listeners.add(l); }
    public void removeListener(Listener l) { listeners.remove(l); }

    public synchronized void connect() {
        log.info("🔌 WS connect {}", endpoint);
        http.newWebSocketBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .buildAsync(URI.create(endpoint), this)
                .whenComplete((sock, err) -> {
                    if (err != null) {
                        log.warn("❌ WS connect failed: {}", err.toString());
                        scheduleReconnect(1);
                    } else {
                        this.ws = sock;
                        sock.request(1);
                        fire(Listener::onOpen);
                        // Таймеры
                        sched.scheduleAtFixedRate(this::safePing, pingSec, pingSec, TimeUnit.SECONDS);
                        sched.schedule(this::forceRotate, rotateHours, TimeUnit.HOURS);
                        // Flush queued SUBS
                        sendAllCurrentSubscriptions();
                    }
                });
    }

    public synchronized void close() {
        try { if (ws != null) ws.sendClose(WebSocket.NORMAL_CLOSURE, "bye"); } catch (Exception ignore) {}
        sched.shutdownNow();
    }

    public void subscribe(String... channels) {
        if (channels == null || channels.length == 0) return;
        for (String ch : channels) subs.add(ch);
        WebSocket s = ws;
        if (s == null) { log.info("🕗 queued SUB {}", String.join(", ", channels)); return; }
        sendText(buildCmd("SUBSCRIPTION", channels));
        log.info("📬 SUB {}", String.join(", ", channels));
    }

    public void unsubscribe(String... channels) {
        if (channels == null || channels.length == 0) return;
        for (String ch : channels) subs.remove(ch);
        WebSocket s = ws;
        if (s == null) { log.info("🕗 queued UNSUB {}", String.join(", ", channels)); return; }
        sendText(buildCmd("UNSUBSCRIPTION", channels));
        log.info("🗑️  UNSUB {}", String.join(", ", channels));
    }

    private void sendAllCurrentSubscriptions() {
        if (subs.isEmpty() || ws == null) return;
        sendText(buildCmd("SUBSCRIPTION", subs.toArray(String[]::new)));
        log.info("📬 SUB (flush queued) {}", String.join(", ", subs));
    }

    private void safePing() { try { sendText("{\"method\":\"PING\"}"); } catch (Exception ignore) {} }
    private void forceRotate() { try { if (ws != null) ws.sendClose(WebSocket.NORMAL_CLOSURE, "rotate"); } catch (Exception ignore) {} }

    private void scheduleReconnect(int attempt) {
        if (!reconnecting.compareAndSet(false, true)) return;
        long delay = Math.min(backoffMaxSec, (long) (backoffBaseSec * Math.pow(2, attempt - 1)));
        log.info("⏳ WS reconnect in {} sec (attempt #{})", delay, attempt);
        sched.schedule(() -> { try { connect(); } finally { reconnecting.set(false); } }, delay, TimeUnit.SECONDS);
    }

    private synchronized void sendText(String json) {
        if (ws == null) throw new IllegalStateException("WS not connected");
        ws.sendText(json, true);
    }

    private static String buildCmd(String method, String[] params) {
        StringBuilder sb = new StringBuilder("{\"method\":\"").append(method).append("\",\"params\":[");
        for (int i = 0; i < params.length; i++) {
            if (i > 0) sb.append(',');
            sb.append('"').append(params[i].replace("\"", "\\\"")).append('"');
        }
        return sb.append("]}").toString();
    }

    // WebSocket.Listener
    @Override public void onOpen(WebSocket webSocket) { webSocket.request(1); }
    @Override public CompletionStage<?> onText(WebSocket ws, CharSequence data, boolean last) {
        String s = data.toString();
        if (s.contains("PONG")) fire(Listener::onPong); else fire(l -> l.onAck(s));
        ws.request(1); return CompletableFuture.completedFuture(null);
    }
    @Override public CompletionStage<?> onBinary(WebSocket ws, ByteBuffer msg, boolean last) {
        byte[] bytes = new byte[msg.remaining()]; msg.get(bytes);
        try {
            PushDataV3ApiWrapper w = PushDataV3ApiWrapper.parseFrom(bytes);
            String symbol = w.getSymbol(); long sendTime = w.getSendTime();
            // User
            if (w.hasPrivateAccount()) fire(l -> l.onPrivateAccount(w.getPrivateAccount(), sendTime));
            else if (w.hasPrivateOrders()) fire(l -> l.onPrivateOrders(symbol, w.getPrivateOrders(), sendTime));
            else if (w.hasPrivateDeals()) fire(l -> l.onPrivateDeals(symbol, w.getPrivateDeals(), sendTime));
                // Market
            else if (w.hasPublicAggreDepths()) {
                fire(l -> l.onAggreDepths(symbol, w.getPublicAggreDepths(), sendTime));
            }
            else if (w.hasPublicIncreaseDepths()) fire(l -> l.onDepthInc(symbol, w.getPublicIncreaseDepths(), sendTime));
            else if (w.hasPublicLimitDepths()) fire(l -> l.onLimitDepth(symbol, w.getPublicLimitDepths(), sendTime));
            else if (w.hasPublicAggreBookTicker()) fire(l -> l.onBookTicker(symbol, w.getPublicAggreBookTicker(), sendTime));
            else if (w.hasPublicAggreDeals()) fire(l -> l.onDeals(symbol, w.getPublicAggreDeals(), sendTime));
            else if (w.hasPublicMiniTicker()) fire(l -> l.onMiniTicker(symbol, w.getPublicMiniTicker(), sendTime));
        } catch (InvalidProtocolBufferException e) {
            log.warn("✖️ PB parse: {}", e.toString()); fire(l -> l.onError(e));
        }
        ws.request(1); return CompletableFuture.completedFuture(null);
    }
    @Override public void onError(WebSocket ws, Throwable err) {
        log.warn("💥 WS error: {}", err.toString()); fire(l -> l.onError(err)); scheduleReconnect(1);
    }
    @Override public CompletionStage<?> onClose(WebSocket ws, int code, String reason) {
        log.info("🔒 WS closed: {} {}", code, reason); fire(l -> l.onClosed(code, reason)); scheduleReconnect(1);
        return CompletableFuture.completedFuture(null);
    }

    private void fire(Consumer<Listener> c) { for (Listener l : listeners) try { c.accept(l); } catch (Throwable ignore) {} }
}

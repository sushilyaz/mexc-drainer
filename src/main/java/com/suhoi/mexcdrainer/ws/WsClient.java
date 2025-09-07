package com.suhoi.mexcdrainer.ws;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import okhttp3.*;
import okio.ByteString; // ВАЖНО: ByteString из okio (OkHttp 4.x)

import java.io.Closeable;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Универсальный WS-клиент на OkHttp 4.12.0:
 * - Автопинг (app-level PING) с единственной задачей;
 * - Экспоненциальный реконнект (с ограничением);
 * - SUB/UNSUB по протоколу MEXC (JSON {method, params});
 * - Хук onOpenHook(...) — для ресабскрая после реконнекта.
 */
@Slf4j
public abstract class WsClient implements Closeable {

    protected final String wsUrl;
    protected final ObjectMapper om;

    private final OkHttpClient http = new OkHttpClient.Builder()
            .readTimeout(0, TimeUnit.MILLISECONDS) // бесконечное чтение
            .build();

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "mexc-ws-scheduler");
        t.setDaemon(true);
        return t;
    });

    private volatile WebSocket socket;
    private volatile boolean closed = false;

    // backoff
    private final AtomicLong backoffMs = new AtomicLong(200);
    private final long maxBackoffMs = 5_000;

    // ping
    private final int pingIntervalMs;
    private volatile ScheduledFuture<?> pingTask;

    protected WsClient(String wsUrl, ObjectMapper om, int pingIntervalMs) {
        this.wsUrl = wsUrl;
        this.om = om;
        this.pingIntervalMs = pingIntervalMs;
    }

    // Обязательные обработчики
    protected abstract void onText(String text);
    protected abstract void onBinary(byte[] bytes);

    // Хук на успешный open — удобно для ресабскрая
    protected void onOpenHook(WebSocket ws) { /* no-op */ }

    /** Установить соединение. initialParams — одноразовые подписки. */
    public synchronized void connect(List<String> initialParams) {
        if (closed) return;

        Request req = new Request.Builder().url(wsUrl).build();
        socket = http.newWebSocket(req, new WebSocketListener() {
            @Override public void onOpen(WebSocket ws, Response response) {
                log.info("[WS_OPEN] url={}", wsUrl);
                backoffMs.set(200);

                if (initialParams != null && !initialParams.isEmpty()) {
                    sendSub(initialParams);
                }

                // единственный app-level PING
                if (pingTask != null && !pingTask.isCancelled()) pingTask.cancel(false);
                pingTask = scheduler.scheduleAtFixedRate(WsClient.this::safePing,
                        pingIntervalMs, pingIntervalMs, TimeUnit.MILLISECONDS);

                onOpenHook(ws);
            }

            @Override public void onMessage(WebSocket ws, String text) {
                try { onText(text); } catch (Exception e) {
                    log.warn("[WS_ON_TEXT_ERR] {}", e.getMessage(), e);
                }
            }

            @Override public void onMessage(WebSocket ws, ByteString bytes) {
                try { onBinary(bytes.toByteArray()); } catch (Exception e) {
                    log.warn("[WS_ON_BIN_ERR] {}", e.getMessage(), e);
                }
            }

            @Override public void onFailure(WebSocket ws, Throwable t, Response r) {
                log.warn("[WS_FAIL] url={} err={}", wsUrl, t.toString());
                scheduleReconnect();
            }

            @Override public void onClosed(WebSocket ws, int code, String reason) {
                log.info("[WS_CLOSED] code={} reason={}", code, reason);
                scheduleReconnect();
            }
        });
    }

    // --- Протокол MEXC ---

    public synchronized void sendSub(List<String> params) {
        if (socket == null) return;
        try {
            var root = om.createObjectNode().put("method", "SUBSCRIPTION");
            var arr = root.putArray("params");
            for (String p : params) arr.add(p);
            socket.send(root.toString());
        } catch (Exception e) {
            log.warn("[WS_SUB_ERR] {}", e.getMessage(), e);
        }
    }

    public synchronized void sendUnsub(List<String> params) {
        if (socket == null) return;
        try {
            var root = om.createObjectNode().put("method", "UNSUBSCRIPTION");
            var arr = root.putArray("params");
            for (String p : params) arr.add(p);
            socket.send(root.toString());
        } catch (Exception e) {
            log.warn("[WS_UNSUB_ERR] {}", e.getMessage(), e);
        }
    }

    public synchronized void sendPing() {
        if (socket == null) return;
        try {
            socket.send(om.createObjectNode().put("method", "PING").toString());
        } catch (Exception e) {
            log.warn("[WS_PING_ERR] {}", e.getMessage(), e);
        }
    }
    private void safePing() { try { sendPing(); } catch (Exception ignored) {} }

    // --- Реконнект ---

    private void scheduleReconnect() {
        if (closed) return;
        long delay = backoffMs.get();
        long next = Math.min((long) (delay * 1.8), maxBackoffMs);
        backoffMs.set(next);
        // один вызов reconnect через текущий delay
        scheduler.schedule(this::reconnect, delay, TimeUnit.MILLISECONDS);
    }

    private synchronized void reconnect() {
        if (closed) return;
        try { if (socket != null) socket.cancel(); } catch (Exception ignored) {}
        log.info("[WS_RECONNECT] url={} delay={}ms", wsUrl, backoffMs.get());
        connect(null); // каналы восстановит наследник в onOpenHook(...)
    }

    // --- Закрытие ---

    @Override
    public synchronized void close() {
        closed = true;
        try { if (socket != null) socket.close(1000, "bye"); } catch (Exception ignored) {}
        if (pingTask != null) try { pingTask.cancel(true); } catch (Exception ignored) {}
        scheduler.shutdownNow();
    }
}

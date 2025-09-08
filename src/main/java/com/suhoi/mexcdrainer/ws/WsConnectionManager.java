package com.suhoi.mexcdrainer.ws;

import lombok.Builder;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class WsConnectionManager implements WebSocket.Listener, AutoCloseable {

    @FunctionalInterface
    public interface TextHandler { void onText(String text); }
    @FunctionalInterface
    public interface BinaryHandler { void onBinary(byte[] data); }
    @FunctionalInterface
    public interface OpenHandler { void onOpen(); }
    @FunctionalInterface
    public interface CloseHandler { void onClosed(int status, String reason); }
    @FunctionalInterface
    public interface ErrorHandler { void onError(Throwable t); }

    @Getter private final URI uri;
    private final HttpClient client;
    private final ScheduledExecutorService ses;
    private final AtomicBoolean connecting = new AtomicBoolean(false);
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private volatile WebSocket ws;

    private final TextHandler textHandler;
    private final BinaryHandler binaryHandler;
    private final OpenHandler openHandler;
    private final CloseHandler closeHandler;
    private final ErrorHandler errorHandler;

    private final int pingIntervalMs;
    private final int wsPingFrameIntervalMs;
    private final Set<String> subscriptions = ConcurrentHashMap.newKeySet();

    private ScheduledFuture<?> pingTask;
    private ScheduledFuture<?> wsPingTask;

    @Builder
    public WsConnectionManager(URI uri,
                               TextHandler textHandler,
                               BinaryHandler binaryHandler,
                               OpenHandler openHandler,
                               CloseHandler closeHandler,
                               ErrorHandler errorHandler,
                               int pingIntervalMs,
                               int wsPingFrameIntervalMs) {
        this.uri = uri;
        this.textHandler = textHandler;
        this.binaryHandler = binaryHandler;
        this.openHandler = openHandler;
        this.closeHandler = closeHandler;
        this.errorHandler = errorHandler;
        this.pingIntervalMs = Math.max(5000, pingIntervalMs);
        this.wsPingFrameIntervalMs = Math.max(5000, wsPingFrameIntervalMs);
        this.client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();
        this.ses = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "ws-" + uri.getHost());
            t.setDaemon(true);
            return t;
        });
    }

    public void connectWithRetry(int baseDelayMs, int maxDelayMs) {
        int delay = baseDelayMs;
        while (!closed.get()) {
            try {
                connectOnce();
                return;
            } catch (Exception e) {
                log.warn("WS connect failed: {}. Retry in {}ms", e.getMessage(), delay);
                sleep(delay);
                delay = Math.min(maxDelayMs, delay * 2);
            }
        }
    }

    private void connectOnce() {
        if (!connecting.compareAndSet(false, true)) return;
        try {
            log.info("WS connecting -> {}", uri);
            this.ws = client.newWebSocketBuilder()
                    .connectTimeout(Duration.ofSeconds(5))
                    .buildAsync(uri, this).join();
        } finally {
            connecting.set(false);
        }
    }

    public void close() {
        closed.set(true);
        try { if (pingTask != null) pingTask.cancel(true); } catch (Exception ignore) {}
        try { if (wsPingTask != null) wsPingTask.cancel(true); } catch (Exception ignore) {}
        try { if (ws != null) ws.sendClose(WebSocket.NORMAL_CLOSURE, "bye"); } catch (Exception ignore) {}
        try { ses.shutdownNow(); } catch (Exception ignore) {}
    }

    public void sendText(String s) {
        WebSocket w = ws;
        if (w != null) w.sendText(s, true);
    }

    public void sendBinary(byte[] bytes) {
        WebSocket w = ws;
        if (w != null) w.sendBinary(ByteBuffer.wrap(bytes), true);
    }

    /** JSON запрос подписки/отписки сохраняем для авто-ресаба. */
    public void rememberSubscription(String channel) {
        subscriptions.add(channel);
    }
    public void forgetSubscription(String channel) {
        subscriptions.remove(channel);
    }

    // Listener

    @Override public void onOpen(WebSocket webSocket) {
        log.info("WS opened: {}", uri);
        startKeepAlive();
        // Ре-подписка
        if (!subscriptions.isEmpty()) {
            subscriptions.forEach(ch -> sendText("""
                {"method":"SUBSCRIPTION","params":["%s"]}
                """.formatted(ch)));
        }
        if (openHandler != null) openHandler.onOpen();
        webSocket.request(1);
    }

    @Override public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
        if (textHandler != null) textHandler.onText(data.toString());
        webSocket.request(1);
        return null;
    }

    @Override public CompletionStage<?> onBinary(WebSocket webSocket, ByteBuffer data, boolean last) {
        if (binaryHandler != null) {
            byte[] bytes = new byte[data.remaining()];
            data.get(bytes);
            binaryHandler.onBinary(bytes);
        }
        webSocket.request(1);
        return null;
    }

    @Override public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
        log.warn("WS closed: {} code={} reason={}", uri, statusCode, reason);
        stopKeepAlive();
        if (!closed.get()) {
            if (closeHandler != null) closeHandler.onClosed(statusCode, reason);
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override public void onError(WebSocket webSocket, Throwable error) {
        log.error("WS error {}: {}", uri, error.toString(), error);
        if (errorHandler != null) errorHandler.onError(error);
    }

    private void startKeepAlive() {
        stopKeepAlive();
        pingTask = ses.scheduleAtFixedRate(() -> {
            try { sendText("{\"method\":\"PING\"}"); } catch (Exception ignore) {}
        }, pingIntervalMs, pingIntervalMs, TimeUnit.MILLISECONDS);

        wsPingTask = ses.scheduleAtFixedRate(() -> {
            try {
                if (ws != null) ws.sendPing(ByteBuffer.wrap(new byte[]{1}));
            } catch (Exception ignore) {}
        }, wsPingFrameIntervalMs, wsPingFrameIntervalMs, TimeUnit.MILLISECONDS);
    }

    private void stopKeepAlive() {
        try { if (pingTask != null) pingTask.cancel(true); } catch (Exception ignore) {}
        try { if (wsPingTask != null) wsPingTask.cancel(true); } catch (Exception ignore) {}
    }

    private static void sleep(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
    }
}

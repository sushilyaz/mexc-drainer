package com.suhoi.mexcdrainer.ws.user;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.suhoi.mexcdrainer.config.AppProperties;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.*;

/**
 * Менеджер приватных user-ws сессий по apiKey.
 * - По каждому apiKey держит ровно один UserDataWsService.
 * - Делает реф-каунт. Когда ref==0 — НЕ закрывает сразу, а ставит отложенное закрытие (idle close).
 * - Даёт API awaitFinal(apiKey, orderIdOrClientId, timeout).
 */
@Slf4j
@RequiredArgsConstructor
public class UserDataWsManager implements AutoCloseable {

    private final ObjectMapper om;
    private final ListenKeyClient listenKeyClient;
    private final AppProperties props;

    /** Активные сессии по apiKey. */
    private final Map<String, RefCounted> sessions = new ConcurrentHashMap<>();

    /** Исполнитель для таймаутов простоя (idle-close). */
    private final ScheduledExecutorService idleSes =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "mexc-userws-idle");
                t.setDaemon(true);
                return t;
            });

    /** Через сколько закрывать сессию при отсутствии референсов. */
    private static final long IDLE_CLOSE_MS = TimeUnit.MINUTES.toMillis(15);

    /**
     * Получить/создать сессию для apiKey и увеличить ref.
     * Возвращает handle, который нужно закрыть (try-with-resources приветствуется).
     * Сама сессия НЕ закрывается немедленно, а уходит в idle-close по таймеру.
     */
    public SessionHandle acquire(String apiKey, String secret) {
        Objects.requireNonNull(apiKey, "apiKey");
        Objects.requireNonNull(secret, "secret");

        RefCounted rc = sessions.compute(apiKey, (k, old) -> {
            if (old == null) {
                // создаём свежую сессию
                var s = new UserDataWsService(om, listenKeyClient);
                s.start(
                        apiKey,
                        secret,
                        props.getWs().getPrivateUrl(),
                        props.getWs().getPingIntervalMs(),
                        props.getWs().getWsPingFrameIntervalMs(),
                        props.getWs().getReconnect().getBaseDelayMs(),
                        props.getWs().getReconnect().getMaxDelayMs(),
                        props.getWs().getListenKeyKeepAliveMinutes()
                );
                log.info("[UserWS] started (apiKey=***) privateUrl={}", props.getWs().getPrivateUrl());
                return new RefCounted(s);
            } else {
                // отменяем запланированное idle-close, если было, и увеличиваем ref
                if (old.closeTask != null) {
                    boolean cancelled = old.closeTask.cancel(false);
                    if (cancelled) {
                        log.debug("[UserWS] idle-close cancelled (apiKey=***)");
                    }
                    old.closeTask = null;
                }
                old.inc();
                log.debug("[UserWS] acquire -> ref={} (apiKey=***)", old.ref);
                return old;
            }
        });

        return new SessionHandle(apiKey, this);
    }

    /**
     * Дождаться финального статуса ордера (FILLED/CANCELED/ PARTIALLY_CANCELED)
     * по orderId или clientOrderId (любой ключ сработает).
     */
    public UserDataWsService.OrderUpdate awaitFinal(String apiKey, String orderIdOrClientId, Duration timeout) {
        RefCounted rc = sessions.get(apiKey);
        if (rc == null) throw new IllegalStateException("No user-ws session for apiKey");
        return rc.s.awaitOrderFinal(orderIdOrClientId, timeout);
    }

    /** Внутренний вызов из SessionHandle.close(): уменьшить ref и, при 0, запланировать idle-close. */
    void release(String apiKey) {
        sessions.computeIfPresent(apiKey, (k, rc) -> {
            int r = rc.dec();
            log.debug("[UserWS] release -> ref={} (apiKey=***)", r);
            if (r == 0) {
                // планируем отложенное закрытие
                rc.closeTask = idleSes.schedule(() -> {
                    try {
                        log.info("[UserWS] idle-close firing (apiKey=***)");
                        rc.s.close();
                    } catch (Exception ignore) {
                    } finally {
                        sessions.remove(k);
                    }
                }, IDLE_CLOSE_MS, TimeUnit.MILLISECONDS);
            }
            return rc;
        });
    }

    @Override
    public void close() {
        // Закрываем все сессии без ожидания idle
        sessions.forEach((k, rc) -> {
            try {
                if (rc.closeTask != null) rc.closeTask.cancel(false);
            } catch (Exception ignore) {}
            try {
                rc.s.close();
            } catch (Exception ignore) {}
        });
        sessions.clear();

        try {
            idleSes.shutdownNow();
        } catch (Exception ignore) {}
    }

    /* ================= helpers ================= */

    private static final class RefCounted {
        final UserDataWsService s;
        int ref = 1;
        ScheduledFuture<?> closeTask;

        RefCounted(UserDataWsService s) { this.s = s; }
        void inc() { ref++; }
        int dec() { return --ref; }
    }

    /** RAII-дескриптор, чтобы освобождать acquire() в try-with-resources. */
    @Value
    public static class SessionHandle implements AutoCloseable {
        String apiKey;
        UserDataWsManager parent;

        @Override
        public void close() {
            parent.release(apiKey);
        }
    }
}

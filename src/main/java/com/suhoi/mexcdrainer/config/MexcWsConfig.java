package com.suhoi.mexcdrainer.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.suhoi.mexcdrainer.ws.market.MarketWsService;
import com.suhoi.mexcdrainer.ws.user.ListenKeyClient;
import com.suhoi.mexcdrainer.ws.user.UserDataWsManager;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestTemplate;

/**
 * Конфиг бинoв для WS-слоя MEXC.
 * - MarketWsService: одиночный инстанс для всех символов.
 * - UserDataWsManager: реестр user-ws по apiKey (А и B — отдельные сессии).
 * - ListenKeyClient: REST-клиент listenKey (POST/PUT/DELETE).
 */
@Configuration
@RequiredArgsConstructor
public class MexcWsConfig {

    private final AppProperties props;

    @Bean
    public MarketWsService marketWsService() {
        var s = new MarketWsService();
        var ws = props.getWs();
        // ВАЖНО: запускаем сразу, чтобы к моменту первой подписки ws != null
        s.start(
                ws.getPublicUrl(),
                ws.getBookTickerInterval(),
                ws.getDepthInterval(),
                ws.getPingIntervalMs(),
                ws.getWsPingFrameIntervalMs(),
                ws.getReconnect().getBaseDelayMs(),
                ws.getReconnect().getMaxDelayMs()
        );
        return s;
    }

    @Bean
    public ListenKeyClient listenKeyClient(RestTemplate rt, ObjectMapper om) {
        return new ListenKeyClient(rt, om);
    }

    @Bean
    public UserDataWsManager userDataWsManager(ObjectMapper om, ListenKeyClient lkc) {
        // менеджер сам создаёт сессии под каждый apiKey по требованию
        return new UserDataWsManager(om, lkc, props);
    }
}

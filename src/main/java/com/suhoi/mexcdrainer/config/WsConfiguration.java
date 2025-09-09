package com.suhoi.mexcdrainer.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.suhoi.mexcdrainer.service.MexcTradeService;
import com.suhoi.mexcdrainer.ws.ListenKeyClient;
import com.suhoi.mexcdrainer.ws.MexcWsClient;
import com.suhoi.mexcdrainer.ws.facade.BalanceWsFacade;
import com.suhoi.mexcdrainer.ws.facade.MarketBookFacade;
import com.suhoi.mexcdrainer.ws.facade.UserOrderAwaiter;
import com.suhoi.mexcdrainer.ws.facade.WsEnsureFacade;
import com.suhoi.mexcdrainer.ws.market.MarketWsService;
import com.suhoi.mexcdrainer.ws.market.OrderBookRepository;
import com.suhoi.mexcdrainer.ws.user.BalanceTracker;
import com.suhoi.mexcdrainer.ws.user.OrderStateTracker;
import com.suhoi.mexcdrainer.ws.user.OwnOrdersRegistry;
import com.suhoi.mexcdrainer.ws.user.UserWsManager;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestTemplate;

@Configuration
@EnableConfigurationProperties({WsMarketProperties.class, WsUserProperties.class})
@RequiredArgsConstructor
public class WsConfiguration {

    private final WsMarketProperties marketProps;

    @Bean
    public RestTemplate restTemplate() { // нужен для ListenKeyClient
        return new RestTemplate();
    }

    @Bean
    public ListenKeyClient listenKeyClient(RestTemplate rt, ObjectMapper om) {
        return new ListenKeyClient(rt, om); // тот же класс из мини-проекта
    }

    @Bean(initMethod = "connect", destroyMethod = "close")
    public MexcWsClient marketWsClient() {
        return new MexcWsClient(
                marketProps.getEndpoint(),
                marketProps.getPingPeriodSec(),
                marketProps.getRotateAfterHours(),
                marketProps.getReconnectBackoffBaseSec(),
                marketProps.getReconnectBackoffMaxSec());
    }

    @Bean
    public OrderBookRepository orderBookRepository() {
        return new OrderBookRepository(marketProps);
    }

    @Bean(initMethod = "start", destroyMethod = "stop")
    public MarketWsService marketWsService(MexcWsClient marketWsClient,
                                           OrderBookRepository repo) {
        return new MarketWsService(marketWsClient, repo, marketProps);
    }

    @Bean
    public OwnOrdersRegistry ownOrdersRegistry() {
        return new OwnOrdersRegistry();
    }

    @Bean
    public OrderStateTracker orderStateTracker() {
        return new OrderStateTracker();
    }

    @Bean
    public BalanceTracker balanceTracker() {
        return new BalanceTracker();
    }

    @Bean
    public MarketBookFacade marketBookFacade(OrderBookRepository repo, OwnOrdersRegistry ownOrdersRegistry, WsMarketProperties props) {
        return new MarketBookFacade(repo, ownOrdersRegistry, props);
    }

    @Bean
    public UserOrderAwaiter userOrderAwaiter(OrderStateTracker stateTracker, WsUserProperties userProps) {
        return new UserOrderAwaiter(stateTracker, userProps);
    }

    @Bean
    public BalanceWsFacade balanceWsFacade(BalanceTracker balanceTracker, WsUserProperties userProps) {
        return new BalanceWsFacade(balanceTracker, userProps);
    }

    @Bean
    public WsEnsureFacade wsEnsureFacade(MarketBookFacade book, MexcTradeService mexcTradeService) {
        return new WsEnsureFacade(book, mexcTradeService);
    }

    @Bean(initMethod = "start", destroyMethod = "stop")
    public UserWsManager userWsManager(WsMarketProperties wsMarketProps,
                                       WsUserProperties wsUserProps,
                                       ListenKeyClient lkClient,
                                       OrderStateTracker stateTracker,
                                       BalanceTracker balanceTracker,
                                       OwnOrdersRegistry ownOrdersRegistry) {
        return new UserWsManager(wsMarketProps, wsUserProps, lkClient, stateTracker, balanceTracker, ownOrdersRegistry);
    }
}

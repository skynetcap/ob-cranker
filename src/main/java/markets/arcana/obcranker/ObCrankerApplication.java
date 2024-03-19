package markets.arcana.obcranker;

import com.google.common.io.Resources;
import com.mmorrell.openbook.manager.OpenBookManager;
import com.mmorrell.openbook.model.OpenBookMarket;
import lombok.extern.slf4j.Slf4j;
import org.p2p.solanaj.core.Account;
import org.p2p.solanaj.core.PublicKey;
import org.p2p.solanaj.rpc.RpcClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@SpringBootApplication
@Slf4j
public class ObCrankerApplication {

    @Value("${application.endpoint}")
    private String endpoint;

    @Value("${application.privateKey}")
    private String privateKeyFileName;

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(8);

    public static void main(String[] args) {
        SpringApplication.run(ObCrankerApplication.class, args);
    }

    @EventListener(ApplicationReadyEvent.class)
    public void crankEventHeapLoop() {
        OpenBookManager manager = new OpenBookManager(new RpcClient(endpoint, 10));

        Account tradingAccount = null;
        try {
            tradingAccount = Account.fromJson(
                    Resources.toString(Resources.getResource(privateKeyFileName), Charset.defaultCharset()));
        } catch (IOException e) {
            log.error("Error reading private key: {}", e.getMessage());
        }
        Account finalTradingAccount = tradingAccount;

        log.info("Crank wallet: {}", finalTradingAccount.getPublicKey().toBase58());
        log.info("RPC: {}", endpoint);

        scheduler.scheduleAtFixedRate(() -> {
            try {
                // SOL/USDC
                PublicKey marketId = PublicKey.valueOf("CFSMrBssNG8Ud1edW59jNLnq2cwrQ9uY5cM3wXmqRJj3");
                Optional<String> transactionId = manager.consumeEvents(
                        finalTradingAccount,
                        marketId,
                        8,
                        "Cranked by arcana.markets \uD83E\uDDD9"
                );

                if (transactionId.isPresent()) {
                    log.info("Cranked primary events: {}", transactionId.get());
                } else {
                    log.info("No primary events found to consume.");
                }
            } catch (Exception ex) {
                log.error("Error cranking SOL/USDC: {}", ex.getMessage(), ex);
            }

        }, 0, 2000, TimeUnit.MILLISECONDS);

        scheduler.scheduleAtFixedRate(() -> {
            try {
                manager.cacheMarkets();
                for (OpenBookMarket market : manager.getOpenBookMarkets()) {
                    Optional<String> transactionId = manager.consumeEvents(
                            finalTradingAccount,
                            market.getMarketId(),
                            8,
                            "Cranked by arcana.markets \uD83E\uDDD9"
                    );

                    if (transactionId.isPresent()) {
                        log.info("Cranked events [{}]: {}", market.getName(), transactionId.get());
                    } else {
                        log.info("No events found to consume.");
                    }
                    try {
                        Thread.sleep(250);
                    } catch (InterruptedException e) {
                        log.error("Error consuming: {}", e.getMessage());
                    }
                }
            } catch (Exception ex) {
                log.error("Error caching/cranking markets: {}", ex.getMessage(), ex);
            }

        }, 0, 30, TimeUnit.SECONDS);
    }
}
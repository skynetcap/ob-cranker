package markets.arcana.obcranker;

import com.google.common.io.Resources;
import com.mmorrell.openbook.manager.OpenBookManager;
import lombok.extern.slf4j.Slf4j;
import org.p2p.solanaj.core.Account;
import org.p2p.solanaj.core.PublicKey;
import org.p2p.solanaj.rpc.RpcClient;
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

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    public static void main(String[] args) {
        SpringApplication.run(ObCrankerApplication.class, args);
    }

    @EventListener(ApplicationReadyEvent.class)
    public void crankEventHeapLoop() throws InterruptedException, IOException {
        OpenBookManager manager = new OpenBookManager(new RpcClient("https://mainnet.helius-rpc" +
                ".com/?api-key=a778b653-bdd6-41bc-8cda-0c7377faf1dd"));

        scheduler.scheduleAtFixedRate(() -> {
            // SOL/USDC
            PublicKey marketId = PublicKey.valueOf("C3YPL3kYCSYKsmHcHrPWx1632GUXGqi2yMXJbfeCc57q");
            Account tradingAccount = null;
            try {
                tradingAccount = Account.fromJson(
                        Resources.toString(Resources.getResource("mikeDBaJgkicqhZcoYDBB4dRwZFFJCThtWCYD7A9FAH.json"), Charset.defaultCharset()));
            } catch (IOException e) {
                e.printStackTrace();
            }
            Optional<String> transactionId = manager.consumeEvents(
                    tradingAccount,
                    marketId,
                    8
            );

            if (transactionId.isPresent()) {
                log.info("Cranked events: {}", transactionId.get());
            } else {
                log.info("No events found to consume.");
            }

        }, 0, 5, TimeUnit.SECONDS);
    }
}
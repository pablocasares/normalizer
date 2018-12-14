package io.wizzie.normalizer;

import io.wizzie.bootstrapper.builder.Config;
import io.wizzie.normalizer.builder.Builder;
import io.wizzie.normalizer.logo.LogoPrinter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Normalizer {
    private static final Logger log = LoggerFactory.getLogger(Normalizer.class);

    public static void main(String[] args) throws Exception {
        LogoPrinter.PrintLogo();
        if (args.length == 1) {
            log.info("Starting Normalizer engine.");
            Config config = new Config(args[0]);
            Builder builder = new Builder(config.clone());

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    builder.close();
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }

                log.info("Stopped Normalizer engine.");
            }));

        } else {
            log.error("Execute: java -cp ${JAR_PATH} io.wizzie.normalizer.Normalizer <config_file>");
        }
    }
}

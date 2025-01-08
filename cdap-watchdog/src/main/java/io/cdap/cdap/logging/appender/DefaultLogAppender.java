package io.cdap.cdap.logging.appender;

import com.google.inject.Inject;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.logging.appender.loader.ILogAppenderExtensionLoader;
import io.cdap.cdap.logs.ILogAppender;
import java.io.IOException;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultLogAppender extends LogAppender {

  public static final String PROVIDER = "stackdriver";
  private static final String APPENDER_NAME = "DefaultLogAppender";
  private static final Logger LOG = LoggerFactory.getLogger(DefaultLogAppender.class);

  private final CConfiguration cConf;
  private ILogAppender appender;

  @Inject
  public DefaultLogAppender(CConfiguration cConf) {
    this.cConf = cConf;
    setName(APPENDER_NAME);
  }

  @Override
  public void start() {
    appender = loadCloudLogAppender();
    Optional.ofNullable(this.appender)
        .ifPresent(
            appender -> appender.initialize(new DefaultILogAppenderContext(cConf, PROVIDER)));
    super.start();
    LOG.info("Successfully started {}", APPENDER_NAME);
  }

  @Override
  public void stop() {
    super.stop();

    Optional.ofNullable(this.appender)
        .ifPresent(appender -> {
          try {
            appender.close();
            LOG.info("Successfully stopped {}", APPENDER_NAME);
          } catch (IOException e) {
            LOG.warn("Failed to stop {}", APPENDER_NAME, e);
          }
        });
  }

  @Override
  protected void appendEvent(LogMessage logMessage) {
    logMessage.prepareForDeferredProcessing();
    logMessage.getCallerData();
    Optional.ofNullable(this.appender)
        .ifPresent(
            appender -> appender.appendEvent(logMessage));
  }

  /**
   * Loads the cloud log appender using the extension loader.
   *
   * @return the CloudLogAppenderProvider instance
   */
  private ILogAppender loadCloudLogAppender() {
    ILogAppender appender = new ILogAppenderExtensionLoader(cConf).get(
        DefaultLogAppender.PROVIDER);
    if (appender == null) {
      LOG.info("Failed to load cloud log appender provider: {}", PROVIDER);
    }
    return appender;
  }
}

package com.continuuity.log.appender.loggly;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import org.junit.Test;
import org.junit.Before;
import org.junit.After;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.InetAddress;
import java.net.URL;

/**
 * Testing the loggly log appender.
 */
public class LogglyAppenderTest {
  private LoggerContext context;

  @Before
  public void setUp() {
    context = (LoggerContext) LoggerFactory.getILoggerFactory();
  }

  @After
  public void tearDown() {
    context = null;
  }

  @Test
  public void configuredWithFile() throws Exception {
    final URL url = getClass().getClassLoader().getResource("loggly-test.xml");
    context.reset();
    final JoranConfigurator configurator = new JoranConfigurator();
    configurator.setContext(this.context);
    configurator.doConfigure(new File(url.getFile()));
    final Logger root = context.getLogger(getClass());
    root.error("Message 1");
    root.error("Message 2");
    root.error("Message 3");
  }
}

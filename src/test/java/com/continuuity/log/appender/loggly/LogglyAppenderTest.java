package com.continuuity.log.appender.loggly;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import com.continuuity.log.appender.log4j.*;
import com.continuuity.log.appender.log4j.LogglyAppender;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.PropertyConfigurator;
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
  public void configuredLogbackWithFile() throws Exception {
    final URL url = getClass().getClassLoader().getResource("loggly-test.xml");
    context.reset();
    final JoranConfigurator configurator = new JoranConfigurator();
    configurator.setContext(this.context);
    configurator.doConfigure(new File(url.getFile()));
    final Logger log = context.getLogger(getClass());
    log.error("Message 1");
    log.error("Message 2");
    log.error("Message 3");
    Thread.sleep(10000);
  }

  @Test
  public void configuredLog4jWithFile() throws Exception {
    final URL url = getClass().getClassLoader().getResource("log4j.properties");
    PropertyConfigurator.configure(url);
    org.apache.log4j.Logger log = org.apache.log4j.Logger.getLogger("test");
    log.error("Log4j Message 1");
    log.error("Log4j Message 2");
    log.error("Log4j Message 3");
    Thread.sleep(10000);
  }
}

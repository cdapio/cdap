/*
 * Copyright © 2026 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.cdap.logging.pipeline;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.filter.Filter;
import ch.qos.logback.core.read.ListAppender;
import ch.qos.logback.core.spi.FilterReply;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.io.Syncable;
import io.cdap.cdap.logging.appender.ForwardingAppender;
import io.cdap.cdap.logging.framework.InvalidPipelineException;
import io.cdap.cdap.logging.framework.LogPipelineLoader;
import io.cdap.cdap.logging.framework.LogPipelineSpecification;
import java.io.File;
import java.io.Flushable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Map;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests for {@link LogPipelineConfigurator} and {@link LogPipelineLoader} on Logback 1.6.
 */
public class LogPipelineConfiguratorTest {

  @Rule
  public final TemporaryFolder tmpFolder = new TemporaryFolder();

  /**
   * Test appender implementing {@link Flushable} (without {@link Syncable}) to verify wrapping.
   */
  public static final class FlushTrackingAppender extends ListAppender<ILoggingEvent>
      implements Flushable {

    private int flushCount;
    private boolean failNextFlush;

    @Override
    public void flush() throws IOException {
      if (failNextFlush) {
        failNextFlush = false;
        throw new IOException("simulated flush failure");
      }
      flushCount++;
    }
  }

  /**
   * Test appender whose {@link #start()} throws a {@link RuntimeException}.
   */
  public static final class FailingStartAppender extends ListAppender<ILoggingEvent> {

    @Override
    public void start() {
      throw new IllegalStateException("simulated appender start failure");
    }
  }

  /**
   * Test filter whose {@link #start()} throws a {@link RuntimeException}.
   */
  public static final class FailingStartFilter extends Filter<ILoggingEvent> {

    @Override
    public void start() {
      throw new IllegalStateException("simulated filter start failure");
    }

    @Override
    public FilterReply decide(ILoggingEvent event) {
      return FilterReply.NEUTRAL;
    }
  }

  @Test
  public void testContextPropertiesIncludeAndAppenderWrapping() throws Exception {
    File included = tmpFolder.newFile("included.xml");
    Files.write(included.toPath(), (
        "<included>"
        + "  <appender name=\"${incName}\" class=\"" + FlushTrackingAppender.class.getName() + "\"/>"
        + "  <appender name=\"unused\" class=\"" + FlushTrackingAppender.class.getName() + "\"/>"
        + "</included>").getBytes(StandardCharsets.UTF_8));

    File pipelineXml = tmpFolder.newFile("custom.xml");
    Files.write(pipelineXml.toPath(), (
        "<configuration>"
        + "  <contextName>custom</contextName>"
        + "  <property name=\"log.process.pipeline.buffer.size\" value=\"2048\"/>"
        + "  <include file=\"" + included.getAbsolutePath() + "\"/>"
        + "  <root level=\"INFO\"><appender-ref ref=\"${incName}\"/></root>"
        + "</configuration>").getBytes(StandardCharsets.UTF_8));

    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.Logging.PIPELINE_CONFIG_DIR, tmpFolder.getRoot().getAbsolutePath());
    cConf.set("log.pipeline.custom.incName", "wrapped");
    cConf.set("log.pipeline.custom.extraKey", "extraVal");

    Map<String, LogPipelineSpecification<LoggerContext>> specs =
        new LogPipelineLoader(cConf).load(() -> {
          LoggerContext context = new LoggerContext();
          context.putObject(Constants.Logging.PIPELINE_VALIDATION, Boolean.TRUE);
          return context;
        });
    LogPipelineSpecification<LoggerContext> spec = specs.get("custom");
    Assert.assertNotNull(spec);
    try {
      Assert.assertEquals("extraVal", spec.getContext().getProperty("extraKey"));
      Assert.assertEquals(2048L, spec.getConf().getLong(Constants.Logging.PIPELINE_BUFFER_SIZE));

      Appender<ILoggingEvent> attached =
          spec.getContext().getLogger(Logger.ROOT_LOGGER_NAME).getAppender("wrapped");
      Assert.assertTrue(attached instanceof ForwardingAppender);
      Assert.assertTrue(attached instanceof Syncable);
      FlushTrackingAppender delegate =
          (FlushTrackingAppender) ((ForwardingAppender<ILoggingEvent>) attached).getDelegate();

      delegate.failNextFlush = true;
      try {
        ((Syncable) attached).sync();
        Assert.fail("Expected IOException");
      } catch (IOException expected) {
        // Next doAppend retries the failed sync/flush
      }
      spec.getContext().getLogger("test").info("hello");
      Assert.assertEquals(1, delegate.flushCount);
      Assert.assertEquals(1, delegate.list.size());
    } finally {
      specs.values().forEach(s -> s.getContext().stop());
    }
  }

  @Test
  public void testUnreferencedInvalidAppenderFailsValidation() throws Exception {
    File pipelineXml = tmpFolder.newFile("unreferenced.xml");
    Files.write(pipelineXml.toPath(), (
        "<configuration>"
        + "  <appender name=\"unused\" class=\"no.such.MissingAppender\"/>"
        + "</configuration>").getBytes(StandardCharsets.UTF_8));

    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.Logging.PIPELINE_CONFIG_DIR, tmpFolder.getRoot().getAbsolutePath());
    try {
      new LogPipelineLoader(cConf).validate();
      Assert.fail("Expected InvalidPipelineException for an unreferenced invalid appender");
    } catch (InvalidPipelineException expected) {
      // expected, same as Logback 1.2
    }
  }

  @Test
  public void testAppenderStartRuntimeExceptionFailsValidation() throws Exception {
    File pipelineXml = tmpFolder.newFile("failing-appender.xml");
    Files.write(pipelineXml.toPath(), (
        "<configuration>"
        + "  <appender name=\"failing\" class=\"" + FailingStartAppender.class.getName() + "\"/>"
        + "  <root level=\"INFO\"><appender-ref ref=\"failing\"/></root>"
        + "</configuration>").getBytes(StandardCharsets.UTF_8));

    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.Logging.PIPELINE_CONFIG_DIR, tmpFolder.getRoot().getAbsolutePath());
    try {
      new LogPipelineLoader(cConf).validate();
      Assert.fail("Expected InvalidPipelineException when Appender.start() throws RuntimeException");
    } catch (InvalidPipelineException expected) {
      Assert.assertNotNull(expected.getCause());
      Assert.assertTrue(expected.getCause().getMessage().contains("Failed to configure appender failing"));
      Assert.assertTrue(expected.getCause().getCause() instanceof IllegalStateException);
      Assert.assertEquals("simulated appender start failure",
          expected.getCause().getCause().getMessage());
    }
  }

  @Test
  public void testNestedFilterStartRuntimeExceptionFailsValidation() throws Exception {
    File pipelineXml = tmpFolder.newFile("failing-filter.xml");
    Files.write(pipelineXml.toPath(), (
        "<configuration>"
        + "  <appender name=\"filtered\" class=\"" + FlushTrackingAppender.class.getName() + "\">"
        + "    <filter class=\"" + FailingStartFilter.class.getName() + "\"/>"
        + "  </appender>"
        + "  <root level=\"INFO\"><appender-ref ref=\"filtered\"/></root>"
        + "</configuration>").getBytes(StandardCharsets.UTF_8));

    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.Logging.PIPELINE_CONFIG_DIR, tmpFolder.getRoot().getAbsolutePath());
    try {
      new LogPipelineLoader(cConf).validate();
      Assert.fail("Expected InvalidPipelineException when nested Filter.start() throws RuntimeException");
    } catch (InvalidPipelineException expected) {
      Assert.assertNotNull(expected.getCause());
      Assert.assertTrue(expected.getCause().getMessage().startsWith("Configuration failed for "));
      Assert.assertTrue(expected.getCause().getCause() instanceof IllegalStateException);
      Assert.assertEquals("simulated filter start failure",
          expected.getCause().getCause().getMessage());
    }
  }
}

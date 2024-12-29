/*
 * Copyright © 2025 Cask Data, Inc.
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

package io.cdap.cdap.logging.appender;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.logging.publisher.DefaultLogPublisherContext;
import io.cdap.cdap.logging.publisher.LogPublisherExtensionLoader;
import io.cdap.cdap.spi.logs.LogPublisher;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Unit tests for {@link ExtensionLogAppender} class.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ExtensionLogAppender.class, LogPublisherExtensionLoader.class})
public class ExtensionLogAppenderTest {

  @Mock
  private LogPublisherExtensionLoader mockLoader;

  @Mock
  private CConfiguration mockConf;

  @Mock
  private LogMessage mockLogMessage;

  @Mock
  private LogPublisher mockLogPublisher;

  private ExtensionLogAppender appender;

  @Before
  public void setup() throws Exception {
    MockitoAnnotations.openMocks(this);
    when(mockConf.get(Constants.Logging.LOG_PUBLISHER_PROVIDER)).thenReturn("testProvider");
    when(mockConf.getBoolean(Constants.Logging.LOG_PUBLISHER_ENABLED)).thenReturn(true);
    PowerMockito.whenNew(LogPublisherExtensionLoader.class).withArguments(mockConf)
        .thenReturn(mockLoader);
    appender = new ExtensionLogAppender(mockConf);
  }

  @Test
  public void testStartInitializesLogPublisher() {
    when(mockLoader.get("testProvider")).thenReturn(mockLogPublisher);
    doNothing().when(mockLogPublisher).initialize(any());

    appender.start();

    verify(mockLogPublisher, times(1)).initialize(any(DefaultLogPublisherContext.class));
    assertTrue(appender.isStarted());
  }

  @Test
  public void testStopClosesLogPublisher() {
    when(mockLoader.get("testProvider")).thenReturn(mockLogPublisher);
    doNothing().when(mockLogPublisher).close();

    appender.start();
    appender.stop();

    verify(mockLogPublisher, times(1)).close();
    assertFalse(appender.isStarted());
  }

  @Test
  public void testAppendEventDelegatesToLogPublisher() {
    when(mockLoader.get("testProvider")).thenReturn(mockLogPublisher);

    appender.start();

    doNothing().when(mockLogMessage).prepareForDeferredProcessing();
    when(mockLogMessage.getCallerData()).thenReturn(null);
    doNothing().when(mockLogPublisher).publish(mockLogMessage);

    appender.appendEvent(mockLogMessage);

    verify(mockLogMessage, times(1)).prepareForDeferredProcessing();
    verify(mockLogMessage, times(1)).getCallerData();
    verify(mockLogPublisher, times(1)).publish(mockLogMessage);
  }

  @Test(expected = RuntimeException.class)
  public void testLoadLogPublisherThrowsExceptionWhenProviderNotFound() {
    when(mockLoader.get("testProvider")).thenReturn(null);
    appender.start();
  }

  @Test
  public void testLoadLogPublisherReturnsNoOpWhenDisabled() {
    when(mockConf.getBoolean(Constants.Logging.LOG_PUBLISHER_ENABLED)).thenReturn(false);

    appender.start();

    assertEquals("NoOpPublisher", appender.publisher.get().getName());
  }
}

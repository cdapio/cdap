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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import ch.qos.logback.core.Context;
import java.util.Arrays;
import java.util.Collections;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * Tests for {@link CompositeLogAppender}
 */
public class CompositeLogAppenderTest {

  @Mock
  private LogAppender mockAppender1;

  @Mock
  private LogAppender mockAppender2;

  private CompositeLogAppender compositeLogAppender;

  @Before
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    compositeLogAppender = new CompositeLogAppender(
        Arrays.asList(mockAppender1, mockAppender2));
  }

  @Test
  public void testStart() {
    doNothing().when(mockAppender1).start();
    doNothing().when(mockAppender2).start();

    compositeLogAppender.start();

    assertTrue("CompositeLogAppender should be started", compositeLogAppender.isStarted());
    verify(mockAppender1, times(1)).start();
    verify(mockAppender2, times(1)).start();
  }

  @Test
  public void testStop() {
    doNothing().when(mockAppender1).stop();
    doNothing().when(mockAppender2).stop();

    compositeLogAppender.stop();

    assertFalse("CompositeLogAppender should be stopped", compositeLogAppender.isStarted());
    verify(mockAppender1, times(1)).stop();
    verify(mockAppender2, times(1)).stop();
  }

  @Test
  public void testAppendEvent() throws Exception {
    LogMessage logMessage = mock(LogMessage.class);
    doNothing().when(mockAppender1).appendEvent(logMessage);
    doNothing().when(mockAppender2).appendEvent(logMessage);

    compositeLogAppender.appendEvent(logMessage);

    verify(mockAppender1, times(1)).appendEvent(logMessage);
    verify(mockAppender2, times(1)).appendEvent(logMessage);
  }

  @Test
  public void testSetContext() {
    Context context = mock(Context.class);

    compositeLogAppender.setContext(context);

    verify(mockAppender1, times(1)).setContext(context);
    verify(mockAppender2, times(1)).setContext(context);
  }

  @Test(expected = RuntimeException.class)
  public void testStartWhenOneFails() {
    doThrow(new RuntimeException("Start failed")).when(mockAppender1).start();
    doNothing().when(mockAppender2).start();

    compositeLogAppender.start();

    verify(mockAppender1, times(1)).start();
    verify(mockAppender2, times(1)).start();
  }

  @Test(expected = RuntimeException.class)
  public void testStopWhenOneFails() {
    doThrow(new RuntimeException("Stop failed")).when(mockAppender1).stop();
    doNothing().when(mockAppender2).stop();

    compositeLogAppender.stop();

    assertFalse("CompositeLogAppender should be stopped", compositeLogAppender.isStarted());
    verify(mockAppender1, times(1)).stop();
    verify(mockAppender2, times(1)).stop();
  }

  @Test(expected = RuntimeException.class)
  public void testAppendEventWhenOneFails() {
    LogMessage logMessage = mock(LogMessage.class);
    doThrow(new RuntimeException("Append failed")).when(mockAppender1).appendEvent(logMessage);
    doNothing().when(mockAppender2).appendEvent(logMessage);

    compositeLogAppender.appendEvent(logMessage);

    verify(mockAppender1, times(1)).appendEvent(logMessage);
    verify(mockAppender2, times(1)).appendEvent(logMessage);
  }

  @Test(expected = RuntimeException.class)
  public void testSetContextWhenOneFails() {
    Context context = mock(Context.class);
    doThrow(new RuntimeException("Set context failed")).when(mockAppender1).setContext(context);
    doNothing().when(mockAppender2).setContext(context);

    compositeLogAppender.setContext(context);

    verify(mockAppender1, times(1)).setContext(context);
    verify(mockAppender2, times(1)).setContext(context);
  }

  @Test
  public void testNoAppenders() {
    compositeLogAppender = new CompositeLogAppender(Collections.emptyList());
    compositeLogAppender.start();
    assertTrue("CompositeLogAppender should be started", compositeLogAppender.isStarted());

    compositeLogAppender.stop();
    assertFalse("CompositeLogAppender should be stopped", compositeLogAppender.isStarted());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNullAppenders() {
    compositeLogAppender = new CompositeLogAppender(null);
    assertNull("Appender name should not be set", compositeLogAppender.getName());
  }
}

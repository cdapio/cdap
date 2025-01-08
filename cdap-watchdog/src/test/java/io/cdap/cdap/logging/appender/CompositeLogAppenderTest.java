package io.cdap.cdap.logging.appender;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ch.qos.logback.core.Context;
import io.cdap.cdap.logging.framework.local.LocalLogAppender;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class CompositeLogAppenderTest {

  @Mock
  private LogAppender mockAppender1;

  @Mock
  private LogAppender mockAppender2;

  @Mock
  private LocalLogAppender mockLocalAppender;

  private CompositeLogAppender compositeLogAppender;

  @Before
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    compositeLogAppender = new CompositeLogAppender(
        Arrays.asList(mockAppender1, mockAppender2, mockLocalAppender));
  }

  @Test
  public void testStart() {
    doNothing().when(mockAppender1).start();
    doNothing().when(mockAppender2).start();

    assertDoesNotThrow(compositeLogAppender::start);

    verify(mockAppender1, times(1)).start();
    verify(mockAppender2, times(1)).start();
  }

  @Test
  public void testStop() {
    doNothing().when(mockAppender1).stop();
    doNothing().when(mockAppender2).stop();

    assertDoesNotThrow(compositeLogAppender::stop);

    verify(mockAppender1, times(1)).stop();
    verify(mockAppender2, times(1)).stop();
  }

  @Test
  public void testAppendEvent() {
    LogMessage logMessage = mock(LogMessage.class);
    doNothing().when(mockAppender1).appendEvent(logMessage);
    doNothing().when(mockAppender2).appendEvent(logMessage);

    assertDoesNotThrow(() -> compositeLogAppender.appendEvent(logMessage));

    verify(mockAppender1, times(1)).appendEvent(logMessage);
    verify(mockAppender2, times(1)).appendEvent(logMessage);
  }

  @Test
  public void testSetContext() {
    Context context = mock(Context.class);

    assertDoesNotThrow(() -> compositeLogAppender.setContext(context));

    verify(mockAppender1, times(1)).setContext(context);
    verify(mockAppender2, times(1)).setContext(context);
  }

  @Test
  public void testShouldSkipLogging() {
    AtomicReference<Set<Thread>> mockPipelineThreads = new AtomicReference<>(
        Collections.singleton(Thread.currentThread()));
    when(mockLocalAppender.getPipelineThreads()).thenReturn(mockPipelineThreads);

    boolean result = compositeLogAppender.shouldSkipLogging();

    verify(mockLocalAppender, times(1)).getPipelineThreads();
    assert result;
  }

  @Test
  public void testShouldNotSkipLogging() {
    AtomicReference<Set<Thread>> mockPipelineThreads = new AtomicReference<>(
        Collections.singleton(new Thread("MockThread")));
    when(mockLocalAppender.getPipelineThreads()).thenReturn(mockPipelineThreads);

    boolean result = compositeLogAppender.shouldSkipLogging();

    verify(mockLocalAppender, times(1)).getPipelineThreads();
    assert !result;
  }

  @Test
  public void testSafelyExecuteHandlesException() {
    doThrow(new RuntimeException("Mock exception")).when(mockAppender1).start();

    assertDoesNotThrow(() -> compositeLogAppender.start());

    verify(mockAppender1, times(1)).start();
    verify(mockAppender2, times(1)).start();
  }
}

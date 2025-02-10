/*
 * Copyright © 2024 Cask Data, Inc.
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

package io.cdap.cdap.logging;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.IThrowableProxy;
import ch.qos.logback.classic.spi.StackTraceElementProxy;
import com.google.common.collect.Iterators;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.api.exception.ErrorCategory.ErrorCategoryEnum;
import io.cdap.cdap.api.exception.ErrorType;
import io.cdap.cdap.api.exception.ProgramFailureException;
import io.cdap.cdap.api.exception.WrappedStageException;
import io.cdap.cdap.api.metrics.MetricValue;
import io.cdap.cdap.api.metrics.MetricValues;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.Constants.Metrics;
import io.cdap.cdap.logging.read.LogEvent;
import io.cdap.cdap.logging.read.LogOffset;
import io.cdap.cdap.metrics.collect.AggregatedMetricsCollectionService;
import io.cdap.cdap.proto.ErrorClassificationResponse;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Test class for {@link ErrorLogsClassifier}.
 */
public class ErrorLogsClassifierTest {

  private static final Gson GSON = new Gson();
  private final MockResponder responder = new MockResponder();
  private static final Type LIST_TYPE =
      new TypeToken<List<ErrorClassificationResponse>>() {}.getType();

  @Test
  public void testClassifyLogsWithFailureDetailsProvider() {
    LogEvent logEvent1 = new LogEvent(getEvent1(), LogOffset.LATEST_OFFSET);
    LogEvent logEvent2 = new LogEvent(getEvent2(), LogOffset.LATEST_OFFSET);
    List<LogEvent> events = new ArrayList<>();
    events.add(logEvent2);
    events.add(logEvent1);
    CloseableIterator<LogEvent> closeableIterator = getCloseableIterator(events.iterator());
    List<MetricValues> metricValuesList = new ArrayList<>();
    MetricsCollectionService mockMetricsCollectionService =
        getMockCollectionService(metricValuesList);
    mockMetricsCollectionService.startAndWait();
    CConfiguration cConf = Mockito.mock(CConfiguration.class);
    ErrorLogsClassifier classifier = new ErrorLogsClassifier(cConf, mockMetricsCollectionService);
    classifier.classify(closeableIterator, responder, "namespace", "program", "app", "run");
    List<ErrorClassificationResponse> responses =
        GSON.fromJson(responder.getResponseContentAsString(), LIST_TYPE);
    mockMetricsCollectionService.stopAndWait();
    Assert.assertEquals(1, responses.size());
    Assert.assertEquals("stageName", responses.get(0).getStageName());
    Assert.assertEquals("errorCategory-'stageName'", responses.get(0).getErrorCategory());
    Assert.assertEquals("errorReason", responses.get(0).getErrorReason());
    Assert.assertEquals("some error occurred", responses.get(0).getErrorMessage());
    Assert.assertEquals("errorType", responses.get(0).getErrorType());
    Assert.assertEquals("dependency", responses.get(0).getDependency());
    Assert.assertEquals("errorCodeType", responses.get(0).getErrorCodeType());
    Assert.assertEquals("errorCode", responses.get(0).getErrorCode());
    Assert.assertEquals("supportedDocumentationUrl",
        responses.get(0).getSupportedDocumentationUrl());
    Assert.assertSame(1, metricValuesList.size());
    Assert.assertTrue(containsMetric(metricValuesList.get(0),
        Metrics.Program.FAILED_RUNS_CLASSIFICATION_COUNT));
  }

  @Test
  public void testClassifyLogsWithRuleBasedClassification() {
    List<MetricValues> metricValuesList = new ArrayList<>();
    MetricsCollectionService mockMetricsCollectionService =
        getMockCollectionService(metricValuesList);
    mockMetricsCollectionService.startAndWait();
    CConfiguration cConf = Mockito.mock(CConfiguration.class);
    ErrorLogsClassifier classifier = new ErrorLogsClassifier(cConf, mockMetricsCollectionService);
    LogEvent logEvent3 = new LogEvent(getEvent3(IllegalArgumentException.class.getName()),
        LogOffset.LATEST_OFFSET);
    LogEvent logEvent4 = new LogEvent(getEvent3(IOException.class.getName()),
        LogOffset.LATEST_OFFSET);
    List<LogEvent> events = new ArrayList<>();
    events.add(logEvent3);
    events.add(logEvent4);
    ErrorLogsClassifier spy = Mockito.spy(classifier);
    Mockito.when(spy.getRuleList()).thenReturn(getRulesList());
    Mockito.doCallRealMethod().when(spy).classify(Mockito.any(), Mockito.any(), Mockito.any(),
        Mockito.any(), Mockito.any(), Mockito.any());
    mockMetricsCollectionService.startAndWait();
    CloseableIterator<LogEvent> closeableIterator = getCloseableIterator(events.iterator());
    spy.classify(closeableIterator, responder, "namespace", "program", "app", "run2");
    mockMetricsCollectionService.stopAndWait();
    List<ErrorClassificationResponse> responses =
        GSON.fromJson(responder.getResponseContentAsString(), LIST_TYPE);
    Assert.assertEquals(1, responses.size());
    Assert.assertEquals(ErrorCategoryEnum.OTHERS.name(), responses.get(0).getErrorCategory());
    Assert.assertEquals(ErrorType.USER.name(), responses.get(0).getErrorType());
    Assert.assertSame(1, metricValuesList.size());
    Assert.assertTrue(containsMetric(metricValuesList.get(0),
        Metrics.Program.FAILED_RUNS_CLASSIFICATION_COUNT));
  }

  private CloseableIterator<LogEvent> getCloseableIterator(Iterator<LogEvent> iterator) {
    return new CloseableIterator<LogEvent>() {
      @Override
      public boolean hasNext() {
        return iterator.hasNext();
      }

      @Override
      public LogEvent next() {
        return iterator.next();
      }

      @Override
      public void close() {
        // no-op
      }
    };
  }

  private ILoggingEvent getEvent1() {
    Map<String, String> map = new HashMap<>();
    map.put(Constants.Logging.TAG_FAILED_STAGE, "stageName");
    map.put(Constants.Logging.TAG_ERROR_CATEGORY, "errorCategory");
    map.put(Constants.Logging.TAG_ERROR_REASON, "errorReason");
    map.put(Constants.Logging.TAG_ERROR_TYPE, "errorType");
    IThrowableProxy throwableProxy = Mockito.mock(IThrowableProxy.class);
    Mockito.when(throwableProxy.getMessage()).thenReturn("some error occurred");
    Mockito.when(throwableProxy.getClassName()).thenReturn(WrappedStageException.class.getName());
    ILoggingEvent event = Mockito.mock(ILoggingEvent.class);
    Mockito.when(event.getThrowableProxy()).thenReturn(throwableProxy);
    Mockito.when(event.getMDCPropertyMap()).thenReturn(map);
    return event;
  }

  private ILoggingEvent getEvent2() {
    Map<String, String> map = new HashMap<>();
    map.put(Constants.Logging.TAG_FAILED_STAGE, "stageName");
    map.put(Constants.Logging.TAG_ERROR_CATEGORY, "errorCategory");
    map.put(Constants.Logging.TAG_ERROR_REASON, "errorReason");
    map.put(Constants.Logging.TAG_ERROR_TYPE, "errorType");
    map.put(Constants.Logging.TAG_DEPENDENCY, "dependency");
    map.put(Constants.Logging.TAG_ERROR_CODE_TYPE, "errorCodeType");
    map.put(Constants.Logging.TAG_ERROR_CODE, "errorCode");
    map.put(Constants.Logging.TAG_SUPPORTED_DOC_URL, "supportedDocumentationUrl");
    IThrowableProxy throwableProxy = Mockito.mock(IThrowableProxy.class);
    Mockito.when(throwableProxy.getMessage()).thenReturn("some error occurred");
    Mockito.when(throwableProxy.getClassName()).thenReturn(ProgramFailureException.class.getName());
    ILoggingEvent event = Mockito.mock(ILoggingEvent.class);
    Mockito.when(event.getThrowableProxy()).thenReturn(throwableProxy);
    Mockito.when(event.getMDCPropertyMap()).thenReturn(map);
    return event;
  }

  private ILoggingEvent getEvent3(String exceptionClassName) {
    Map<String, String> map = new HashMap<>();
    IThrowableProxy throwableProxy = Mockito.mock(IThrowableProxy.class);
    Mockito.when(throwableProxy.getMessage()).thenReturn("some error occurred");
    Mockito.when(throwableProxy.getClassName())
        .thenReturn(exceptionClassName);
    StackTraceElementProxy steProxy = new StackTraceElementProxy(
        new StackTraceElement("anyClass", "anyMethod", null, 2));
    StackTraceElementProxy [] stackTraceElementProxies = {steProxy};
    Mockito.when(throwableProxy.getStackTraceElementProxyArray())
        .thenReturn(stackTraceElementProxies);
    ILoggingEvent event = Mockito.mock(ILoggingEvent.class);
    Mockito.when(event.getThrowableProxy()).thenReturn(throwableProxy);
    Mockito.when(event.getMDCPropertyMap()).thenReturn(map);
    return event;
  }

  private List<ErrorClassificationRule> getRulesList() {
    List<ErrorClassificationRule> rules = new ArrayList<>();
    ErrorClassificationRule rule =
        new ErrorClassificationRule.Builder()
            .setId("001_illegal_argument_exception")
            .setExceptionClassName(IllegalArgumentException.class.getName())
            .setErrorType(ErrorType.USER)
            .setDependency(false)
            .setPriority(100)
            .build();
    rules.add(rule);
    rule = new ErrorClassificationRule.Builder()
        .setId("002_ioe_exception")
        .setExceptionClassName(IOException.class.getName())
        .setErrorType(ErrorType.UNKNOWN)
        .setDependency(false)
        .setPriority(200)
        .build();
    rules.add(rule);
    return rules;
  }

  private MetricsCollectionService getMockCollectionService(Collection<MetricValues> collection) {
    return new AggregatedMetricsCollectionService(1000L) {
      @Override
      protected void publish(Iterator<MetricValues> metrics) {
        Iterators.addAll(collection, metrics);
      }
    };
  }

  private boolean containsMetric(MetricValues metricValues, String metricName) {
    for (MetricValue metricValue : metricValues.getMetrics()) {
      if (metricValue.getName().equals(metricName)) {
        return true;
      }
    }
    return false;
  }
}

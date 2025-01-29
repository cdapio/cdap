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
import com.google.common.base.Strings;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.inject.Inject;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.api.exception.ErrorType;
import io.cdap.cdap.api.exception.FailureDetailsProvider;
import io.cdap.cdap.api.exception.WrappedStageException;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.metrics.MetricsContext;
import io.cdap.cdap.common.conf.Constants.Logging;
import io.cdap.cdap.common.conf.Constants.Metrics;
import io.cdap.cdap.logging.read.LogEvent;
import io.cdap.cdap.proto.ErrorClassificationResponse;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Classifies error logs and returns {@link ErrorClassificationResponse}.
 * TODO -
 *  - Add rule based classification.
 */
public class ErrorLogsClassifier {
  private static final Gson GSON = new Gson();
  private static final String DATAPROC_RUNTIME_EXCEPTION =
      "io.cdap.cdap.runtime.spi.provisioner.dataproc.DataprocRuntimeException";
  private static final ImmutableList<String> ALLOWLIST_CLASSES =
      ImmutableList.<String>builder().add(DATAPROC_RUNTIME_EXCEPTION).build();
  private static final LoadingCache<String, Boolean> CLASS_CACHE = CacheBuilder.newBuilder()
      .maximumSize(5000)
      .build(new CacheLoader<String, Boolean>() {
        @Override
        public Boolean load(String className) {
          try {
            return ALLOWLIST_CLASSES.contains(className)
                || FailureDetailsProvider.class.isAssignableFrom(Class.forName(className));
          } catch (Exception e) {
            return false; // Handle missing class
          }
        }
      });
  private static final Logger LOG = LoggerFactory.getLogger(ErrorLogsClassifier.class);
  private final Cache<ErrorClassificationResponseCacheKey,
      List<ErrorClassificationResponse>> responseCache;
  private final MetricsCollectionService metricsCollectionService;

  private static boolean isFailureDetailsProviderInstance(String className) {
    try {
      return CLASS_CACHE.get(className);
    } catch (Exception e) {
      return false; // Handle any unexpected errors
    }
  }

  /**
   * Constructor for {@link ErrorLogsClassifier}.
   */
  @Inject
  public ErrorLogsClassifier(MetricsCollectionService metricsCollectionService) {
    this.metricsCollectionService = metricsCollectionService;
    responseCache = CacheBuilder.newBuilder().maximumSize(1000)
        .expireAfterWrite(7, TimeUnit.DAYS).build();
  }

  /**
   * Classifies error logs and returns {@link ErrorClassificationResponse}.
   *
   * @param logIter Logs Iterator that can be closed.
   * @param responder The HttpResponder.
   * @param namespace The namespace of program.
   * @param program The name of program.
   * @param appId The name of application.
   * @param runId The run id of program.
   */
  public void classify(CloseableIterator<LogEvent> logIter, HttpResponder responder,
      String namespace, @Nullable String program, String appId, String runId) throws Exception {
    ErrorClassificationResponseCacheKey errorClassificationResponseCacheKey =
        new ErrorClassificationResponseCacheKey(namespace, program, appId, runId);
    List<ErrorClassificationResponse> responses =
        responseCache.getIfPresent(errorClassificationResponseCacheKey);
    if (responses != null) {
      responder.sendJson(HttpResponseStatus.OK, GSON.toJson(responses));
      return;
    }
    responses = getErrorClassificationResponse(namespace, program, appId, runId,
        logIter, responder);
    responseCache.put(errorClassificationResponseCacheKey, responses);
  }

  private List<ErrorClassificationResponse> getErrorClassificationResponse(String namespace,
      @Nullable String program, String appId, String runId, CloseableIterator<LogEvent> logIter,
      HttpResponder responder) {
    Map<String, ErrorClassificationResponse> responseMap = new HashMap<>();
    Set<ErrorClassificationResponse> responseSet = new HashSet<>();

    while (logIter.hasNext()) {
      ILoggingEvent logEvent = logIter.next().getLoggingEvent();
      Map<String, String> mdc = logEvent.getMDCPropertyMap();
      String stageName = mdc.get(Logging.TAG_FAILED_STAGE);
      IThrowableProxy throwableProxy = logEvent.getThrowableProxy();
      while (throwableProxy != null) {
        if (isFailureDetailsProviderInstance(throwableProxy.getClassName())) {
          populateResponse(throwableProxy, mdc, stageName, responseMap, responseSet);
        }
        throwableProxy = throwableProxy.getCause();
      }
    }
    List<ErrorClassificationResponse> responses = new ArrayList<>(responseMap.values());
    responses.addAll(responseSet);
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(responses));

    // emit metric
    try {
      emitMetric(namespace, program, appId, runId, responses);
    } catch (Exception e) {
      LOG.error("Unable to emit metric {}.", Metrics.Program.FAILED_RUNS_CLASSIFICATION_COUNT, e);
    }
    return responses;
  }

  private void emitMetric(String namespace, @Nullable String program, String appId, String runId,
      List<ErrorClassificationResponse> responses) {
    MetricsContext metricsContext = metricsCollectionService.getContext(
        getParentTags(namespace, program, appId, runId));
    for (ErrorClassificationResponse response : responses) {
      MetricsContext context = metricsContext.childContext(getChildTags(response));
      context.gauge(Metrics.Program.FAILED_RUNS_CLASSIFICATION_COUNT, 1);
    }
  }

  private Map<String, String> getParentTags(String namespace,
      @Nullable String program, String appId, String runId) {
    ImmutableMap.Builder<String, String> tags = ImmutableMap.builder();
    tags.put(Metrics.Tag.NAMESPACE, namespace);
    if (!Strings.isNullOrEmpty(program)) {
      tags.put(Metrics.Tag.PROGRAM, program);
    }
    tags.put(Metrics.Tag.APP, appId);
    tags.put(Metrics.Tag.RUN_ID, runId);
    return tags.build();
  }

  private Map<String, String> getChildTags(ErrorClassificationResponse response) {
    String stageName = response.getStageName();
    String errorCategory = response.getErrorCategory();
    if (!Strings.isNullOrEmpty(stageName) && !Strings.isNullOrEmpty(errorCategory)
        && errorCategory.endsWith("-" + stageName)) {
      // remove stageName from errorCategory to reduce metric cardinality.
      errorCategory = errorCategory.substring(0, errorCategory.length() - stageName.length() - 1);
    }
    ImmutableMap.Builder<String, String> tags = ImmutableMap.builder();
    tags.put(Metrics.Tag.ERROR_CATEGORY, errorCategory);
    tags.put(Metrics.Tag.ERROR_TYPE, response.getErrorType());
    tags.put(Metrics.Tag.DEPENDENCY, response.getDependency());
    if (!Strings.isNullOrEmpty(response.getErrorCodeType())) {
      tags.put(Metrics.Tag.ERROR_CODE_TYPE, response.getErrorCodeType());
    }
    if (!Strings.isNullOrEmpty(response.getErrorCode())) {
      tags.put(Metrics.Tag.ERROR_CODE, response.getErrorCode());
    }
    return tags.build();
  }

  private void populateResponse(IThrowableProxy throwableProxy,
      Map<String, String> mdc, String stageName,
      Map<String, ErrorClassificationResponse> responseMap,
      Set<ErrorClassificationResponse> responseSet) {
    boolean stageNotPresent = Strings.isNullOrEmpty(stageName);
    if (stageNotPresent) {
      responseSet.add(getClassificationResponse(stageName, mdc, throwableProxy,
          mdc.get(Logging.TAG_ERROR_CATEGORY)));
      return;
    }

    String errorCategory = String.format("%s-'%s'", mdc.get(Logging.TAG_ERROR_CATEGORY), stageName);
    responseMap.putIfAbsent(stageName,
        getClassificationResponse(stageName, mdc, throwableProxy, errorCategory));

    if (WrappedStageException.class.getName().equals(
        responseMap.get(stageName).getThrowableClassName())) {
      // WrappedStageException takes lower precedence than other FailureDetailsProvider exceptions.
      responseMap.put(stageName,
          getClassificationResponse(stageName, mdc, throwableProxy, errorCategory));
    }
  }

  private ErrorClassificationResponse getClassificationResponse(String stageName,
      Map<String, String> mdc, IThrowableProxy throwableProxy, String errorCategory) {
    return new ErrorClassificationResponse.Builder()
        .setStageName(stageName)
        .setErrorCategory(errorCategory)
        .setErrorReason(mdc.get(Logging.TAG_ERROR_REASON))
        .setErrorMessage(throwableProxy.getMessage())
        .setErrorType(mdc.get(Logging.TAG_ERROR_TYPE) == null ? ErrorType.UNKNOWN.name() :
            mdc.get(Logging.TAG_ERROR_TYPE))
        .setDependency(mdc.get(Logging.TAG_DEPENDENCY) == null ? String.valueOf(false) :
            mdc.get(Logging.TAG_DEPENDENCY))
        .setErrorCodeType(mdc.get(Logging.TAG_ERROR_CODE_TYPE))
        .setErrorCode(mdc.get(Logging.TAG_ERROR_CODE))
        .setSupportedDocumentationUrl(mdc.get(Logging.TAG_SUPPORTED_DOC_URL))
        .setThrowableClassName(throwableProxy.getClassName())
        .build();
  }
}

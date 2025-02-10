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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.inject.Inject;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.api.exception.ErrorCategory.ErrorCategoryEnum;
import io.cdap.cdap.api.exception.ErrorType;
import io.cdap.cdap.api.exception.FailureDetailsProvider;
import io.cdap.cdap.api.exception.WrappedStageException;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.metrics.MetricsContext;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants.Logging;
import io.cdap.cdap.common.conf.Constants.Metrics;
import io.cdap.cdap.logging.read.LogEvent;
import io.cdap.cdap.proto.ErrorClassificationResponse;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.io.FileReader;
import java.io.Reader;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Classifies error logs and returns {@link ErrorClassificationResponse}.
 */
public class ErrorLogsClassifier {
  private static final Gson GSON = new Gson();
  private static final String DATAPROC_RUNTIME_EXCEPTION =
      "io.cdap.cdap.runtime.spi.provisioner.dataproc.DataprocRuntimeException";
  private static final ImmutableList<String> ALLOWLIST_CLASSES =
      ImmutableList.<String>builder().add(DATAPROC_RUNTIME_EXCEPTION).build();
  private static final String RULES_FILE_PATH_KEY = "error.classification.rules.file.path";
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
  private final List<ErrorClassificationRule> ruleList;

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
  public ErrorLogsClassifier(CConfiguration cConf,
      MetricsCollectionService metricsCollectionService) {
    this.metricsCollectionService = metricsCollectionService;
    responseCache = CacheBuilder.newBuilder().maximumSize(1000)
        .expireAfterWrite(7, TimeUnit.DAYS).build();
    String rulesFilePath = cConf.get(RULES_FILE_PATH_KEY);
    if (Strings.isNullOrEmpty(rulesFilePath)) {
      LOG.info("Skipping read rules list file as it is not configured.");
      ruleList = Collections.emptyList();
      return;
    }

    Type listType = new TypeToken<List<ErrorClassificationRule>>() {}.getType();
    try (Reader reader = new FileReader(rulesFilePath)) {
      ruleList = GSON.fromJson(reader, listType);
    } catch (Exception e) {
      throw new IllegalArgumentException("Unable to read rules file.", e);
    }
    Collections.sort(ruleList);
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
  public void classify(CloseableIterator<LogEvent> logIter,
      HttpResponder responder, String namespace, @Nullable String program, String appId,
      String runId) {
    ErrorClassificationResponseCacheKey errorClassificationResponseCacheKey =
        new ErrorClassificationResponseCacheKey(namespace, program, appId, runId);
    List<ErrorClassificationResponse> responses =
        responseCache.getIfPresent(errorClassificationResponseCacheKey);
    if (responses != null && !responses.isEmpty()) {
      responder.sendJson(HttpResponseStatus.OK, GSON.toJson(responses));
      return;
    }
    responses = getErrorClassificationResponse(namespace, program, appId, runId,
        logIter, responder);
    responseCache.put(errorClassificationResponseCacheKey, responses);
  }

  @VisibleForTesting
  public List<ErrorClassificationRule> getRuleList() {
    return ruleList;
  }

  private List<ErrorClassificationResponse> getErrorClassificationResponse(String namespace,
      @Nullable String program, String appId, String runId, CloseableIterator<LogEvent> logIter,
      HttpResponder responder) {
    Map<String, ErrorClassificationResponseWrapper> responseMap = new HashMap<>();
    Set<ErrorClassificationResponseWrapper> responseSet = new HashSet<>();
    ErrorClassificationResponseWrapper ruleMatchedResponse = null;
    boolean isFailureDetailsProviderInstancePresent = false;

    while (logIter.hasNext()) {
      ILoggingEvent logEvent = logIter.next().getLoggingEvent();
      Map<String, String> mdc = logEvent.getMDCPropertyMap();
      String stageName = mdc.get(Logging.TAG_FAILED_STAGE);
      IThrowableProxy throwableProxy = logEvent.getThrowableProxy();
      while (throwableProxy != null) {
        if (isFailureDetailsProviderInstance(throwableProxy.getClassName())) {
          isFailureDetailsProviderInstancePresent = true;
          populateResponse(throwableProxy, mdc, stageName, responseMap, responseSet);
        } else if (!isFailureDetailsProviderInstancePresent) {
          String errorReason = String.format("Program run '%s:%s:%s' failed,"
              + "View raw logs for more details.", namespace, appId, runId);
          ruleMatchedResponse = findMatchingRule(throwableProxy, ruleMatchedResponse, errorReason);
        }
        throwableProxy = throwableProxy.getCause();
      }
    }

    List<ErrorClassificationResponseWrapper> responses = new ArrayList<>(responseMap.values());
    responses.addAll(responseSet);
    if (responses.isEmpty() && ruleMatchedResponse != null) {
      responses.add(ruleMatchedResponse);
    }
    List<ErrorClassificationResponse> errorClassificationResponses = responses.stream()
        .map(ErrorClassificationResponseWrapper::getErrorClassificationResponse)
        .collect(Collectors.toList());
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(errorClassificationResponses));

    // emit metric
    try {
      emitMetric(namespace, program, appId, runId, responses);
    } catch (Exception e) {
      LOG.error("Unable to emit metric {}.", Metrics.Program.FAILED_RUNS_CLASSIFICATION_COUNT, e);
    }
    return errorClassificationResponses;
  }

  private void emitMetric(String namespace, @Nullable String program, String appId, String runId,
      List<ErrorClassificationResponseWrapper> responses) {
    MetricsContext metricsContext = metricsCollectionService.getContext(
        getParentTags(namespace, program, appId, runId));
    for (ErrorClassificationResponseWrapper response : responses) {
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

  private Map<String, String> getChildTags(ErrorClassificationResponseWrapper responseWrapper) {
    ErrorClassificationResponse response = responseWrapper.getErrorClassificationResponse();
    ImmutableMap.Builder<String, String> tags = ImmutableMap.builder();
    tags.put(Metrics.Tag.ERROR_CATEGORY, Strings.isNullOrEmpty(responseWrapper.getRuleId())
        ? responseWrapper.getParentErrorCategory() : String.format("%s-rule=%s",
        responseWrapper.getParentErrorCategory(), responseWrapper.getRuleId()));
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

  private void populateResponse(IThrowableProxy throwableProxy, Map<String, String> mdc,
      String stageName, Map<String, ErrorClassificationResponseWrapper> responseMap,
      Set<ErrorClassificationResponseWrapper> responseSet) {
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

  @Nullable
  private ErrorClassificationResponseWrapper findMatchingRule(IThrowableProxy throwableProxy,
      ErrorClassificationResponseWrapper ruleMatchedResponsePrev, String errorReason) {
    ErrorClassificationResponseWrapper ruleMatchedResponse = null;
    for (ErrorClassificationRule rule : getRuleList()) {
      if (classNameMatched(rule.getExceptionClassRegex(), throwableProxy.getClassName())
          && codeMethodMatched(rule.getCodeMethodRegex(),
          throwableProxy.getStackTraceElementProxyArray())) {
        String errorMessage = throwableProxy.getMessage();
        ruleMatchedResponse = new ErrorClassificationResponseWrapper(
            new ErrorClassificationResponse.Builder()
                .setErrorCategory(ErrorCategoryEnum.OTHERS.name())
                .setErrorType(rule.getErrorType().name())
                .setErrorReason(errorReason)
                .setErrorMessage(errorMessage)
                .setDependency(String.valueOf(rule.isDependency())).build(),
            throwableProxy.getClassName(), ErrorCategoryEnum.OTHERS.name(), rule.getPriority(),
            rule.getId());
        break;
      }
    }
    if (ruleMatchedResponse == null) {
      return ruleMatchedResponsePrev;
    } else if (ruleMatchedResponsePrev == null) {
      return ruleMatchedResponse;
    } else {
      return ruleMatchedResponse.getRulePriority() < ruleMatchedResponsePrev.getRulePriority()
          ? ruleMatchedResponse : ruleMatchedResponsePrev;
    }
  }

  private boolean codeMethodMatched(Pattern codeMethodRegex,
      StackTraceElementProxy[] stackTraceElementProxies) {
    return Arrays.stream(stackTraceElementProxies).map(StackTraceElementProxy::getStackTraceElement)
        .anyMatch(ste -> codeMethodRegex.matcher(ste.toString()).find());
  }

  private boolean classNameMatched(Pattern classNameRegex, String exceptionClassName) {
    return classNameRegex.matcher(exceptionClassName).find();
  }

  private ErrorClassificationResponseWrapper getClassificationResponse(String stageName,
      Map<String, String> mdc, IThrowableProxy throwableProxy, String errorCategory) {
    return new ErrorClassificationResponseWrapper(
        new ErrorClassificationResponse.Builder()
            .setStageName(stageName)
            .setErrorCategory(errorCategory)
            .setErrorReason(mdc.get(Logging.TAG_ERROR_REASON))
            .setErrorMessage(throwableProxy.getMessage())
            .setErrorType(mdc.get(Logging.TAG_ERROR_TYPE) == null ? ErrorType.UNKNOWN.name()
                : mdc.get(Logging.TAG_ERROR_TYPE))
            .setDependency(mdc.get(Logging.TAG_DEPENDENCY) == null ? String.valueOf(false)
                : mdc.get(Logging.TAG_DEPENDENCY))
            .setErrorCodeType(mdc.get(Logging.TAG_ERROR_CODE_TYPE))
            .setErrorCode(mdc.get(Logging.TAG_ERROR_CODE))
            .setSupportedDocumentationUrl(mdc.get(Logging.TAG_SUPPORTED_DOC_URL)).build(),
        mdc.get(Logging.TAG_ERROR_CATEGORY), throwableProxy.getClassName(), null, null);
  }
}

/*
 * Copyright © 2021-2023 Cask Data, Inc.
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

package io.cdap.cdap.common.internal.remote;

import com.google.common.base.Preconditions;
import com.google.common.io.ByteStreams;
import com.google.common.net.HttpHeaders;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.retry.RetryCountProvider;
import io.cdap.cdap.api.retry.RetryableException;
import io.cdap.cdap.api.service.ServiceUnavailableException;
import io.cdap.cdap.api.service.worker.RemoteExecutionException;
import io.cdap.cdap.api.service.worker.RunnableTaskRequest;
import io.cdap.cdap.common.ServiceException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.Constants.Security.Encryption;
import io.cdap.cdap.common.encryption.AeadCipher;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.http.HttpCodes;
import io.cdap.cdap.common.service.Retries;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.common.service.RetryStrategy;
import io.cdap.cdap.features.Feature;
import io.cdap.cdap.internal.io.ExposedByteArrayOutputStream;
import io.cdap.cdap.proto.BasicThrowable;
import io.cdap.cdap.proto.security.Credential;
import io.cdap.cdap.security.spi.authentication.SecurityRequestContext;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequestConfig;
import io.cdap.common.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.HttpURLConnection;
import java.net.NoRouteToHostException;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.zip.DeflaterInputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class for executing a {@link RunnableTaskRequest} on a remote worker.
 */
public class RemoteTaskExecutor {

  private static final Logger LOG = LoggerFactory.getLogger(RemoteTaskExecutor.class);
  private static final Gson GSON = new Gson();
  private static final String TASK_WORKER_URL = "/worker/run";
  private static final String SYSTEM_WORKER_URL = "/system/run";
  private static final Predicate<Throwable> RETRYABLE_PREDICATE_SYSTEM_WORKER = throwable ->
      (throwable instanceof RetryableException)
          || (throwable instanceof ServiceException
              && HttpCodes.isRetryable(((ServiceException) throwable).getStatusCode()))
          || (throwable instanceof SocketTimeoutException);
  private static final Predicate<Throwable> RETRYABLE_PREDICATE_TASK_WORKER = throwable ->
      (throwable instanceof RetryableException);
  private static final String FEATURE_FLAG_KEY =
      "feature." + Feature.TASK_WORKER_MANAGER.getFeatureFlagString();
  /** Guards the unreachable manager warning so it is logged once per JVM rather than per task. */
  private static final AtomicBoolean MANAGER_UNREACHABLE_WARNING_LOGGED = new AtomicBoolean();
  private final boolean compression;
  private final RemoteClient remoteClient;
  private final RetryStrategy retryStrategy;
  private final Predicate<Throwable> retryablePredicate;
  private final MetricsCollectionService metricsCollectionService;
  private final AeadCipher userEncryptionAeadCipher;
  private final String workerUrl;
  private final boolean isWorkerEncryptionRequired;
  private final String serviceName;
  private final boolean taskWorkerManagerEnabled;

  public RemoteTaskExecutor(CConfiguration cConf, MetricsCollectionService metricsCollectionService,
      RemoteClientFactory remoteClientFactory, Type workerType, AeadCipher aeadCipher) {
    this(cConf, metricsCollectionService, remoteClientFactory, workerType,
        new DefaultHttpRequestConfig(false), aeadCipher);
  }

  /**
   * Creates a {@link RemoteTaskExecutor} using {@code httpRequestConfig} for worker HTTP calls.
   */
  public RemoteTaskExecutor(CConfiguration cConf, MetricsCollectionService metricsCollectionService,
      RemoteClientFactory remoteClientFactory, Type workerType,
      HttpRequestConfig httpRequestConfig, AeadCipher aeadCipher) {
    this.compression = cConf.getBoolean(Constants.TaskWorker.COMPRESSION_ENABLED);
    this.metricsCollectionService = metricsCollectionService;
    this.userEncryptionAeadCipher = aeadCipher;
    if (workerType == Type.TASK_WORKER) {
      // System worker traffic never goes through the manager.
      this.taskWorkerManagerEnabled = TaskWorkerManagerUtil.isEnabled(cConf);
      this.serviceName = taskWorkerManagerEnabled
          ? Constants.Service.TASK_WORKER_MANAGER : Constants.Service.TASK_WORKER;
      this.workerUrl = TASK_WORKER_URL;
      this.retryStrategy = RetryStrategies.fromConfiguration(cConf,
          Constants.Service.TASK_WORKER + ".");
      this.retryablePredicate = RETRYABLE_PREDICATE_TASK_WORKER;
      this.isWorkerEncryptionRequired = true;
    } else {
      this.taskWorkerManagerEnabled = false;
      this.serviceName = Constants.Service.SYSTEM_WORKER;
      this.workerUrl = SYSTEM_WORKER_URL;
      this.retryStrategy = RetryStrategies.fromConfiguration(cConf,
          Constants.Service.SYSTEM_WORKER + ".");
      this.retryablePredicate = RETRYABLE_PREDICATE_SYSTEM_WORKER;
      this.isWorkerEncryptionRequired = false;
    }

    // Cap connect timeout so a hung connect during a singleton manager restart retries quickly
    // instead of burning the caller's full connect timeout.
    HttpRequestConfig clientConfig = taskWorkerManagerEnabled
        ? capConnectTimeout(httpRequestConfig,
            cConf.getInt(Constants.TaskWorkerManager.CLIENT_CONNECT_TIMEOUT_MS))
        : httpRequestConfig;
    this.remoteClient = remoteClientFactory.createRemoteClient(serviceName, clientConfig,
        Constants.Gateway.INTERNAL_API_VERSION_3);
  }

  /**
   * Sends the {@link RunnableTaskRequest} to a remote worker and returns the result. Retries
   * sending the request if the workers are busy.
   *
   * @param runnableTaskRequest {@link RunnableTaskRequest} with details of task
   * @return byte[] response from remote task
   * @throws Exception returned by remote task if any
   */
  public byte[] runTask(RunnableTaskRequest runnableTaskRequest) throws Exception {
    //initialize start time for collecting latency metric
    long startTime = System.currentTimeMillis();
    ByteBuffer requestBody = encodeTaskRequest(runnableTaskRequest);
    // Tells the task worker manager which namespace's leased pod the request belongs to.
    String namespace =
        taskWorkerManagerEnabled ? TaskDetails.extractNamespace(runnableTaskRequest) : null;

    try {
      return Retries.callWithRetries((retryContext) -> {
        try {
          HttpRequest.Builder requestBuilder = remoteClient
              .requestBuilder(HttpMethod.POST, workerUrl)
              .withBody(requestBody.duplicate());
          if (namespace != null) {
            requestBuilder.addHeader(Constants.TaskWorkerManager.HEADER_CDAP_NAMESPACE, namespace);
          }
          if (compression) {
            requestBuilder.addHeader(HttpHeaders.CONTENT_ENCODING, "gzip");
            requestBuilder.addHeader(HttpHeaders.ACCEPT_ENCODING, "gzip, deflate");
          }
          HttpRequest httpRequest = requestBuilder.build();

          long requestStartTime = System.currentTimeMillis();
          Credential currentCredential = SecurityRequestContext.getUserCredential();
          HttpResponse httpResponse;
          try {
            // Encrypting user credentials for task worker calls
            if (isWorkerEncryptionRequired && currentCredential != null) {
              String encryptedValue = userEncryptionAeadCipher.encryptToBase64(
                  currentCredential.getValue(),
                  Encryption.TASK_WORKER_ENCRYPTION_ASSOCIATED_DATA.getBytes());
              SecurityRequestContext.setUserCredential(
                  new Credential(encryptedValue, currentCredential.getType()));
            }
            httpResponse = remoteClient.execute(httpRequest);
            if (taskWorkerManagerEnabled) {
              MANAGER_UNREACHABLE_WARNING_LOGGED.set(false);
            }
          } catch (SocketTimeoutException e) {
            // Not connected means not sent, so a retry can't run the task twice; treat it like a
            // refused connection. A read timeout stays fatal: the worker may be running the task.
            if (taskWorkerManagerEnabled && isConnectTimeout(e)) {
              throw new ServiceUnavailableException(Constants.Service.TASK_WORKER_MANAGER, e);
            }
            throw e;
          } finally {
            // Restore even on failure, or the next retry would encrypt the ciphertext again and
            // the worker would reject it.
            if (isWorkerEncryptionRequired) {
              SecurityRequestContext.setUserCredential(currentCredential);
            }
          }
          long executionDurationMs = System.currentTimeMillis() - requestStartTime;

          LOG.trace("Received response from {} with status code {} in {} ms", serviceName,
              httpResponse.getResponseCode(), executionDurationMs);

          if (httpResponse.getResponseCode() == HttpResponseStatus.TOO_MANY_REQUESTS.code()) {
            throw new RetryableException(
                String.format("Received response code %s for %s", httpResponse.getResponseCode(),
                    runnableTaskRequest.getClassName()));
          }
          if (httpResponse.getResponseCode() != HttpURLConnection.HTTP_OK) {
            String responseBody = httpResponse.getResponseBodyAsString();
            BasicThrowable basicThrowable = null;
            try {
              basicThrowable = GSON.fromJson(responseBody, BasicThrowable.class);
            } catch (JsonSyntaxException ignored) {
              // Non-JSON error body from a proxy or Netty; surface as ServiceException below.
            }
            if (basicThrowable == null || basicThrowable.getClassName() == null
                || basicThrowable.getStackTraces() == null) {
              String message = responseBody == null || responseBody.isEmpty()
                  ? String.format("Request to %s for %s failed with status %d",
                      serviceName, runnableTaskRequest.getClassName(),
                      httpResponse.getResponseCode())
                  : String.format("Request to %s for %s failed with status %d: %s",
                      serviceName, runnableTaskRequest.getClassName(),
                      httpResponse.getResponseCode(), responseBody);
              throw new ServiceException(message, null,
                  HttpResponseStatus.valueOf(httpResponse.getResponseCode()));
            }
            throw RemoteExecutionException.fromBasicThrowable(basicThrowable);
          }
          byte[] result = httpResponse.getUncompressedResponseBody();
          //emit metrics with successful result
          emitMetrics(startTime, true, runnableTaskRequest, retryContext.getRetryAttempt());
          return result;
        } catch (NoRouteToHostException e) {
          throw new RetryableException(
              String.format("Received exception %s for %s", e.getMessage(),
                  runnableTaskRequest.getClassName()));
        }
      }, retryStrategy, retryablePredicate);
    } catch (ServiceException se) {
      Exception ex = getTaskException(se);
      //emit metrics with failed result
      emitMetrics(startTime, false, runnableTaskRequest, getAttempts(ex));
      throw ex;
    } catch (Exception e) {
      //emit metrics with failed result
      emitMetrics(startTime, false, runnableTaskRequest, getAttempts(e));
      if (taskWorkerManagerEnabled && e instanceof ServiceUnavailableException) {
        throw managerUnreachableException((ServiceUnavailableException) e, startTime);
      }
      throw e;
    }
  }

  /**
   * Returns {@code config} with its connect timeout lowered to {@code maxConnectTimeoutMs}, keeping
   * every other setting. A connect timeout of 0 means "wait forever", so it is capped too.
   */
  private static HttpRequestConfig capConnectTimeout(HttpRequestConfig config,
      int maxConnectTimeoutMs) {
    Preconditions.checkArgument(maxConnectTimeoutMs > 0, "%s must be positive, got %s",
        Constants.TaskWorkerManager.CLIENT_CONNECT_TIMEOUT_MS, maxConnectTimeoutMs);
    int connectTimeout = config.getConnectTimeout();
    if (connectTimeout > 0 && connectTimeout <= maxConnectTimeoutMs) {
      return config;
    }
    return new HttpRequestConfig(maxConnectTimeoutMs, config.getReadTimeout(),
        config.isVerifySSLCert(), config.getFixedLengthStreamingThreshold());
  }

  /**
   * Returns whether {@code e} was raised while connecting rather than while reading the response.
   * {@code HttpURLConnection} tells the two apart only by the message.
   */
  private static boolean isConnectTimeout(SocketTimeoutException e) {
    String message = e.getMessage();
    return message != null && message.toLowerCase(Locale.ROOT).contains("connect timed out");
  }

  /**
   * Builds the error for a task the manager path never ran: the manager answered that no task
   * worker was reachable, or the manager itself was refused or not discovered.
   */
  private ServiceException managerUnreachableException(ServiceUnavailableException cause,
      long startTime) {
    long elapsedSeconds = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - startTime);
    if (Constants.TaskWorkerManager.WORKER_UNREACHABLE_BODY.equals(cause.getMessage())) {
      return new ServiceException(
          String.format("The %s service could not reach a task worker for %d seconds, so the task "
                  + "was not run.", Constants.Service.TASK_WORKER_MANAGER, elapsedSeconds),
          cause, HttpResponseStatus.SERVICE_UNAVAILABLE);
    }
    warnOnceAboutUnreachableManager();
    return new ServiceException(
        String.format("Could not reach the %s service after %d seconds, so the task was not run.",
            Constants.Service.TASK_WORKER_MANAGER, elapsedSeconds),
        cause, HttpResponseStatus.SERVICE_UNAVAILABLE);
  }

  /** Logs once per unreachable episode so repeated task failures do not flood the log. */
  private static void warnOnceAboutUnreachableManager() {
    if (Thread.currentThread().isInterrupted()
        || !MANAGER_UNREACHABLE_WARNING_LOGGED.compareAndSet(false, true)) {
      return;
    }
    LOG.warn("The {} service could not be reached and {} is enabled; task worker requests will "
            + "fail until it is reachable.", Constants.Service.TASK_WORKER_MANAGER,
        FEATURE_FLAG_KEY);
  }

  private Exception getTaskException(ServiceException e) {
    if (e.getJsonDetails() == null) {
      // This is not an application-level exception, might be a timeout or similar failure
      return e;
    }
    try {
      BasicThrowable basicThrowable = GSON.fromJson(e.getJsonDetails(), BasicThrowable.class);
      return RemoteExecutionException.fromBasicThrowable(basicThrowable);
    } catch (JsonSyntaxException jse) {
      e.addSuppressed(jse);
      return e;
    }
  }

  /**
   * Find if attempt count is included in the suppressed throwable.
   *
   * @param e Exception to analyze
   * @return attempt count
   */
  private int getAttempts(Exception e) {
    Throwable[] suppressed = e.getSuppressed();
    for (Throwable t : suppressed) {
      if (t instanceof RetryCountProvider) {
        return ((RetryCountProvider) t).getRetries();
      }
    }
    return 0;
  }

  private void emitMetrics(long startTime, boolean success, RunnableTaskRequest runnableTaskRequest,
      int attempts) {
    String taskClass = getTaskClassName(runnableTaskRequest);
    Map<String, String> metricTags = new HashMap<>();
    metricTags.put(Constants.Metrics.Tag.CLASS, taskClass);
    metricTags.put(Constants.Metrics.Tag.STATUS, success ? "success" : "failure");
    metricTags.put(Constants.Metrics.Tag.TRIES, String.valueOf(attempts));
    metricsCollectionService.getContext(metricTags)
        .increment(Constants.Metrics.TaskWorker.CLIENT_REQUEST_COUNT, 1L);
    metricsCollectionService.getContext(metricTags)
        .gauge(Constants.Metrics.TaskWorker.CLIENT_REQUEST_LATENCY_MS,
            System.currentTimeMillis() - startTime);
  }

  private String getTaskClassName(RunnableTaskRequest runnableTaskRequest) {
    if (runnableTaskRequest.getParam() == null
        || runnableTaskRequest.getParam().getEmbeddedTaskRequest() == null) {
      return runnableTaskRequest.getClassName();
    }
    return runnableTaskRequest.getParam().getEmbeddedTaskRequest().getClassName();
  }

  private ByteBuffer encodeTaskRequest(RunnableTaskRequest request) throws IOException {
    ExposedByteArrayOutputStream bos = new ExposedByteArrayOutputStream();
    try (Writer writer = new OutputStreamWriter(compression ? new GZIPOutputStream(bos) : bos,
        StandardCharsets.UTF_8)) {
      GSON.toJson(request, writer);
    }
    return bos.toByteBuffer();
  }

  /**
   * Decodes and return the response body based on the content encoding.
   */
  private byte[] getResponseBody(HttpResponse response) throws IOException {
    String encoding = response.getHeaders().entries().stream()
        .filter(e -> HttpHeaders.CONTENT_ENCODING.equalsIgnoreCase(e.getKey()))
        .map(Map.Entry::getValue)
        .findFirst()
        .orElse(null);

    if (encoding == null) {
      return response.getResponseBody();
    }

    if ("gzip".equalsIgnoreCase(encoding)) {
      try (InputStream is = new GZIPInputStream(
          new ByteArrayInputStream(response.getResponseBody()))) {
        return ByteStreams.toByteArray(is);
      }
    }
    if ("deflate".equalsIgnoreCase(encoding)) {
      try (InputStream is = new DeflaterInputStream(
          new ByteArrayInputStream(response.getResponseBody()))) {
        return ByteStreams.toByteArray(is);
      }
    }

    throw new IllegalArgumentException("Unsupported content encoding " + encoding);
  }

  /*
   Use task worker for executing an unsecured logic (e.g., deploying a pipeline which contains user code) remotely on
   a task worker pod.
   Use system worker pod for executing secure logic remotely on a system worker pod.
   */
  public enum Type {
    SYSTEM_WORKER,
    TASK_WORKER
  }
}

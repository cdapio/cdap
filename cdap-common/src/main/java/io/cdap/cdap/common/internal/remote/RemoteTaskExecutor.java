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

import com.google.common.io.ByteStreams;
import com.google.common.net.HttpHeaders;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.retry.RetryCountProvider;
import io.cdap.cdap.api.retry.RetryableException;
import io.cdap.cdap.api.service.worker.RemoteExecutionException;
import io.cdap.cdap.api.service.worker.RunnableTaskRequest;
import io.cdap.cdap.common.ServiceException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.Constants.Security.Encryption;
import io.cdap.cdap.common.encryption.AeadCipher;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.service.Retries;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.common.service.RetryStrategy;
import io.cdap.cdap.internal.io.ExposedByteArrayOutputStream;
import io.cdap.cdap.proto.BasicThrowable;
import io.cdap.cdap.proto.security.Credential;
import io.cdap.cdap.security.spi.authentication.SecurityRequestContext;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequestConfig;
import io.cdap.common.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import java.util.Map;
import java.util.function.Predicate;
import java.util.zip.DeflaterInputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Helper class for executing a {@link RunnableTaskRequest} on a remote worker.
 */
public class RemoteTaskExecutor {

  private static final Logger LOG = LoggerFactory.getLogger(RemoteTaskExecutor.class);
  private static final Gson GSON = new Gson();
  private static final String TASK_WORKER_URL = "/worker/run";
  private static final String SYSTEM_WORKER_URL = "/system/run";
  private static final Predicate<Throwable> RETRYABLE_PREDICATE_SYSTEM_WORKER = throwable ->
      (throwable instanceof RetryableException) || (throwable instanceof ServiceException)
          || (throwable instanceof SocketTimeoutException);
  private static final Predicate<Throwable> RETRYABLE_PREDICATE_TASK_WORKER = throwable ->
      (throwable instanceof RetryableException);
  private final boolean compression;
  private final RemoteClient remoteClient;
  private final RetryStrategy retryStrategy;
  private final Predicate<Throwable> retryablePredicate;
  private final MetricsCollectionService metricsCollectionService;
  private final AeadCipher userEncryptionAeadCipher;
  private final String workerUrl;
  private final boolean isWorkerEncryptionRequired;

  public RemoteTaskExecutor(CConfiguration cConf, MetricsCollectionService metricsCollectionService,
      RemoteClientFactory remoteClientFactory, Type workerType, AeadCipher aeadCipher) {
    this(cConf, metricsCollectionService, remoteClientFactory, workerType,
        new DefaultHttpRequestConfig(false), aeadCipher);
  }

  public RemoteTaskExecutor(CConfiguration cConf, MetricsCollectionService metricsCollectionService,
      RemoteClientFactory remoteClientFactory, Type workerType,
      HttpRequestConfig httpRequestConfig, AeadCipher aeadCipher) {
    this.compression = cConf.getBoolean(Constants.TaskWorker.COMPRESSION_ENABLED);
    String taskServiceName = cConf.getBoolean(Constants.Security.Authorization.ENABLED)
        ? Constants.Service.TASK_MANAGER : Constants.Service.TASK_WORKER;
    String serviceName = workerType == Type.TASK_WORKER
        ? taskServiceName : Constants.Service.SYSTEM_WORKER;
    this.remoteClient = remoteClientFactory.createRemoteClient(serviceName,
        httpRequestConfig,
        Constants.Gateway.INTERNAL_API_VERSION_3);
    this.metricsCollectionService = metricsCollectionService;
    this.userEncryptionAeadCipher = aeadCipher;
    if (workerType == Type.TASK_WORKER) {
      this.workerUrl = TASK_WORKER_URL;
      this.retryStrategy = RetryStrategies.fromConfiguration(cConf,
          Constants.Service.TASK_WORKER + ".");
      this.retryablePredicate = RETRYABLE_PREDICATE_TASK_WORKER;
      this.isWorkerEncryptionRequired = true;
    } else {
      this.workerUrl = SYSTEM_WORKER_URL;
      this.retryStrategy = RetryStrategies.fromConfiguration(cConf,
          Constants.Service.SYSTEM_WORKER + ".");
      this.retryablePredicate = RETRYABLE_PREDICATE_SYSTEM_WORKER;
      this.isWorkerEncryptionRequired = false;
    }
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

    try {
      return Retries.callWithRetries((retryContext) -> {
        try {
          String namespace = runnableTaskRequest.getNamespace();
          if ("system".equals(namespace) && runnableTaskRequest.getParam() != null
              && runnableTaskRequest.getParam().getEmbeddedTaskRequest() != null) {
            String embeddedNamespace = runnableTaskRequest.getParam().getEmbeddedTaskRequest().getNamespace();
            if (embeddedNamespace != null && !embeddedNamespace.isEmpty()) {
              namespace = embeddedNamespace;
              LOG.info("shruzard - RemoteTaskExecutor: Mapped SystemAppTask namespace to embedded: {}",
                       namespace);
            }
          }
          String routingKey = namespace;
          if (System.currentTimeMillis() - startTime > 60000) {
              LOG.warn("shruzard - TaskManager Proxy unreachable for 60s! "
                       + "Bypassing proxy and falling back to direct Worker routing!");
              routingKey = null; // Setting to null triggers CDAP's native RandomEndpoint discovery in RemoteClient
          }

          HttpRequest.Builder requestBuilder = remoteClient
              .requestBuilder(HttpMethod.POST, workerUrl, routingKey)
              .addHeader("X-CDF-Namespace", namespace)
              .withBody(requestBody.duplicate());
          if (compression) {
            requestBuilder.addHeader(HttpHeaders.CONTENT_ENCODING, "gzip");
            requestBuilder.addHeader(HttpHeaders.ACCEPT_ENCODING, "gzip, deflate");
          }

          // Encrypting user credentials for task worker calls
          Credential currentCredential = SecurityRequestContext.getUserCredential();
          if (isWorkerEncryptionRequired && currentCredential != null) {
            String encryptedValue = userEncryptionAeadCipher.encryptToBase64(currentCredential.getValue(),
                Encryption.TASK_WORKER_ENCRYPTION_ASSOCIATED_DATA.getBytes());
            Credential encryptedCredential = new Credential(encryptedValue, currentCredential.getType());
            SecurityRequestContext.setUserCredential(encryptedCredential);
          }

          HttpRequest httpRequest = requestBuilder.build();
          HttpResponse httpResponse = remoteClient.execute(httpRequest);

          // Resetting user credentials for further execution of current request
          if (isWorkerEncryptionRequired) {
            SecurityRequestContext.setUserCredential(currentCredential);
          }

          if (httpResponse.getResponseCode() == HttpResponseStatus.TOO_MANY_REQUESTS.code()) {
            throw new RetryableException(
                String.format("Task Worker cluster is fully saturated. "
                        + "Unable to secure a compute lease after "
                        + "60 seconds (HTTP 429 for %s). Please try again.",
                    runnableTaskRequest.getClassName()));
          }
          if (httpResponse.getResponseCode() != HttpURLConnection.HTTP_OK) {
            BasicThrowable basicThrowable = GSON
                .fromJson(httpResponse.getResponseBodyAsString(), BasicThrowable.class);
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
      if (e instanceof RetryableException && e.getMessage() != null
          && e.getMessage().contains("Task Worker cluster is fully saturated")) {
        throw new ServiceException(
            e.getMessage(), e, HttpResponseStatus.TOO_MANY_REQUESTS);
      }
      throw e;
    }
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

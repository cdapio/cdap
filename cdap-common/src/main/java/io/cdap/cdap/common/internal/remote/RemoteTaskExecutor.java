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
import io.cdap.cdap.api.service.ServiceUnavailableException;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.HttpURLConnection;
import java.net.NoRouteToHostException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
          || (throwable instanceof SocketTimeoutException) || (throwable instanceof SocketException)
          || (throwable instanceof NoRouteToHostException);
  private static final Predicate<Throwable> RETRYABLE_PREDICATE_TASK_WORKER = throwable ->
      (throwable instanceof RetryableException)
          || (throwable instanceof SocketException)
          || (throwable instanceof SocketTimeoutException)
          || (throwable instanceof NoRouteToHostException);
  private static final String PROXY_FEATURE_FLAG_KEY =
      "feature." + Feature.RBAC_TASK_WORKER_MANAGER.getFeatureFlagString();
  /** Guards the deployment level diagnosis so it is logged once per JVM rather than per task. */
  private static final AtomicBoolean PROXY_UNREACHABLE_WARNING_LOGGED = new AtomicBoolean();
  private final boolean compression;
  private final RemoteClient remoteClient;
  private final RetryStrategy retryStrategy;
  private final Predicate<Throwable> retryablePredicate;
  private final MetricsCollectionService metricsCollectionService;
  private final AeadCipher userEncryptionAeadCipher;
  private final String workerUrl;
  private final boolean isWorkerEncryptionRequired;
  private final String serviceName;
  private final boolean rbacProxyEnabled;

  public RemoteTaskExecutor(CConfiguration cConf, MetricsCollectionService metricsCollectionService,
      RemoteClientFactory remoteClientFactory, Type workerType, AeadCipher aeadCipher) {
    this(cConf, metricsCollectionService, remoteClientFactory, workerType,
        new DefaultHttpRequestConfig(false), aeadCipher);
  }

  public RemoteTaskExecutor(CConfiguration cConf, MetricsCollectionService metricsCollectionService,
      RemoteClientFactory remoteClientFactory, Type workerType,
      HttpRequestConfig httpRequestConfig, AeadCipher aeadCipher) {
    this.compression = cConf.getBoolean(Constants.TaskWorker.COMPRESSION_ENABLED);

    // The netty proxy only makes sense on RBAC instances: it exists to lease a task worker pod per
    // namespace so user code from two namespaces never shares a JVM. Both the feature flag and
    // instance-level RBAC must be on, otherwise task.worker.manager isn't even deployed in the cluster.
    boolean proxyConfigured = TaskWorkerManager.isEnabled(cConf);

    // System worker traffic runs trusted platform code and is never leased per namespace, so it
    // goes straight to system.worker. Scoping the flag here keeps the whole proxy code path in
    // runTask (routing key, namespace header, circuit breaker) off for that worker type.
    this.rbacProxyEnabled = proxyConfigured && workerType == Type.TASK_WORKER;

    if (workerType == Type.TASK_WORKER) {
      this.serviceName = rbacProxyEnabled
          ? Constants.Service.TASK_WORKER_MANAGER : Constants.Service.TASK_WORKER;
    } else {
      this.serviceName = Constants.Service.SYSTEM_WORKER;
    }
    LOG.debug("RemoteTaskExecutor routing {} traffic to service {} (rbacProxyEnabled={})",
        workerType, serviceName, rbacProxyEnabled);

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
          // STEP 1: Route on the namespace the worker will admit the task under.
          String namespace = null;
          String routingKey = null;
          if (rbacProxyEnabled) {
            namespace = TaskDetails.extractNamespace(runnableTaskRequest);
            routingKey = namespace;
          }

          // STEP 2: Construct the outbound request. There is deliberately no direct-to-worker
          // fallback: the proxy holds the per-namespace pod lease, so bypassing it would leave the
          // routing registry describing a placement that never happened. A proxy outage therefore
          // fails the task rather than silently degrading, and cdap-operator is responsible for
          // bringing the proxy back.
          HttpRequest.Builder requestBuilder =
              remoteClient.requestBuilder(HttpMethod.POST, workerUrl, routingKey);
          if (routingKey != null) {
            // Tells the netty proxy which namespace's leased pod this request belongs to.
            requestBuilder.addHeader(Constants.Gateway.HEADER_CDAP_NAMESPACE, namespace);
          }
          requestBuilder.withBody(requestBody.duplicate());
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

          long requestStartTime = System.currentTimeMillis();
          HttpResponse httpResponse;
          try {
            httpResponse = remoteClient.execute(httpRequest);
          } finally {
            // Restore in a finally block. The request can fail with an IOException or a
            // ServiceException, and leaving the encrypted credential on the thread local would make
            // the next retry encrypt the ciphertext again. Each pass grows the value and the worker
            // can no longer decrypt it. That matters most during a proxy outage, which is exactly
            // when this loop retries hardest.
            if (isWorkerEncryptionRequired) {
              SecurityRequestContext.setUserCredential(currentCredential);
            }
          }
          long executionDurationMs = System.currentTimeMillis() - requestStartTime;

          LOG.trace("Received response from {} with status code {} in {} ms", serviceName,
              httpResponse.getResponseCode(), executionDurationMs);

          // STEP 4: Handle Responses & Retryable Exceptions
          // A 429 means no compute lease could be secured right now: either the proxy could not
          // lease a pod for this namespace, or the worker hit its own concurrency limit. Retry with
          // backoff rather than failing the pipeline outright.
          if (httpResponse.getResponseCode() == HttpResponseStatus.TOO_MANY_REQUESTS.code()) {
            throw new TaskWorkerSaturatedException(
                String.format("Task Worker cluster is fully saturated (HTTP 429). Could not secure "
                        + "a compute lease for %s. Triggering backoff...",
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
        } catch (ServiceException e) {
          // 503 natively throws ServiceUnavailableException (which extends RetryableException),
          // but 502 and 504 surface as a plain ServiceException, which the retry predicate does not
          // match. Trap those two infrastructure errors and force a retry so a proxy that is
          // restarting gets the full retry budget to come back.
          if (e.getStatusCode() == HttpResponseStatus.BAD_GATEWAY.code()
              || e.getStatusCode() == HttpResponseStatus.GATEWAY_TIMEOUT.code()) {
            throw new RetryableException("Proxy infrastructure unreachable (HTTP "
                + e.getStatusCode() + "). Forcing retry.", e);
          }
          throw e; // Non-infrastructure ServiceExceptions (like 403 or 401) must fail immediately
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
      if (e instanceof TaskWorkerSaturatedException) {
        throw new ServiceException(
            String.format("Task Worker cluster is fully saturated. Unable to secure a compute "
                    + "lease after %d seconds (HTTP 429). Please try again later.",
                TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - startTime)),
            e, HttpResponseStatus.TOO_MANY_REQUESTS);
      }
      if (rbacProxyEnabled && isProxyUnreachable(e)) {
        throw proxyUnreachableException(e, startTime);
      }
      throw e;
    }
  }

  /**
   * Returns true when the failure means we never got an answer out of the proxy, as opposed to the
   * proxy returning an application level error. Only transport and discovery failures qualify, so
   * a real task failure is never rewritten into an infrastructure message.
   */
  private static boolean isProxyUnreachable(Exception e) {
    return e instanceof ServiceUnavailableException
        || e instanceof NoRouteToHostException
        || e instanceof SocketTimeoutException
        || e instanceof SocketException;
  }

  /**
   * Builds the terminal error for a proxy that never answered. Without this the caller sees a bare
   * "service is not available" for {@code task.worker.manager}, which reads like a task worker problem and
   * gives no hint that the proxy is a separate deployment with its own lifecycle.
   */
  private ServiceException proxyUnreachableException(Exception cause, long startTime) {
    long elapsedSeconds = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - startTime);
    warnOnceAboutUnreachableProxy();
    return new ServiceException(
        String.format("Could not reach the %s proxy after %d seconds, so the task was not run. "
                + "Task worker traffic is routed through the proxy because %s is enabled. Verify "
                + "that the %s service is deployed and healthy.",
            Constants.Service.TASK_WORKER_MANAGER, elapsedSeconds, PROXY_FEATURE_FLAG_KEY,
            Constants.Service.TASK_WORKER_MANAGER),
        cause, HttpResponseStatus.SERVICE_UNAVAILABLE);
  }

  /**
   * Logs the deployment level diagnosis a single time per JVM. Every task hitting an absent proxy
   * fails the same way, so repeating this per task would bury the signal.
   */
  private static void warnOnceAboutUnreachableProxy() {
    if (!PROXY_UNREACHABLE_WARNING_LOGGED.compareAndSet(false, true)) {
      return;
    }
    LOG.warn("The {} proxy could not be reached and {} is enabled, so no task worker request can "
            + "succeed. The proxy is created by cdap-operator, not by CDAP, so this is expected if "
            + "the flag was turned on against an operator that does not deploy the {} service. "
            + "This message is logged once.",
        Constants.Service.TASK_WORKER_MANAGER, PROXY_FEATURE_FLAG_KEY, Constants.Service.TASK_WORKER_MANAGER);
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

  /**
   * Marks an HTTP 429 (no compute lease available) response so the outer handler can translate it
   * into a caller-facing error without matching on the message text. Extends
   * {@link RetryableException} so the retry predicate keeps retrying it.
   */
  private static final class TaskWorkerSaturatedException extends RetryableException {

    TaskWorkerSaturatedException(String message) {
      super(message);
    }
  }
}

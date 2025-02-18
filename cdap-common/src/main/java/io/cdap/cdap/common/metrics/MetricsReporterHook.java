/*
 * Copyright © 2014-2020 Cask Data, Inc.
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

package io.cdap.cdap.common.metrics;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.feature.FeatureFlagsProvider;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.metrics.MetricsContext;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.feature.DefaultFeatureFlagsProvider;
import io.cdap.cdap.common.http.HttpHeaderNames;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.http.AbstractHandlerHook;
import io.cdap.http.HttpResponder;
import io.cdap.http.internal.HandlerInfo;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Records gateway requests/response metrics.
 */
public class MetricsReporterHook extends AbstractHandlerHook {

  private static final Logger LOG = LoggerFactory.getLogger(MetricsReporterHook.class);
  private static final Pattern NAMESPACE_PATTERN = Pattern.compile("/namespaces/([^/]+)");
  private static final String LATENCY_METRIC_NAME = "response.latency";

  private final String serviceName;
  private final LoadingCache<Map<String, String>, MetricsContext> collectorCache;

  private final FeatureFlagsProvider featureFlagsProvider;

  public MetricsReporterHook(CConfiguration cConf,
      MetricsCollectionService metricsCollectionService, String serviceName) {
    this.serviceName = serviceName;
    this.featureFlagsProvider = new DefaultFeatureFlagsProvider(cConf);
    if (metricsCollectionService != null) {
      this.collectorCache = CacheBuilder.newBuilder()
          .expireAfterAccess(1, TimeUnit.HOURS)
          .build(new CacheLoader<Map<String, String>, MetricsContext>() {
            @Override
            public MetricsContext load(Map<String, String> key) {
              return metricsCollectionService.getContext(key);
            }
          });
    } else {
      collectorCache = null;
    }
  }

  @Override
  public boolean preCall(HttpRequest request, HttpResponder responder, HandlerInfo handlerInfo) {

    if (collectorCache == null) {
      return true;
    }
    try {
      MetricsContext collector = collectorCache.get(createContext(request, handlerInfo));
      collector.increment("request.received", 1);
      request.headers().add(HttpHeaderNames.CDAP_REQ_TIMESTAMP_HDR, System.nanoTime());
    } catch (Throwable e) {
      LOG.error("Got exception while getting collector", e);
    }
    return true;
  }

  @Override
  public void postCall(HttpRequest request, HttpResponseStatus status, HandlerInfo handlerInfo) {
    if (collectorCache == null) {
      return;
    }
    try {
      MetricsContext collector = collectorCache.get(createContext(request, handlerInfo));
      String name;
      int code = status.code();
      if (code < 100) {
        name = "unknown";
      } else if (code < 200) {
        name = "information";
      } else if (code < 300) {
        name = "successful";
      } else if (code < 400) {
        name = "redirect";
      } else if (code < 500) {
        name = "client-error";
      } else if (code < 600) {
        name = "server-error";
      } else {
        name = "unknown";
      }

      // todo: report metrics broken down by status
      collector.increment("response." + name, 1/*, "status:" + code*/);

      // store response time metric
      long currTime = System.nanoTime();
      String startTimeStr = request.headers().get(HttpHeaderNames.CDAP_REQ_TIMESTAMP_HDR);
      if (startTimeStr != null) {
        long responseTimeNanos = currTime - Long.parseLong(startTimeStr);
        collector.event(LATENCY_METRIC_NAME, responseTimeNanos);
      }

    } catch (Throwable e) {
      LOG.error("Got exception while getting collector", e);
    }
  }

  private Map<String, String> createContext(HttpRequest request, HandlerInfo handlerInfo) {
    // todo: really inefficient to call this on the intense data flow path
    return ImmutableMap.of(
        Constants.Metrics.Tag.NAMESPACE, getNamespaceFromUriIfPresent(request.uri()),
        Constants.Metrics.Tag.COMPONENT, serviceName,
        Constants.Metrics.Tag.HANDLER, getSimpleName(handlerInfo.getHandlerName()),
        Constants.Metrics.Tag.METHOD, handlerInfo.getMethodName());
  }

  private String getSimpleName(String className) {
    int ind = className.lastIndexOf('.');
    return className.substring(ind + 1);
  }

  private static String getNamespaceFromUriIfPresent(@Nullable final String uri) {
    if (uri == null || uri.isEmpty()) {
      return NamespaceId.SYSTEM.getEntityName();
    }

    Matcher matcher = NAMESPACE_PATTERN.matcher(uri);
    if (matcher.find()) {
      return matcher.group(1);
    }

    return NamespaceId.SYSTEM.getEntityName();
  }
}

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
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.metrics.MetricsContext;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.http.AbstractHandlerHook;
import io.cdap.http.HttpResponder;
import io.cdap.http.internal.HandlerInfo;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Records gateway requests/response metrics.
 */
public class MetricsReporterHook extends AbstractHandlerHook {
  private static final Logger LOG = LoggerFactory.getLogger(MetricsReporterHook.class);

  private final String serviceName;
  private final LoadingCache<Map<String, String>, MetricsContext> collectorCache;

  public MetricsReporterHook(MetricsCollectionService metricsCollectionService, String serviceName) {
    this.serviceName = serviceName;

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
      MetricsContext collector = collectorCache.get(createContext(handlerInfo));
      collector.increment("request.received", 1);
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
      MetricsContext collector = collectorCache.get(createContext(handlerInfo));
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
    } catch (Throwable e) {
      LOG.error("Got exception while getting collector", e);
    }
  }

  private Map<String, String> createContext(HandlerInfo handlerInfo) {
    // todo: really inefficient to call this on the intense data flow path
    return ImmutableMap.of(
      Constants.Metrics.Tag.NAMESPACE, NamespaceId.SYSTEM.getEntityName(),
      Constants.Metrics.Tag.COMPONENT, serviceName,
      Constants.Metrics.Tag.HANDLER, getSimpleName(handlerInfo.getHandlerName()),
      Constants.Metrics.Tag.METHOD, handlerInfo.getMethodName());
  }

  private String getSimpleName(String className) {
    int ind = className.lastIndexOf('.');
    return className.substring(ind + 1);
  }
}

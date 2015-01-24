/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.common.hooks;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.metrics.MetricsCollectionService;
import co.cask.cdap.common.metrics.MetricsCollector;
import co.cask.http.AbstractHandlerHook;
import co.cask.http.HandlerInfo;
import co.cask.http.HttpResponder;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Records gateway requests/response metrics.
 */
public class MetricsReporterHook extends AbstractHandlerHook {
  private static final Logger LOG = LoggerFactory.getLogger(MetricsReporterHook.class);

  private final MetricsCollectionService metricsCollectionService;

  private final String serviceName;

  private final LoadingCache<Map<String, String>, MetricsCollector> collectorCache;

  public MetricsReporterHook(final MetricsCollectionService metricsCollectionService, String serviceName) {
    this.metricsCollectionService = metricsCollectionService;
    this.serviceName = serviceName;

    if (metricsCollectionService != null) {
      this.collectorCache = CacheBuilder.newBuilder()
        .expireAfterAccess(1, TimeUnit.HOURS)
        .build(new CacheLoader<Map<String, String>, MetricsCollector>() {
          @Override
          public MetricsCollector load(Map<String, String> key) throws Exception {
            return metricsCollectionService.getCollector(key);
          }
        });
    } else {
      collectorCache = null;
    }
  }

  @Override
  public boolean preCall(HttpRequest request, HttpResponder responder, HandlerInfo handlerInfo) {
    if (metricsCollectionService != null) {
      try {
        MetricsCollector collector = collectorCache.get(createContext(handlerInfo));
        collector.increment("request.received", 1);
      } catch (Throwable e) {
        LOG.error("Got exception while getting collector", e);
      }
    }
    return true;
  }

  @Override
  public void postCall(HttpRequest request, HttpResponseStatus status, HandlerInfo handlerInfo) {
    if (metricsCollectionService != null) {
      try {
        MetricsCollector collector = collectorCache.get(createContext(handlerInfo));
        String name;
        int code = status.getCode();
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
  }

  private Map<String, String> createContext(HandlerInfo handlerInfo) {
    // todo: really inefficient to call this on the intense data flow path
    return ImmutableMap.of(
      Constants.Metrics.Tag.NAMESPACE, Constants.SYSTEM_NAMESPACE,
      Constants.Metrics.Tag.SERVICE, serviceName,
      Constants.Metrics.Tag.HANDLER, getSimpleName(handlerInfo.getHandlerName()),
      Constants.Metrics.Tag.METHOD, handlerInfo.getMethodName());
  }

  private String getSimpleName(String className) {
    int ind = className.lastIndexOf('.');
    return className.substring(ind + 1);
  }
}

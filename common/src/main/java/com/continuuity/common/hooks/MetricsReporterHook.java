package com.continuuity.common.hooks;

import com.continuuity.common.metrics.MetricsCollectionService;
import com.continuuity.common.metrics.MetricsCollector;
import com.continuuity.common.metrics.MetricsScope;
import com.continuuity.http.AbstractHandlerHook;
import com.continuuity.http.HandlerInfo;
import com.continuuity.http.HttpResponder;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Records gateway requests/response metrics.
 */
public class MetricsReporterHook extends AbstractHandlerHook {
  private static final Logger LOG = LoggerFactory.getLogger(MetricsReporterHook.class);

  private final MetricsCollectionService metricsCollectionService;

  private final String serviceName;

  private final LoadingCache<String, MetricsCollector> collectorCache;

  public MetricsReporterHook(final MetricsCollectionService metricsCollectionService, String serviceName) {
    this.metricsCollectionService = metricsCollectionService;
    this.serviceName = serviceName;

    if (metricsCollectionService != null) {
      this.collectorCache = CacheBuilder.newBuilder()
        .expireAfterAccess(1, TimeUnit.HOURS)
        .build(new CacheLoader<String, MetricsCollector>() {
          @Override
          public MetricsCollector load(String key) throws Exception {
            return metricsCollectionService.getCollector(MetricsScope.REACTOR, key, "0");
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
        collector.gauge("request.received", 1);
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
        collector.gauge("response." + name, 1, "status:" + code);
      } catch (Throwable e) {
        LOG.error("Got exception while getting collector", e);
      }
    }
  }

  private String createContext(HandlerInfo handlerInfo) {
    return String.format("%s.%s.%s", serviceName, getSimpleName(handlerInfo.getHandlerName()),
                         handlerInfo.getMethodName());
  }

  private String getSimpleName(String className) {
    int ind = className.lastIndexOf('.');
    return className.substring(ind + 1);
  }
}

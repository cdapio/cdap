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

package io.cdap.cdap.common.auditlogging;

import io.cdap.cdap.api.feature.FeatureFlagsProvider;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.feature.DefaultFeatureFlagsProvider;
import io.cdap.cdap.common.http.HttpHeaderNames;
import io.cdap.cdap.features.Feature;
import io.cdap.cdap.security.spi.authentication.SecurityRequestContext;
import io.cdap.cdap.security.spi.authorization.AuditLogRequest;
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
 * Sets
 */
public class AuditLogSetterHook extends AbstractHandlerHook {

  private static final Logger LOG = LoggerFactory.getLogger(AuditLogSetterHook.class);

  private final String serviceName;

  private final FeatureFlagsProvider featureFlagsProvider;

  public AuditLogSetterHook(CConfiguration cConf, String serviceName) {
    this.serviceName = serviceName;
    this.featureFlagsProvider = new DefaultFeatureFlagsProvider(cConf);
  }

  @Override
  public void postCall(HttpRequest request, HttpResponseStatus status, HandlerInfo handlerInfo) {
    if (!Feature.DATAPLANE_AUDIT_LOGGING.isEnabled(featureFlagsProvider)){
      return;
    }

    String startTimeStr = request.headers().get(HttpHeaderNames.CDAP_REQ_TIMESTAMP_HDR);
    Long endTimeNanos = System.nanoTime();
    Long startTimeNanos = null;
    if (startTimeStr != null) {
      startTimeNanos = Long.parseLong(startTimeStr);
    } else {
      startTimeNanos = endTimeNanos;
    }
    long endTime = System.currentTimeMillis() * 1000000;
    long startTime = endTime - (endTimeNanos - startTimeNanos);

    LOG.trace("Setting a Audit Log event to published in SecurityRequestContext");
    SecurityRequestContext.setAuditLogRequest(
      new AuditLogRequest(
        status.code(),
        SecurityRequestContext.getUserIp(),
        request.uri(),
        getSimpleName(handlerInfo.getHandlerName()),
        handlerInfo.getMethodName(),
        request.method().name(),
        SecurityRequestContext.getAuditLogQueue(),
        startTime,
        endTime
      )
    );
  }

  private String getSimpleName(String className) {
    int ind = className.lastIndexOf('.');
    return className.substring(ind + 1);
  }
}

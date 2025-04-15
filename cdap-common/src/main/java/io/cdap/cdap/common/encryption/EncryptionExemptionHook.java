/*
 * Copyright © 2025 Cask Data, Inc.
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

package io.cdap.cdap.common.encryption;

import com.google.common.collect.ImmutableList;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.http.HttpHeaderNames;
import io.cdap.cdap.security.spi.authentication.SecurityRequestContext;
import io.cdap.http.AbstractHandlerHook;
import io.cdap.http.HttpResponder;
import io.cdap.http.internal.HandlerInfo;
import io.netty.handler.codec.http.HttpRequest;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sets encryption exception metadata to {@link SecurityRequestContext}.
 */
public class EncryptionExemptionHook extends AbstractHandlerHook {
  private static final Logger LOG = LoggerFactory.getLogger(EncryptionExemptionHook.class);
  private final String serviceName;

  private static final List<Pattern> EXEMPTED_URIS = ImmutableList.of(
      Pattern.compile("/v3Internal/namespaces/([^/]+)/artifacts/([^/]+)/versions/([^/]+)(/.*)?$"),
      Pattern.compile("/v3/namespaces/([^/]+)/artifacts/([^/]+)/versions/([^/]+)(/.*)?$"),
      Pattern.compile("/v3Internal/namespaces/([^/]+)/credentials/workloadIdentity/provision$"),
      Pattern.compile("/v3Internal/namespaces/([^/]+)/preferences([^/]+)"),
      Pattern.compile("/v3/namespaces/([^/]+)/securekeys/([^/]+)(/.*)?$")
  );

  public EncryptionExemptionHook(CConfiguration cConf, String serviceName) {
    this.serviceName = serviceName;
  }

  @Override
  public boolean preCall(HttpRequest request, HttpResponder responder, HandlerInfo handlerInfo) {
    try {
      for (Pattern uriPattern : EXEMPTED_URIS) {
        Matcher matcher = uriPattern.matcher(request.uri());
        if (matcher.matches()) {
          // For any pattern match, set the header to false to prevent Unauthenticated exception after decryption
          request.headers().set(HttpHeaderNames.TASK_WORKER_DECRYPTION_HDR, "false");
          return true;
        }
      }
    } catch (Throwable e) {
      LOG.error("Encountered exception while pattern matching for URI {}", request.uri(), e);
    }

    request.headers().set(HttpHeaderNames.TASK_WORKER_DECRYPTION_HDR, "true");
    return true;
  }
}

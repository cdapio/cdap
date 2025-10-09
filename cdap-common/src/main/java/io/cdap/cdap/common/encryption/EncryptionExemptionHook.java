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
import io.cdap.cdap.common.http.HttpHeaderNames;
import io.cdap.cdap.security.spi.authentication.SecurityRequestContext;
import io.cdap.http.AbstractHandlerHook;
import io.cdap.http.HttpResponder;
import io.cdap.http.internal.HandlerInfo;
import io.netty.handler.codec.http.HttpRequest;
import java.util.List;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sets encryption exception metadata to {@link SecurityRequestContext}.
 */
public class EncryptionExemptionHook extends AbstractHandlerHook {

  private static final Logger LOG = LoggerFactory.getLogger(EncryptionExemptionHook.class);

  private static final List<Pattern> SHARED_WORKER_EXEMPTED_URIS = ImmutableList.of(
      Pattern.compile("/v3/namespaces/([^/]+)/artifacts/([^/]+)/versions/([^/]+)(/.*)?$"),
      Pattern.compile("/v3Internal/namespaces/([^/]+)/artifacts/([^/]+)/versions/([^/]+)(/.*)?$"),
      Pattern.compile(
          "/v3Internal/namespaces/([^/]+)/credentials/workloadIdentity/provision(\\?scopes=(.*))?$"),
      Pattern.compile("/v3/namespaces/([^/]+)/apps/([^/]+)/services/([^/]+)/methods"
          + "/v1/contexts/([^/]+)/connections(?:/.*)?$"),
      Pattern.compile(
          "/v3/namespaces/([^/]+)/apps/([^/]+)/services/([^/]+)/methods"
              + "/v1/oauth/provider/([^/]+)(?:/authurl|/credential/([^/]+)(?:/valid)?)?$"));
  private static final List<Pattern> TASK_WORKER_EXEMPTED_URIS = ImmutableList.of(
      Pattern.compile("/v3/namespaces/([^/]+)/securekeys/([^/]+)(/.*)?$"),
      Pattern.compile("/v3Internal/namespaces/([^/]+)/preferences([^/]+)"));
  private static final List<Pattern> PREVIEW_RUNNER_EXEMPTED_URIS = ImmutableList.of(
      Pattern.compile("/v3Internal/namespaces/([^/]+)/artifacts/([^/]+)/versions(\\?.*)?$"),
      Pattern.compile(
          "/v3Internal/namespaces/([^/]+)/apps/([^/]+)/workflows/([^/]+)/preferences(\\?.*)?$"),
      Pattern.compile("/v3/namespaces/([^/]+)/data/datasets/([^/]+)$"),
      Pattern.compile("/v1/namespaces/system/topics/preview/publish"));

  @Override
  public boolean preCall(HttpRequest request, HttpResponder responder, HandlerInfo handlerInfo) {
    WorkerDecryptionScope exemptionType = WorkerDecryptionScope.NONE;
    String uri = request.uri();

    if (isUriExempt(uri, SHARED_WORKER_EXEMPTED_URIS)) {
      exemptionType = WorkerDecryptionScope.ANY_WORKER;
    } else if (isUriExempt(uri, PREVIEW_RUNNER_EXEMPTED_URIS)) {
      exemptionType = WorkerDecryptionScope.PREVIEW_RUNNER;
    } else if (isUriExempt(uri, TASK_WORKER_EXEMPTED_URIS)) {
      exemptionType = WorkerDecryptionScope.TASK_WORKER;
    }

    // Set the header with the determined exemption type.
    request.headers().set(HttpHeaderNames.WORKER_DECRYPTION_HDR, exemptionType.name());
    LOG.trace("URI {} exemption type set to {}", uri, exemptionType);
    return true;
  }

  private boolean isUriExempt(String uri, List<Pattern> exemptionList) {
    for (Pattern pattern : exemptionList) {
      if (pattern.matcher(uri).matches()) {
        return true;
      }
    }
    return false;
  }
}

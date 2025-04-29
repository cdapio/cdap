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

import static io.cdap.cdap.common.http.HttpHeaderNames.TASK_WORKER_DECRYPTION_HDR;

import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.http.internal.HandlerInfo;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpVersion;
import org.junit.Assert;
import org.junit.Test;

public class EncryptionExemptionHookTest {
  private static final String TESTSERVICENAME = "test.Service";
  private static final String TESTHANDLERNAME = "test.handler";
  private static final String TESTMETHODNAME = "testMethod";

  @Test
  public void testPatternMatchingSuccessful() {
    HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET,
        "/v3/namespaces/default/securekeys/personal-token");
    HandlerInfo handlerInfo = new HandlerInfo(TESTHANDLERNAME, TESTMETHODNAME);
    EncryptionExemptionHook hook = new EncryptionExemptionHook(CConfiguration.create(), TESTSERVICENAME);

    hook.preCall(request, null, handlerInfo);

    Assert.assertFalse(Boolean.parseBoolean(request.headers().get(TASK_WORKER_DECRYPTION_HDR)));
  }

  @Test
  public void testPatternMatchingUnsuccessful() {
    HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "testingUri");
    HandlerInfo handlerInfo = new HandlerInfo(TESTHANDLERNAME, TESTMETHODNAME);
    EncryptionExemptionHook hook = new EncryptionExemptionHook(CConfiguration.create(), TESTSERVICENAME);

    hook.preCall(request, null, handlerInfo);

    Assert.assertTrue(Boolean.parseBoolean(request.headers().get(TASK_WORKER_DECRYPTION_HDR)));
  }
}

/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.common.security;

import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.security.HttpsEnabler;
import io.cdap.http.NettyHttpService;

import javax.annotation.Nullable;
import javax.net.ssl.HttpsURLConnection;

/**
 * A builder style class to help enabling HTTPS for server and client.
 * <p>
 * This is a wrapper around {@link HttpsEnabler}
 */
public class HttpsConfigurer {
  private final CConfiguration cConf;
  private final SConfiguration sConf;
  private final HttpsEnabler httpsEnabler;

  public HttpsCfonfigurer(@Nullable CConfiguration cConf, @Nullable SConfiguration sConf) {
    this.cConf = cConf;
    this.sConf = sConf;
    httpsEnabler = new HttpsEnabler();
  }

  /**
   * A wrapper around {@link HttpsEnabler#enable(NettyHttpService.Builder)}.
   *
   * @param builder the builder to update
   * @param <T> type of the builder
   * @return the builder from the parameter
   */
  public <T extends NettyHttpService.Builder> T enable(T builder) {
    String filePath = cConf.get(Constants.Security.SSL.INTERNAL_CERT_PATH);
    String password = sConf.get(Constants.Security.SSL.INTERNAL_CERT_PASSWORD, "");
    return httpsEnabler.configureKeyStore(filePath, password).enable(builder);
  }

  /**
   * A wrapper around {@link HttpsEnabler#enable(HttpsURLConnection)}.
   *
   * @param urlConn the {@link HttpsURLConnection} to update
   * @return the urlConn from the parameter
   * @throws RuntimeException if failed to enable HTTPS
   */
  public HttpsURLConnection enable(HttpsURLConnection urlConn) {
    return httpsEnabler.enable(urlConn);
  }

  /**
   * A wrapper around {@link HttpsEnabler#setTrustAll(boolean)}
   *
   * @param trustAny if {@link true} it will trust any server;
   * otherwise it will based on the trust store or use the default trust chain.
   * @return this instance
   */
  public synchronized HttpsConfigurer setTrustAll(boolean trustAny) {
    httpsEnabler.setTrustAll(trustAny);
    return this;
  }
}

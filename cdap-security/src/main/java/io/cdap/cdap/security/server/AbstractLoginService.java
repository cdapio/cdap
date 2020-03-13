/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.security.server;

import com.google.inject.Guice;
import com.google.inject.Injector;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.guice.ConfigModule;
import org.apache.commons.text.WordUtils;
import org.eclipse.jetty.security.LoginService;
import org.eclipse.jetty.util.component.AbstractLifeCycle;
import org.eclipse.jetty.util.log.Log;
import org.eclipse.jetty.util.log.Logger;

import javax.annotation.Nullable;

/** AbstractLoginService
 * AbstractLoginService to provide support for username case conversion
 */
public abstract class AbstractLoginService extends AbstractLifeCycle implements LoginService {

  private static final Logger LOG = Log.getLogger(AbstractLoginService.class);
  protected CConfiguration cConfiguration;
  protected String usernameCaseConversion = "";
  protected char[] usernameCamelCaseConversionDelimiterArr = {'.'};

  protected AbstractLoginService() {
    Injector injector = Guice.createInjector(new ConfigModule());
    this.cConfiguration = injector.getInstance(CConfiguration.class);

    String usernameCaseConversion =
            cConfiguration.get(Constants.Security.AuthenticationServer.USERNAME_CASE_CONVERSION_TYPE);
    if (usernameCaseConversion != null) {
      this.usernameCaseConversion = usernameCaseConversion.toLowerCase();
    }

    String usernameCamelCaseConversionDelimiters =
            cConfiguration.get(Constants.Security.AuthenticationServer.USERNAME_TITLECASE_DELIMITER);
    if (usernameCamelCaseConversionDelimiters != null && usernameCamelCaseConversionDelimiters.length() > 0) {
      this.usernameCamelCaseConversionDelimiterArr = usernameCamelCaseConversionDelimiters.toCharArray();
    }

  }

  @Nullable
  protected String usernameCaseConvert(String username) {

    if (username == null || usernameCaseConversion.equalsIgnoreCase("")) {
      return username;
    }
    switch (usernameCaseConversion.toLowerCase()) {
      case "lower":
        username = username.toLowerCase();
        break;
      case "upper":
        username = username.toUpperCase();
        break;
      case "title":
        username = WordUtils.capitalizeFully(username, usernameCamelCaseConversionDelimiterArr);
        break;
      case "server":
        username = serverCaseConvert(username);
        break;
      default:
        LOG.debug("no match for type: '{}'", username);
    }
    return username;
  }

  public abstract String serverCaseConvert(String username);
}

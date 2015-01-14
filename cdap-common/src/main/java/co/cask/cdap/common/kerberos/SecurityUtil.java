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

package co.cask.cdap.common.kerberos;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import com.google.common.base.Preconditions;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosUtil;
import org.apache.zookeeper.client.ZooKeeperSaslClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginException;

/**
 * Utility functions for Kerberos.
 */
public final class SecurityUtil {

  private static final Logger LOG = LoggerFactory.getLogger(SecurityUtil.class);

  private SecurityUtil() { }

  /**
   * Enables Kerberos authentication based on configuration.
   *
   * @param cConf configuration object.
   */
  public static void enableKerberosLogin(CConfiguration cConf) throws IOException {
    if (System.getProperty(Constants.External.JavaSecurity.ENV_AUTH_LOGIN_CONFIG) != null) {
      LOG.warn("Environment variable '{}' was already set to {}. Not generating JAAS configuration.",
               Constants.External.JavaSecurity.ENV_AUTH_LOGIN_CONFIG,
               System.getProperty(Constants.External.JavaSecurity.ENV_AUTH_LOGIN_CONFIG));
      return;
    }

    if (!isKerberosEnabled(cConf)) {
      LOG.info("Kerberos login is not enabled. To enable Kerberos login, enable {} and configure {} and {}",
               Constants.Security.KERBEROS_ENABLED, Constants.Security.CFG_CDAP_MASTER_KRB_PRINCIPAL,
               Constants.Security.CFG_CDAP_MASTER_KRB_KEYTAB_PATH);
      return;
    }

    Preconditions.checkArgument(cConf.get(Constants.Security.CFG_CDAP_MASTER_KRB_PRINCIPAL) != null,
                                "Kerberos authentication is enabled, but " +
                                Constants.Security.CFG_CDAP_MASTER_KRB_PRINCIPAL + " is not configured");

    String principal = cConf.get(Constants.Security.CFG_CDAP_MASTER_KRB_PRINCIPAL);
    principal = SecurityUtil.expandPrincipal(principal);

    Preconditions.checkArgument(cConf.get(Constants.Security.CFG_CDAP_MASTER_KRB_KEYTAB_PATH) != null,
                                "Kerberos authentication is enabled, but " +
                                Constants.Security.CFG_CDAP_MASTER_KRB_KEYTAB_PATH + " is not configured");

    File keyTabFile = new File(cConf.get(Constants.Security.CFG_CDAP_MASTER_KRB_KEYTAB_PATH));
    Preconditions.checkArgument(keyTabFile.exists(),
                                "Kerberos keytab file does not exist: " + keyTabFile.getAbsolutePath());
    Preconditions.checkArgument(keyTabFile.isFile(),
                                "Kerberos keytab file should be a file: " + keyTabFile.getAbsolutePath());
    Preconditions.checkArgument(keyTabFile.canRead(),
                                "Kerberos keytab file cannot be read: " + keyTabFile.getAbsolutePath());

    LOG.info("Using Kerberos principal {} and keytab {}", principal, keyTabFile.getAbsolutePath());

    System.setProperty(Constants.External.Zookeeper.ENV_AUTH_PROVIDER_1,
                       "org.apache.zookeeper.server.auth.SASLAuthenticationProvider");
    System.setProperty(Constants.External.Zookeeper.ENV_ALLOW_SASL_FAILED_CLIENTS, "true");
    System.setProperty(ZooKeeperSaslClient.LOGIN_CONTEXT_NAME_KEY, "Client");

    final Map<String, String> properties = new HashMap<String, String>();
    properties.put("doNotPrompt", "true");
    properties.put("useKeyTab", "true");
    properties.put("useTicketCache", "false");
    properties.put("principal", principal);
    properties.put("keyTab", keyTabFile.getAbsolutePath());

    final AppConfigurationEntry configurationEntry = new AppConfigurationEntry(
      KerberosUtil.getKrb5LoginModuleName(), AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, properties);

    Configuration configuration = new Configuration() {
      @Override
      public AppConfigurationEntry[] getAppConfigurationEntry(String s) {
        return new AppConfigurationEntry[] { configurationEntry };
      }
    };

    // apply the configuration
    Configuration.setConfiguration(configuration);
  }

  /**
   * Expands _HOST in principal name with local hostname.
   *
   * @param principal Kerberos principal name
   * @return expanded principal name
   * @throws UnknownHostException if the local hostname could not be resolved into an address.
   */
  @Nullable
  public static String expandPrincipal(@Nullable String principal) throws UnknownHostException {
    if (principal == null) {
      return principal;
    }

    String localHostname = InetAddress.getLocalHost().getCanonicalHostName();
    return principal.replace("/_HOST@", "/" + localHostname + "@");
  }

  /**
   * @param cConf CConfiguration object.
   * @return true, if Kerberos is enabled.
   */
  public static boolean isKerberosEnabled(CConfiguration cConf) {
    return cConf.getBoolean(Constants.Security.KERBEROS_ENABLED,
                            cConf.getBoolean(Constants.Security.ENABLED));
  }

  public static void loginForMasterService(CConfiguration cConf) throws IOException, LoginException {
    String principal = SecurityUtil.expandPrincipal(cConf.get(Constants.Security.CFG_CDAP_MASTER_KRB_PRINCIPAL));
    String keytabPath = cConf.get(Constants.Security.CFG_CDAP_MASTER_KRB_KEYTAB_PATH);

    if (UserGroupInformation.isSecurityEnabled()) {
      UserGroupInformation.loginUserFromKeytab(principal, keytabPath);
    }
  }
}

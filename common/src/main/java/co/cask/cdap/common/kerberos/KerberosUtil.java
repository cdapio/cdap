/*
 * Copyright 2014 Cask, Inc.
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

import co.cask.cdap.common.conf.Constants;
import com.google.common.base.Preconditions;
import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.client.ZooKeeperSaslClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * Utility functions for Kerberos.
 */
public class KerberosUtil {

  private static final Logger LOG = LoggerFactory.getLogger(KerberosUtil.class);

  /**
   * Enables Kerberos authentication.
   *
   * @param tmpDir temporary directory to write jaas.conf to
   * @param keyTabFile Kerberos keytab file
   * @param principal Kerberos principal corresponding to the keytab file
   */
  public static void enable(File tmpDir, File keyTabFile, String principal) throws IOException {
    if (System.getProperty(Constants.External.JavaSecurity.ENV_AUTH_LOGIN_CONFIG) != null) {
      LOG.warn("Environment variable '{}' was already set to {}. This may cause unexpected behavior in Java security.",
               Constants.External.JavaSecurity.ENV_AUTH_LOGIN_CONFIG,
               System.getProperty(Constants.External.JavaSecurity.ENV_AUTH_LOGIN_CONFIG));
    }

    Preconditions.checkArgument(keyTabFile != null, "Kerberos keytab file is required");
    Preconditions.checkArgument(keyTabFile.exists(),
                                "Kerberos keytab file does not exist: " + keyTabFile.getAbsolutePath());
    Preconditions.checkArgument(keyTabFile.isFile(),
                                "Kerberos keytab file should be a file: " + keyTabFile.getAbsolutePath());
    Preconditions.checkArgument(keyTabFile.canRead(),
                                "Kerberos keytab file cannot be read: " + keyTabFile.getAbsolutePath());

    System.setProperty(Constants.External.Zookeeper.ENV_AUTH_PROVIDER_1,
                       "org.apache.zookeeper.server.auth.SASLAuthenticationProvider");
    System.setProperty(Constants.External.Zookeeper.ENV_ALLOW_SASL_FAILED_CLIENTS, "true");
    System.setProperty(ZooKeeperSaslClient.LOGIN_CONTEXT_NAME_KEY, "Client");

    // Generate the jaas.conf file which will be used by the Kerberos client library for authentication,
    // due to setting the environment variable "java.security.auth.login.config"
    File saslConfFile = new File(tmpDir, "jaas.conf");
    PrintWriter writer = new PrintWriter(new FileWriter(saslConfFile));

    try {
      writer.printf("Client {\n");
      writer.printf("  com.sun.security.auth.module.Krb5LoginModule required\n");
      writer.printf("  useKeyTab=true\n");
      writer.printf("  keyTab=\"%s\"\n", keyTabFile.getAbsolutePath());
      writer.printf("  useTicketCache=false\n");
      writer.printf("  principal=\"%s\";\n", principal);
      writer.printf("};\n");
    } finally {
      writer.close();
    }

    System.setProperty(Constants.External.JavaSecurity.ENV_AUTH_LOGIN_CONFIG, saslConfFile.getAbsolutePath());
    LOG.debug("Set {} to {}", Constants.External.JavaSecurity.ENV_AUTH_LOGIN_CONFIG, saslConfFile.getAbsolutePath());
    LOG.debug("Contents of {} file:\n{}", Constants.External.JavaSecurity.ENV_AUTH_LOGIN_CONFIG,
              FileUtils.readFileToString(saslConfFile));
  }
}

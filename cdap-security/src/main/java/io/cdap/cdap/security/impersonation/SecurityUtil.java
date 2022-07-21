/*
 * Copyright © 2017-2021 Cask Data, Inc.
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

package io.cdap.cdap.security.impersonation;

import io.cdap.cdap.api.security.AccessException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.proto.id.KerberosPrincipalId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.NamespacedEntityId;
import io.cdap.cdap.security.auth.AuthenticationMode;
import io.cdap.cdap.security.spi.AccessIOException;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.hadoop.security.authentication.util.KerberosUtil;
import org.apache.twill.common.Threads;
import org.apache.zookeeper.client.ZooKeeperSaslClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;

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

    String principal = expandPrincipal(getMasterPrincipal(cConf));
    File keytabFile = getMasterKeytabFile(cConf);

    LOG.info("Using Kerberos principal {} and keytab {}", principal, keytabFile.getAbsolutePath());

    System.setProperty(Constants.External.Zookeeper.ENV_AUTH_PROVIDER_1,
                       "org.apache.zookeeper.server.auth.SASLAuthenticationProvider");
    System.setProperty(Constants.External.Zookeeper.ENV_ALLOW_SASL_FAILED_CLIENTS, "true");
    System.setProperty(ZooKeeperSaslClient.LOGIN_CONTEXT_NAME_KEY, "Client");

    final Map<String, String> properties = new HashMap<>();
    properties.put("doNotPrompt", "true");
    properties.put("useKeyTab", "true");
    properties.put("useTicketCache", "false");
    properties.put("principal", principal);
    properties.put("keyTab", keytabFile.getAbsolutePath());

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
      return null;
    }

    String localHostname = InetAddress.getLocalHost().getCanonicalHostName();
    return principal.replace("/_HOST@", "/" + localHostname + "@");
  }

  /**
   * Check if {@link Constants.Security#KERBEROS_ENABLED} is set. The value is default to the value of
   * {@link Constants.Security#ENABLED}.
   *
   * @param cConf CConfiguration object.
   * @return true, if Kerberos is enabled.
   */
  public static boolean isKerberosEnabled(CConfiguration cConf) {
    return cConf.getBoolean(Constants.Security.KERBEROS_ENABLED, cConf.getBoolean(Constants.Security.ENABLED));
  }

  /**
   * Checks if perimeter security is enabled in managed mode.
   *
   * @return {@code true} if security enabled in managed mode
   * @see Constants.Security#ENABLED
   * @see Constants.Security.Authentication#MODE
   */
  public static boolean isManagedSecurity(CConfiguration cConf) {
    return cConf.getBoolean(Constants.Security.ENABLED)
      && cConf.getEnum(Constants.Security.Authentication.MODE,
                       AuthenticationMode.MANAGED).equals(AuthenticationMode.MANAGED);
  }

  /**
   * Checks if perimeter security is enabled in proxy mode.
   *
   * @return {@code true} if security enabled in proxy mode
   * @see Constants.Security#ENABLED
   * @see Constants.Security.Authentication#MODE
   */
  public static boolean isProxySecurity(CConfiguration cConf) {
    return cConf.getBoolean(Constants.Security.ENABLED)
      && cConf.getEnum(Constants.Security.Authentication.MODE,
                       AuthenticationMode.MANAGED).equals(AuthenticationMode.PROXY);
  }

  /**
   * Checks if internal authenticated communication should be enforced.
   *
   * @return {@code true} if internal auth is enabled.
   */
  public static boolean isInternalAuthEnabled(CConfiguration cConf) {
    return cConf.getBoolean(Constants.Security.INTERNAL_AUTH_ENABLED);
  }

  public static String getMasterPrincipal(CConfiguration cConf) {
    String principal = cConf.get(Constants.Security.CFG_CDAP_MASTER_KRB_PRINCIPAL);
    if (principal == null) {
      throw new IllegalArgumentException("Kerberos authentication is enabled, but " +
                                           Constants.Security.CFG_CDAP_MASTER_KRB_PRINCIPAL + " is not configured");
    }
    return principal;
  }

  public static String getMasterKeytabURI(CConfiguration cConf) {
    String uri = cConf.get(Constants.Security.CFG_CDAP_MASTER_KRB_KEYTAB_PATH);
    if (uri == null) {
      throw new IllegalArgumentException(Constants.Security.CFG_CDAP_MASTER_KRB_KEYTAB_PATH + " is not configured");
    }
    return uri;
  }

  public static File getMasterKeytabFile(CConfiguration cConf) {
    File keytabFile = new File(getMasterKeytabURI(cConf));
    if (!Files.isReadable(keytabFile.toPath())) {
      throw new IllegalArgumentException("Keytab file is not a readable file " + keytabFile);
    }
    return keytabFile;
  }

  public static void loginForMasterService(CConfiguration cConf) throws IOException {
    if (!isKerberosEnabled(cConf) || !UserGroupInformation.isSecurityEnabled()) {
      return;
    }

    String expandedPrincipal = expandPrincipal(getMasterPrincipal(cConf));
    File keytabFile = getMasterKeytabFile(cConf);

    LOG.info("Logging in as: principal={}, keytab={}", expandedPrincipal, keytabFile);
    UserGroupInformation.loginUserFromKeytab(expandedPrincipal, keytabFile.getAbsolutePath());

    long delaySec = cConf.getLong(Constants.Security.KERBEROS_KEYTAB_RELOGIN_INTERVAL);
    Executors.newSingleThreadScheduledExecutor(Threads.createDaemonThreadFactory("Kerberos keytab renewal"))
      .scheduleWithFixedDelay(() -> {
        try {
          UserGroupInformation.getLoginUser().checkTGTAndReloginFromKeytab();
        } catch (IOException e) {
          LOG.error("Failed to relogin from keytab", e);
        }
      }, delaySec, delaySec, TimeUnit.SECONDS);
  }

  /**
   * Returns a {@link KerberosName} from the given {@link KerberosPrincipalId} if the given kerberos principal id
   * is valid. Refer to
   * <a href="https://web.mit.edu/kerberos/krb5-1.5/krb5-1.5.4/doc/krb5-user/What-is-a-Kerberos-Principal_003f.html">
   * Kerberos Principal</a> for details.
   *
   * @param principalId The {@link KerberosPrincipalId} from which {@link KerberosName} needs to be created
   * @return {@link KerberosName} for the given {@link KerberosPrincipalId}
   * @throws IllegalArgumentException if failed to create a {@link KerberosName} from the given
   * {@link KerberosPrincipalId}
   */
  public static KerberosName getKerberosName(KerberosPrincipalId principalId) {
    return new KerberosName(principalId.getPrincipal());
  }

  /**
   * Checks if the given {@link KerberosPrincipalId} is valid or not by calling
   * {@link #getKerberosName(KerberosPrincipalId)}. This is just a wrapper around
   * {@link #getKerberosName(KerberosPrincipalId)} to not return an object to the caller for simplicity.
   *
   * @param principalId {@link KerberosPrincipalId} which needs to be validated
   * @throws IllegalArgumentException if failed to create a {@link KerberosName} from the given
   * {@link KerberosPrincipalId}
   */
  public static void validateKerberosPrincipal(KerberosPrincipalId principalId) {
    getKerberosName(principalId);
  }

  /**
   * @param principal The principal whose KeytabURI is being looked up
   * @param cConf To lookup the configured path for the keytabs
   * @return The location of the keytab
   * @throws IOException If the principal is not a valid kerberos principal
   */
  static String getKeytabURIforPrincipal(String principal, CConfiguration cConf) throws AccessException {
    try {
      String confPath = cConf.getRaw(Constants.Security.KEYTAB_PATH);
      if (confPath == null) {
        throw new IllegalArgumentException(String.format("Failed to get a valid keytab path. " +
                                                           "Please ensure that you have specified %s in cdap-site.xml",
                                                         Constants.Security.KEYTAB_PATH));
      }
      String name = new KerberosName(principal).getShortName();
      return confPath.replace(Constants.USER_NAME_SPECIFIER, name);
    } catch (IOException e) {
      throw new AccessIOException(e);
    }
  }

  /**
   * This has the logic to construct an impersonation info as follows:
   * <ul>
   * <li>If the ownerAdmin has an owner and a keytab URI, return this information</li>
   * <li>Else the ownerAdmin does not have an owner for this entity.
   * Return the master impersonation info as found in the cConf</li>
   * </ul>
   */
  public static ImpersonationInfo createImpersonationInfo(OwnerAdmin ownerAdmin, CConfiguration cConf,
                                                          NamespacedEntityId entityId) throws AccessException {
    ImpersonationInfo impersonationInfo = ownerAdmin.getImpersonationInfo(entityId);
    if (impersonationInfo == null) {
      // here we don't need to get the keytab file since we use delegation tokens accross system containers
      return new ImpersonationInfo(getMasterPrincipal(cConf), getMasterKeytabURI(cConf));
    }
    return impersonationInfo;
  }

  /**
   * <p>Verifies the owner principal of an entity is same as the owner specified during entity creation. If an owner
   * was not specified during entity creation but is being specified later (i.e. during updating properties etc) the
   * specified owner principal is same as the effective impersonating principal.</p>
   * <p>Note: This method should not be called for an non-existing entity for example while the entity is being
   * created.</p>
   * @param existingEntity the existing entity whose owner principal is being verified
   * @param specifiedOwnerPrincipal the specified principal
   * @param ownerAdmin {@link OwnerAdmin}
   * @throws IOException  if failed to query the given ownerAdmin
   * @throws UnauthorizedException if the specified owner information is not valid with the existing
   * impersonation principal
   */
  public static void verifyOwnerPrincipal(NamespacedEntityId existingEntity,
                                          @Nullable String specifiedOwnerPrincipal,
                                          OwnerAdmin ownerAdmin)
    throws IOException, UnauthorizedException {
    // if an owner principal was not specified then ensure that a direct owner doesn't exist. Although, if an owner
    // principal was specified then it must be equal to the effective impersonating principal of this entity
    if (!((specifiedOwnerPrincipal == null && ownerAdmin.getOwnerPrincipal(existingEntity) == null) ||
      Objects.equals(specifiedOwnerPrincipal, ownerAdmin.getImpersonationPrincipal(existingEntity)))) {
      // Not giving existing owner information as it might be unacceptable under some security scenarios
      throw new UnauthorizedException(String.format("%s '%s' already exists and the specified %s '%s' is not the " +
                                                      "same as the existing one. The %s of an entity cannot be " +
                                                      "changed.",
                                                    existingEntity.getEntityType(), existingEntity.getEntityName(),
                                                    Constants.Security.PRINCIPAL, specifiedOwnerPrincipal,
                                                    Constants.Security.PRINCIPAL));
    }
  }

  /**
   * Helper function to get the effective owner of an entity. It will check the owner store to get the namespace
   * owner if the provided owner principal is null.
   *
   * Note that this method need not be used after the entity is created, in that case simply
   * use {@link OwnerAdmin}.getImpersonationPrincipal()
   *
   * @param ownerAdmin owner admin to query the owner
   * @param namespaceId the namespace the entity is in
   * @param ownerPrincipal the owner principal of the entity, null if not provided
   * @return the effective owner of the entity, null if no owner is provided for both the enity and the namespace.
   */
  @Nullable
  public static KerberosPrincipalId getEffectiveOwner(OwnerAdmin ownerAdmin, NamespaceId namespaceId,
                                                      @Nullable String ownerPrincipal) throws IOException {
    if (ownerPrincipal != null) {
      // if entity owner is present, return it
      return new KerberosPrincipalId(ownerPrincipal);
    }

    if (!namespaceId.equals(NamespaceId.SYSTEM)) {
      // if entity owner is not present, get the namespace impersonation principal
      String namespacePrincipal = ownerAdmin.getImpersonationPrincipal(namespaceId);
      return namespacePrincipal == null ? null : new KerberosPrincipalId(namespacePrincipal);
    }

    // No owner found
    return null;
  }
}

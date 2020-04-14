/*
 * Copyright © 2016-2020 Cask Data, Inc.
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

import com.google.gson.Gson;
import com.google.inject.Inject;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.FeatureDisabledException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.common.service.Retries;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.common.utils.FileUtils;
import io.cdap.cdap.internal.app.runtime.SystemArguments;
import io.cdap.cdap.internal.app.store.RunRecordDetail;
import io.cdap.cdap.proto.NamespaceConfig;
import io.cdap.cdap.proto.element.EntityType;
import io.cdap.cdap.proto.id.NamespacedEntityId;
import io.cdap.cdap.proto.id.ProgramRunId;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Provides a UGI by logging in with a keytab file for that user.
 */
public class DefaultUGIProvider extends AbstractCachedUGIProvider {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultUGIProvider.class);

  private final LocationFactory locationFactory;
  private final File tempDir;
  private final NamespaceQueryAdmin namespaceQueryAdmin;
  private final Store store;
  private static final Gson gson = new Gson();

  @Inject
  DefaultUGIProvider(CConfiguration cConf, LocationFactory locationFactory, OwnerAdmin ownerAdmin,
                     NamespaceQueryAdmin namespaceQueryAdmin, Store store) {
    super(cConf, ownerAdmin);
    this.locationFactory = locationFactory;
    this.tempDir = new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR),
                            cConf.get(Constants.AppFabric.TEMP_DIR)).getAbsoluteFile();
    this.namespaceQueryAdmin = namespaceQueryAdmin;
    this.store = store;
  }

  /**
   * On master side, we can cache the explore request
   */
  @Override
  protected boolean checkExploreAndDetermineCache(ImpersonationRequest impersonationRequest) throws IOException {
    if (impersonationRequest.getEntityId().getEntityType().equals(EntityType.NAMESPACE) &&
      impersonationRequest.getImpersonatedOpType().equals(ImpersonatedOpType.EXPLORE)) {
      // CDAP-8355 If the operation being impersonated is an explore query then check if the namespace configuration
      // specifies that it can be impersonated with the namespace owner.
      // This is done here rather than in the get getConfiguredUGI because the getConfiguredUGI will be called at
      // remote location as in RemoteUGIProvider. Looking up namespace meta there will make a trip to master followed
      // by one to get the credentials. Hence do it here in the DefaultUGIProvider which is running in master.
      // Although, this will cause cache miss for explore implementation if the namespace config doesn't allow
      // impersonating the namespace owner for explore queries but that a trade-off to avoid multiple remote calls in
      // more prominent calls.
      try {
        NamespaceConfig nsConfig =
          namespaceQueryAdmin.get(impersonationRequest.getEntityId().getNamespaceId()).getConfig();
        if (!nsConfig.isExploreAsPrincipal()) {
          throw new FeatureDisabledException(FeatureDisabledException.Feature.EXPLORE,
                                             NamespaceConfig.class.getSimpleName() + " of " +
                                               impersonationRequest.getEntityId(),
                                             NamespaceConfig.EXPLORE_AS_PRINCIPAL, String.valueOf(true));
        }

      } catch (IOException e) {
        throw e;
      } catch (Exception e) {
        throw new IOException(e);
      }
    }
    return true;
  }

  /**
   * Resolves the {@link UserGroupInformation} for a given user, performing any keytab localization, if necessary.
   *
   * @return a {@link UserGroupInformation}, based upon the information configured for a particular user
   * @throws IOException if there was any IOException during localization of the keytab
   */
  @Override
  protected UGIWithPrincipal createUGI(ImpersonationRequest impersonationRequest) throws IOException {

    // Get impersonation keytab and principal from runtime arguments if present
    Map<String, String> properties = getRuntimeProperties(impersonationRequest.getEntityId());
    if ((properties != null) && (properties.containsKey(SystemArguments.RUNTIME_KEYTAB_PATH))
          && (properties.containsKey(SystemArguments.RUNTIME_PRINCIPAL_NAME))) {
      String keytab = properties.get(SystemArguments.RUNTIME_KEYTAB_PATH);
      String principal = properties.get(SystemArguments.RUNTIME_PRINCIPAL_NAME);
      LOG.debug("Using runtime config principal: %s, keytab: %s", principal, keytab);
      UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(
              principal, keytab);
      return new UGIWithPrincipal(principal, ugi);
    }

    // no need to get a UGI if the current UGI is the one we're requesting; simply return it
    String configuredPrincipalShortName = new KerberosName(impersonationRequest.getPrincipal()).getShortName();
    if (UserGroupInformation.getCurrentUser().getShortUserName().equals(configuredPrincipalShortName)) {
      return new UGIWithPrincipal(impersonationRequest.getPrincipal(), UserGroupInformation.getCurrentUser());
    }

    URI keytabURI = URI.create(impersonationRequest.getKeytabURI());
    boolean isKeytabLocal = keytabURI.getScheme() == null || "file".equals(keytabURI.getScheme());

    File localKeytabFile = isKeytabLocal ?
      new File(keytabURI.getPath()) : localizeKeytab(locationFactory.create(keytabURI));
    try {
      String expandedPrincipal = SecurityUtil.expandPrincipal(impersonationRequest.getPrincipal());
      LOG.debug("Logging in as: principal={}, keytab={}", expandedPrincipal, localKeytabFile);

      // Note: if the keytab file is not local then then localizeKeytab function call above which tries to localize the
      // keytab from HDFS will throw IOException if the file does not exists or is not readable. Where as if the file
      // is local then the localizeKeytab function is not called so its important that we throw IOException if the local
      // keytab file is not readable to ensure that the client gets the same exception in both the modes.
      if (!Files.isReadable(localKeytabFile.toPath())) {
        throw new IOException(String.format("Keytab file is not a readable file: %s", localKeytabFile));
      }

      UserGroupInformation loggedInUGI;
      try {
        loggedInUGI =
          UserGroupInformation.loginUserFromKeytabAndReturnUGI(expandedPrincipal, localKeytabFile.getAbsolutePath());
      } catch (Exception e) {
        // rethrow the exception with additional information tagged, so the user knows which principal/keytab is
        // not working
        throw new IOException(String.format("Failed to login for principal=%s, keytab=%s. Check that the principal " +
                                              "was not deleted and that the keytab is still valid.",
                                            expandedPrincipal, keytabURI),
                              e);
      }

      return new UGIWithPrincipal(impersonationRequest.getPrincipal(), loggedInUGI);
    } finally {
      if (!isKeytabLocal && !localKeytabFile.delete()) {
        LOG.warn("Failed to delete file: {}", localKeytabFile);
      }
    }
  }

  /**
   * Returns the path to a keytab file, after copying it to the local file system, if necessary
   *
   * @param keytabLocation the keytabLocation of the keytab file
   * @return local keytab file
   */
  private File localizeKeytab(Location keytabLocation) throws IOException {
    // ensure temp dir exists
    if (!DirUtils.mkdirs(tempDir)) {
      throw new IOException(String.format(
        "Could not create temporary directory at %s, while localizing keytab", tempDir));
    }

    // create a local file with restricted permissions
    // only allow the owner to read/write, since it contains credentials
    Path localKeytabFile = Files.createTempFile(tempDir.toPath(), null, "keytab.localized", FileUtils.OWNER_ONLY_RW);
    // copy to this local file
    LOG.debug("Copying keytab file from {} to {}", keytabLocation, localKeytabFile);
    try (InputStream is = keytabLocation.getInputStream()) {
      Files.copy(is, localKeytabFile, StandardCopyOption.REPLACE_EXISTING);
    }

    return localKeytabFile.toFile();
  }

  /**
   * Returns the runtime user supplied arguments to a program if entity is of type ProgramRun
   *
   * @param entityId the entity to lookup for runtime arguments.
   * @return properties map
   */
  private Map<String, String> getRuntimeProperties(NamespacedEntityId entityId) {
    if (!(entityId instanceof ProgramRunId)) {
      LOG.debug("Entity Id not of type ProgramRunId, skipping checking of runtime args");
      return Collections.emptyMap();
    }

    ProgramRunId runId = ((ProgramRunId) entityId);
    RunRecordDetail runRecord = null;

    long sleepDelayMs = TimeUnit.MILLISECONDS.toMillis(100);
    long timeoutMs = TimeUnit.SECONDS.toMillis(10);

    try {
      runRecord = Retries.callWithRetries(() -> {
        RunRecordDetail rec = store.getRun(runId);
        if (rec != null) {
          return rec;
        }
        throw new Exception("Retrying again to fetch run record");
      }, RetryStrategies.timeLimit(timeoutMs, TimeUnit.MILLISECONDS,
                  RetryStrategies.fixDelay(sleepDelayMs, TimeUnit.MILLISECONDS)),
                  Exception.class::isInstance);
    } catch (Exception e) {
      LOG.warn("Failed to fetch run record for {} due to {}", runId, e.getMessage(), e);
      return Collections.emptyMap();
    }
    return runRecord.getUserArgs();
  }
}

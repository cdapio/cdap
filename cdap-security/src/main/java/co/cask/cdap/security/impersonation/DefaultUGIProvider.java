/*
 * Copyright © 2016-2017 Cask Data, Inc.
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

package co.cask.cdap.security.impersonation;

import co.cask.cdap.common.FeatureDisabledException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.kerberos.ImpersonatedOpType;
import co.cask.cdap.common.kerberos.ImpersonationInfo;
import co.cask.cdap.common.kerberos.ImpersonationOpInfo;
import co.cask.cdap.common.kerberos.OwnerAdmin;
import co.cask.cdap.common.kerberos.SecurityUtil;
import co.cask.cdap.common.kerberos.UGIWithPrincipal;
import co.cask.cdap.common.namespace.NamespaceQueryAdmin;
import co.cask.cdap.common.utils.DirUtils;
import co.cask.cdap.common.utils.FileUtils;
import co.cask.cdap.proto.NamespaceConfig;
import co.cask.cdap.proto.element.EntityType;
import co.cask.cdap.proto.id.NamespacedEntityId;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
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

/**
 * Provides a UGI by logging in with a keytab file for that user.
 */
public class DefaultUGIProvider extends AbstractCachedUGIProvider {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultUGIProvider.class);

  private final LocationFactory locationFactory;
  private final File tempDir;
  private final OwnerAdmin ownerAdmin;
  private final NamespaceQueryAdmin namespaceQueryAdmin;


  @Inject
  DefaultUGIProvider(CConfiguration cConf, LocationFactory locationFactory, OwnerAdmin ownerAdmin,
                     NamespaceQueryAdmin namespaceQueryAdmin) {
    super(cConf);
    this.locationFactory = locationFactory;
    this.tempDir = new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR),
                            cConf.get(Constants.AppFabric.TEMP_DIR)).getAbsoluteFile();
    this.ownerAdmin = ownerAdmin;
    this.namespaceQueryAdmin = namespaceQueryAdmin;
  }

  /**
   * Resolves the {@link UserGroupInformation} for a given user, performing any keytab localization, if necessary.
   *
   * @return a {@link UserGroupInformation}, based upon the information configured for a particular user
   * @throws IOException if there was any IOException during localization of the keytab
   */
  @Override
  protected UGIWithPrincipal createUGI(ImpersonationOpInfo impersonationOpInfo) throws IOException {
    if (impersonationOpInfo.getEntityId().getEntityType().equals(EntityType.NAMESPACE)) {
      try {
        NamespaceConfig nsConfig = namespaceQueryAdmin.get(impersonationOpInfo.getEntityId().getNamespaceId())
          .getConfig();
        if (impersonationOpInfo.getImpersonatedOpType().equals(ImpersonatedOpType.EXPLORE) &&
          !nsConfig.isExploreAsPrincipal()) {
          throw new FeatureDisabledException(FeatureDisabledException.Feature.EXPLORE,
                                             NamespaceConfig.class.getSimpleName() + " of " +
                                               impersonationOpInfo.getEntityId(),
                                             NamespaceConfig.EXPLORE_AS_PRINCIPAL, String.valueOf(true));
        }

      } catch (IOException e) {
        throw e;
      } catch (Exception e) {
        throw new IOException(e);
      }
    }

    ImpersonationInfo impersonationInfo = getImpersonationInfo(impersonationOpInfo.getEntityId());
    LOG.debug("Configured impersonation info: {}", impersonationInfo);

    // CDAP-8355 If the operation being impersonated is an explore query then check if the namespace configuration
    // specifies that it can be impersonated with the namespace owner.

    // no need to get a UGI if the current UGI is the one we're requesting; simply return it
    String configuredPrincipalShortName = new KerberosName(impersonationInfo.getPrincipal()).getShortName();
    if (UserGroupInformation.getCurrentUser().getShortUserName().equals(configuredPrincipalShortName)) {
      return new UGIWithPrincipal(impersonationInfo.getPrincipal(), UserGroupInformation.getCurrentUser());
    }

    URI keytabURI = URI.create(impersonationInfo.getKeytabURI());
    boolean isKeytabLocal = keytabURI.getScheme() == null || "file".equals(keytabURI.getScheme());

    File localKeytabFile = isKeytabLocal ?
      new File(keytabURI.getPath()) : localizeKeytab(locationFactory.create(keytabURI));
    try {
      String expandedPrincipal = SecurityUtil.expandPrincipal(impersonationInfo.getPrincipal());
      LOG.debug("Logging in as: principal={}, keytab={}", expandedPrincipal, localKeytabFile);

      Preconditions.checkArgument(Files.isReadable(localKeytabFile.toPath()),
                                  "Keytab file is not a readable file: %s", localKeytabFile);

      UserGroupInformation loggedInUGI =
        UserGroupInformation.loginUserFromKeytabAndReturnUGI(expandedPrincipal, localKeytabFile.getAbsolutePath());
      return new UGIWithPrincipal(impersonationInfo.getPrincipal(), loggedInUGI);
    } finally {
      if (!isKeytabLocal && !localKeytabFile.delete()) {
        LOG.warn("Failed to delete file: {}", localKeytabFile);
      }
    }
  }

  private ImpersonationInfo getImpersonationInfo(NamespacedEntityId entityId) throws IOException {
      ImpersonationInfo info = SecurityUtil.createImpersonationInfo(ownerAdmin, cConf, entityId);
      LOG.debug("Impersonating principal {} for entity {}, keytab path is {}",
                info.getPrincipal(), entityId, info.getKeytabURI());
      return info;
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
}

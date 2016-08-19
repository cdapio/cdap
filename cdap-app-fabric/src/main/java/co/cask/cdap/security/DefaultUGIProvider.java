/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.security;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.kerberos.SecurityUtil;
import co.cask.cdap.common.security.ImpersonationInfo;
import co.cask.cdap.common.security.UGIProvider;
import co.cask.cdap.common.utils.DirUtils;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
import com.google.inject.Inject;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Set;

/**
 * Provides a UGI by logging in with a keytab file for that user.
 */
public class DefaultUGIProvider implements UGIProvider {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultUGIProvider.class);

  private final CConfiguration cConf;
  private final LocationFactory locationFactory;

  @Inject
  DefaultUGIProvider(CConfiguration cConf, LocationFactory locationFactory) {
    this.cConf = cConf;
    this.locationFactory = locationFactory;
  }

  /**
   * Resolves the {@link UserGroupInformation} for a given user, performing any keytab localization, if necessary.
   *
   * @return a {@link UserGroupInformation}, based upon the information configured for a particular user
   * @throws IOException if there was any IOException during localization of the keytab
   */
  @Override
  public UserGroupInformation getConfiguredUGI(ImpersonationInfo impersonationInfo) throws IOException {
    LOG.debug("Configured impersonation info: {}", impersonationInfo);

    // by default, don't delete any file
    File cleanupFile = null;

    try {
      URI keytabURI = URI.create(impersonationInfo.getKeytabURI());

      File localKeytabFile;
      if (keytabURI.getScheme() == null || "file".equals(keytabURI.getScheme())) {
        localKeytabFile = new File(keytabURI.getPath());
      } else {
        localKeytabFile = localizeKeytab(locationFactory.create(keytabURI));
        // since we localized the keytab file, remove it after we're done with it
        cleanupFile = localKeytabFile;
      }

      String expandedPrincipal = SecurityUtil.expandPrincipal(impersonationInfo.getPrincipal());
      LOG.debug("Logging in as: principal={}, keytab={}", expandedPrincipal, localKeytabFile);

      Preconditions.checkArgument(java.nio.file.Files.isReadable(localKeytabFile.toPath()),
                                  "Keytab file is not a readable file: %s", localKeytabFile);

      return UserGroupInformation.loginUserFromKeytabAndReturnUGI(expandedPrincipal, localKeytabFile.getAbsolutePath());
    } finally {
      if (cleanupFile != null) {
        if (!cleanupFile.delete()) {
          LOG.warn("Failed to delete file: {}", cleanupFile);
        }
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
    // if scheme is not specified, assume its local file
    URI keytabURI = keytabLocation.toURI();
    if (keytabURI.getScheme() == null || "file".equals(keytabURI.getScheme())) {
      return new File(keytabURI.getPath());
    }

    // ensure temp dir exists
    File tempDir = new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR),
                            cConf.get(Constants.AppFabric.TEMP_DIR)).getAbsoluteFile();
    if (!DirUtils.mkdirs(tempDir)) {
      throw new IOException(String.format(
        "Could not create temporary directory at %s, while localizing keytab", tempDir));
    }

    // create a local file with restricted permissions
    // only allow the owner to read/write, since it contains credentials
    FileAttribute<Set<PosixFilePermission>> ownerOnlyAttrs =
      PosixFilePermissions.asFileAttribute(ImmutableSet.of(PosixFilePermission.OWNER_WRITE,
                                                           PosixFilePermission.OWNER_READ));
    Path localKeytabFile = java.nio.file.Files.createTempFile(tempDir.toPath(), null, "keytab.localized",
                                                              ownerOnlyAttrs);

    // copy to this local file
    LOG.debug("Copying keytab file from {} to {}", keytabLocation, localKeytabFile);

    Files.copy(Locations.newInputSupplier(keytabLocation), localKeytabFile.toFile());
    return localKeytabFile.toFile();
  }
}

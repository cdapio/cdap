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

package co.cask.cdap.data2.datafabric.dataset.type;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.lang.DirectoryClassLoader;
import co.cask.cdap.common.lang.jar.BundleJarUtil;
import co.cask.cdap.common.utils.DirUtils;
import co.cask.cdap.proto.DatasetModuleMeta;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import com.google.inject.Inject;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.Security;
import java.util.Set;

/**
 * Creates a {@link ClassLoader} for a {@link DatasetModuleMeta} by unpacking the moduleMeta jar,
 * since the moduleMeta jar may not be present in the local filesystem.
 */
public class DistributedDatasetTypeClassLoaderFactory implements DatasetTypeClassLoaderFactory {

  private static final Logger LOG = LoggerFactory.getLogger(DistributedDatasetTypeClassLoaderFactory.class);

  private static final ThreadLocal<MessageDigest> MESSAGE_DIGEST = new ThreadLocal<MessageDigest>() {
    @Override
    protected MessageDigest initialValue() {
      try {
        // Prefer MD5
        return MessageDigest.getInstance("MD5");
      } catch (NoSuchAlgorithmException e) {
        // Doesn't expect it to fail. If really do, pick the first provider available.
        Set<String> algorithms = ImmutableSortedSet.copyOf(Security.getAlgorithms("MessageDigest"));
        if (algorithms.isEmpty()) {
          throw new IllegalStateException("No MessageDigest algorithm available.");
        }

        try {
          String algorithm = algorithms.iterator().next();
          LOG.warn("Failed to get MD5 MessageDigest, use {} instead", algorithm);
          return MessageDigest.getInstance(algorithm);
        } catch (NoSuchAlgorithmException ex) {
          // This shouldn't happen
          throw Throwables.propagate(ex);
        }
      }
    }

    @Override
    public MessageDigest get() {
      // Reset the MD before returning
      MessageDigest md = super.get();
      md.reset();
      return md;
    }
  };
  private final LocationFactory locationFactory;

  @Inject
  public DistributedDatasetTypeClassLoaderFactory(LocationFactory locationFactory) {
    this.locationFactory = locationFactory;
  }

  @Override
  public ClassLoader create(DatasetModuleMeta moduleMeta, ClassLoader parentClassLoader) throws IOException {
    if (moduleMeta.getJarLocation() == null) {
      return parentClassLoader;
    }

    // In distributed mode, Dataset ClassLoader is a URLClassLoader with all the jars inside the dataset.jar.

    // Copy the jar from remote location to a local temp file, and compute checksum while copying
    Location jarLocation = locationFactory.create(moduleMeta.getJarLocation());
    File tmpJar = File.createTempFile(jarLocation.getName(), null);
    try {
      MessageDigest messageDigest = MESSAGE_DIGEST.get();
      DigestOutputStream digestOutput = new DigestOutputStream(new FileOutputStream(tmpJar), messageDigest);
      try {
        ByteStreams.copy(Locations.newInputSupplier(jarLocation), digestOutput);
      } finally {
        digestOutput.close();
      }

      // The folder name to expand to is formed by the module name and the checksum.
      String dirName = String.format("%s.%s", moduleMeta.getName(), Bytes.toHexString(messageDigest.digest()));
      File expandDir = new File(tmpJar.getParent(), dirName);

      if (!expandDir.isDirectory()) {
        // If not expanded before, expand the jar to a tmp folder and rename into the intended one.
        // It's needed if there are multiple threads try to get the same dataset.
        File tempDir = Files.createTempDir();
        try {
          BundleJarUtil.unpackProgramJar(Files.newInputStreamSupplier(tmpJar), tempDir);
          if (!tempDir.renameTo(expandDir) && !expandDir.isDirectory()) {
            throw new IOException("Failed to rename expanded jar directory from " + tempDir + " to " + expandDir);
          }
        } finally {
          try {
            if (tempDir.exists()) {
              DirUtils.deleteDirectoryContents(tempDir);
            }
          } catch (IOException e) {
            // Just log, no propagate
            LOG.warn("Failed to delete temp directory {}", tempDir, e);
          }
        }
      }
      return new DirectoryClassLoader(expandDir, parentClassLoader, "lib");

    } finally {
      tmpJar.delete();
    }
  }
}

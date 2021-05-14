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

package io.cdap.cdap.k8s.runtime;

import io.cdap.cdap.master.spi.environment.MasterEnvironment;
import io.cdap.cdap.master.spi.environment.MasterEnvironmentContext;
import io.cdap.cdap.master.spi.environment.MasterEnvironmentRunnable;
import org.apache.twill.api.LocalFile;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.internal.TwillRuntimeSpecification;
import org.apache.twill.internal.json.TwillRuntimeSpecificationAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * A {@link MasterEnvironmentRunnable} for localizing files to the current directory.
 */
public class FileLocalizer implements MasterEnvironmentRunnable {

  private static final Logger LOG = LoggerFactory.getLogger(FileLocalizer.class);

  private final MasterEnvironmentContext context;
  private final MasterEnvironment masterEnv;
  private volatile boolean stopped;

  public FileLocalizer(MasterEnvironmentContext context, MasterEnvironment masterEnv) {
    this.context = context;
    this.masterEnv = masterEnv;
  }

  @Override
  public void run(String[] args) throws Exception {
    if (args.length < 2) {
      // This should never happen
      throw new IllegalArgumentException("Expected to have two arguments: runtime config uri and the runnable name.");
    }

    // Localize the runtime config jar
    URI uri = URI.create(args[0]);
    Location runtimeConfigLocation = new LocalLocationFactory().create(new File(uri.getPath()).toURI());

    FileFetcher fileFetcher = new FileFetcher(masterEnv.getDiscoveryServiceClientSupplier().get());
    fileFetcher.downloadWithRetry(uri, runtimeConfigLocation);

    Path runtimeConfigDir = expand(runtimeConfigLocation,
                                   Paths.get(org.apache.twill.internal.Constants.Files.RUNTIME_CONFIG_JAR));

    try (Reader reader = Files.newBufferedReader(
      runtimeConfigDir.resolve(org.apache.twill.internal.Constants.Files.TWILL_SPEC),
      StandardCharsets.UTF_8)) {
      TwillRuntimeSpecification twillRuntimeSpec = TwillRuntimeSpecificationAdapter.create().fromJson(reader);

      Path targetDir = Paths.get(System.getProperty("user.dir"));
      Files.createDirectories(targetDir);

      for (LocalFile localFile : twillRuntimeSpec.getTwillSpecification().getRunnables().get(args[1]).getLocalFiles()) {
        if (stopped) {
          LOG.info("Stop localization on request");
          break;
        }

        Location localizedFile = new LocalLocationFactory().create(new File(localFile.getURI().getPath()).toURI());
        fileFetcher.download(localFile.getURI(), localizedFile);

        Path targetPath = targetDir.resolve(localFile.getName());

        if (localFile.isArchive()) {
          expand(localizedFile, targetPath);
        } else {
          copy(localizedFile, targetPath);
        }
      }
    }
  }

  @Override
  public void stop() {
    stopped = true;
  }

  private void copy(Location location, Path target) throws IOException {
    LOG.debug("Localize {} to {}", location, target);

    try (InputStream is = location.getInputStream()) {
      Files.copy(is, target, StandardCopyOption.REPLACE_EXISTING);
    }
  }

  private Path expand(Location location, Path targetDir) throws IOException {
    LOG.debug("Localize and expand {} to {}", location, targetDir);

    try (ZipInputStream is = new ZipInputStream(location.getInputStream())) {
      Path targetPath = Files.createDirectories(targetDir);
      ZipEntry entry;
      while ((entry = is.getNextEntry()) != null && !stopped) {
        Path outputPath = targetPath.resolve(entry.getName());

        if (entry.isDirectory()) {
          Files.createDirectories(outputPath);
        } else {
          Files.createDirectories(outputPath.getParent());
          Files.copy(is, outputPath, StandardCopyOption.REPLACE_EXISTING);
        }
      }
    }
    return targetDir;
  }
}

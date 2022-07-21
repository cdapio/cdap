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
import io.cdap.cdap.master.spi.environment.MasterEnvironmentRunnable;
import io.cdap.cdap.master.spi.environment.MasterEnvironmentRunnableContext;
import org.apache.twill.api.LocalFile;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.internal.Constants;
import org.apache.twill.internal.TwillRuntimeSpecification;
import org.apache.twill.internal.json.TwillRuntimeSpecificationAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.net.HttpURLConnection;
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

  private final MasterEnvironmentRunnableContext context;
  private volatile boolean stopped;

  public FileLocalizer(MasterEnvironmentRunnableContext context,
                       @SuppressWarnings("unused") MasterEnvironment masterEnv) {
    this.context = context;
  }

  @Override
  public void run(String[] args) throws Exception {
    if (args.length < 2) {
      // This should never happen
      throw new IllegalArgumentException("Expected to have two arguments: runtime config uri and the runnable name.");
    }

    LocalLocationFactory localLocationFactory = new LocalLocationFactory();

    // Localize the runtime config jar
    URI uri = URI.create(args[0]);

    Path runtimeConfigDir;
    if (localLocationFactory.getHomeLocation().toURI().getScheme().equals(uri.getScheme())) {
      try (FileInputStream is = new FileInputStream(new File(uri))) {
        runtimeConfigDir = expand(uri, is, Paths.get(Constants.Files.RUNTIME_CONFIG_JAR));
      }
    } else {
      try (InputStream is = getHttpURLConnectionInputStream(fileDownloadURLPath(uri))) {
        runtimeConfigDir = expand(uri, is, Paths.get(Constants.Files.RUNTIME_CONFIG_JAR));
      }
    }

    try (Reader reader = Files.newBufferedReader(runtimeConfigDir.resolve(Constants.Files.TWILL_SPEC),
                                                 StandardCharsets.UTF_8)) {
      TwillRuntimeSpecification twillRuntimeSpec = TwillRuntimeSpecificationAdapter.create().fromJson(reader);

      Path targetDir = Paths.get(System.getProperty("user.dir"));
      Files.createDirectories(targetDir);

      for (LocalFile localFile : twillRuntimeSpec.getTwillSpecification().getRunnables().get(args[1]).getLocalFiles()) {
        if (stopped) {
          LOG.info("Stop localization on request");
          break;
        }

        Path targetPath = targetDir.resolve(localFile.getName());

        try (InputStream is = getHttpURLConnectionInputStream(fileDownloadURLPath(localFile.getURI()))) {
          if (localFile.isArchive()) {
            expand(localFile.getURI(), is, targetPath);
          } else {
            copy(localFile.getURI(), is, targetPath);
          }
        }
      }
    }
  }

  @Override
  public void stop() {
    stopped = true;
  }

  /**
   * Return an {@link InputStream} for the given {@link HttpURLConnection} URL path that
   * auto disconnects upon closing the {@link InputStream}
   */
  private InputStream getHttpURLConnectionInputStream(String urlPath) throws IOException {
    HttpURLConnection conn = context.openHttpURLConnection(urlPath);
    return new FilterInputStream(conn.getInputStream()) {
      @Override
      public void close() throws IOException {
        try {
          super.close();
        } finally {
          conn.disconnect();
        }
      }
    };
  }

  private String fileDownloadURLPath(URI uri) {
    return String.format("%s/%s", "v3Internal/location", uri.getPath());
  }

  private void copy(URI uri, InputStream inputStream, Path target) throws IOException {
    LOG.debug("Localize {} to {}", uri, target);

    Files.copy(inputStream, target, StandardCopyOption.REPLACE_EXISTING);
  }

  private Path expand(URI uri, InputStream inputStream, Path targetDir) throws IOException {
    LOG.debug("Localize and expand {} to {}", uri.toString(), targetDir);

    try (ZipInputStream is = new ZipInputStream(inputStream)) {
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

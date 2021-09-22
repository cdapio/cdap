/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.app.runtime.spark.distributed.k8s;

import io.cdap.cdap.app.runtime.spark.distributed.SparkContainerLauncher;
import io.cdap.cdap.common.conf.Constants;
import org.apache.hadoop.util.RunJar;

import java.io.File;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;

/**
 * Spark container launcher for launching spark executors.
 */
public class SparkContainerExecutorLauncher {
  private static final String DRIVER_URL_FLAG = "--driver-url";
  private static final String DELEGATE_CLASS_FLAG = "--delegate-class";
  private static final String ARTIFACT_FETCHER_ENDPOINT = Constants.Gateway.INTERNAL_API_VERSION_3 + "/artifacts/fetch";

  //TODO: Needs to be read from CConf
  private static final int ARTIFACT_FETCHER_PORT = 11013;
  private static final String WORKING_DIRECTORY = "/opt/spark/work-dir/";

  public static void main(String[] args) throws Exception {
    String delegateClass = "org.apache.spark.deploy.SparkSubmit";
    List<String> delegateArgs = new ArrayList<>();
    String driverHost = null;
    for (int i = 0; i < args.length; i++) {
      if (args[i].equals(DRIVER_URL_FLAG)) {
        driverHost = new URI(args[i + 1]).getHost();
      } else if (args[i].equals(DELEGATE_CLASS_FLAG)) {
        delegateClass = args[i + 1];
        i++;
        continue;
      }
      delegateArgs.add(args[i]);
    }

    if (driverHost == null || driverHost.length() == 0) {
      throw new IllegalArgumentException("Spark driver url is not set.");
    }

    URL fetchArtifactURL =
      new URL(String.format("http://%s:%s%s", driverHost, ARTIFACT_FETCHER_PORT, ARTIFACT_FETCHER_ENDPOINT));
    HttpURLConnection connection = (HttpURLConnection) fetchArtifactURL.openConnection();
    connection.connect();

    Path bundleJarFile = new File(WORKING_DIRECTORY).toPath().resolve("bundle.jar");
    try (InputStream in = connection.getInputStream()) {
      Files.copy(in, bundleJarFile, StandardCopyOption.REPLACE_EXISTING);
    } finally {
      connection.disconnect();
    }

    RunJar.unJar(bundleJarFile.toFile(), new File(WORKING_DIRECTORY), RunJar.MATCH_ANY);

    SparkContainerLauncher.launch(delegateClass, delegateArgs.toArray(new String[delegateArgs.size()]));
  }
}

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

package co.cask.cdap.spark.app;

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.spark.AbstractSpark;
import co.cask.cdap.api.spark.SparkClientContext;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.lang.jar.BundleJarUtil;
import com.google.common.base.Preconditions;
import com.google.common.io.Files;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * App to test local files in Spark programs.
 */
public class SparkAppUsingLocalFiles extends AbstractApplication {
  public static final String OUTPUT_DATASET_NAME = "output";
  public static final String LOCAL_FILE_RUNTIME_ARG = "local.file";
  static final String LOCAL_FILE_ALIAS = "local.properties";
  static final String LOCAL_ARCHIVE_ALIAS = "archive.jar";

  @Override
  public void configure() {
    createDataset(OUTPUT_DATASET_NAME, KeyValueTable.class);
    addSpark(new JavaSparkUsingLocalFiles());
    addSpark(new ScalaSparkUsingLocalFiles());
  }

  public static class JavaSparkUsingLocalFiles extends AbstractSpark {
    @Override
    protected void configure() {
      setMainClass(SparkUsingLocalFilesMain.class);
    }

    @Override
    public void beforeSubmit(SparkClientContext context) throws Exception {
      Map<String, String> args = context.getRuntimeArguments();
      String localFilePath = args.get(LOCAL_FILE_RUNTIME_ARG);
      Preconditions.checkArgument(localFilePath != null, "Runtime argument %s must be set.", LOCAL_FILE_RUNTIME_ARG);
      context.localize(LOCAL_FILE_ALIAS, URI.create(localFilePath));
      context.localize(LOCAL_ARCHIVE_ALIAS, createTemporaryArchiveFile(), true);
    }
  }

  public static class ScalaSparkUsingLocalFiles extends AbstractSpark {
    @Override
    protected void configure() {
      setMainClass(ScalaSparkUsingLocalFilesMain.class);
    }

    @Override
    public void beforeSubmit(SparkClientContext context) throws Exception {
      Map<String, String> args = context.getRuntimeArguments();
      String localFilePath = args.get(LOCAL_FILE_RUNTIME_ARG);
      Preconditions.checkArgument(localFilePath != null, "Runtime argument %s must be set.", LOCAL_FILE_RUNTIME_ARG);
      context.localize(LOCAL_FILE_ALIAS, URI.create(localFilePath));
      context.localize(LOCAL_ARCHIVE_ALIAS, createTemporaryArchiveFile(), true);
    }
  }

  private static URI createTemporaryArchiveFile() throws IOException {
    File tmpDir1 = Files.createTempDir();
    List<File> files = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      File tmpFile = File.createTempFile("abcd" + i, "txt", tmpDir1);
      files.add(tmpFile);
    }

    File tmpDir2 = Files.createTempDir();
    File destArchive = new File(tmpDir2, "myBundle.jar");
    BundleJarUtil.createJar(tmpDir1, destArchive);
    for (File file : files) {
      BundleJarUtil.getEntry(Locations.toLocation(destArchive), file.getName()).getInput().close();
    }
    return destArchive.toURI();
  }
}

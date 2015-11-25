/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.spark;

import co.cask.cdap.api.TaskLocalizationContext;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.spark.AbstractSpark;
import co.cask.cdap.api.spark.JavaSparkProgram;
import co.cask.cdap.api.spark.SparkContext;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.lang.jar.BundleJarUtil;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.io.Files;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * App to test local files in Spark programs.
 */
public class SparkAppUsingLocalFiles extends AbstractApplication {
  static final String OUTPUT_DATASET_NAME = "output";
  static final String LOCAL_FILE_RUNTIME_ARG = "local.file";
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
      setMainClass(JavaSparkProgramUsingLocalFiles.class);
    }

    @Override
    public void beforeSubmit(SparkContext context) throws Exception {
      Map<String, String> args = context.getRuntimeArguments();
      String localFilePath = args.get(LOCAL_FILE_RUNTIME_ARG);
      Preconditions.checkArgument(localFilePath != null, "Runtime argument %s must be set.", LOCAL_FILE_RUNTIME_ARG);
      context.localize(LOCAL_FILE_ALIAS, URI.create(localFilePath));
      context.localize(LOCAL_ARCHIVE_ALIAS, createTemporaryArchiveFile(), true);
    }
  }

  public static class JavaSparkProgramUsingLocalFiles implements JavaSparkProgram {

    @Override
    public void run(SparkContext context) {
      Map<String, String> args = context.getRuntimeArguments();
      Preconditions.checkArgument(args.containsKey(LOCAL_FILE_RUNTIME_ARG),
                                  "Runtime argument %s must be set.", LOCAL_FILE_RUNTIME_ARG);
      final String localFilePath = URI.create(args.get(LOCAL_FILE_RUNTIME_ARG)).getPath();
      JavaSparkContext sc = context.getOriginalSparkContext();
      JavaRDD<String> fileContents = sc.textFile(localFilePath, 1);
      final TaskLocalizationContext taskLocalizationContext = context.getTaskLocalizationContext();
      JavaPairRDD<byte[], byte[]> rows = fileContents.mapToPair(new PairFunction<String, byte[], byte[]>() {
        @Override
        public Tuple2<byte[], byte[]> call(String line) throws Exception {
          Map<String, File> localFiles = taskLocalizationContext.getAllLocalFiles();
          Preconditions.checkState(localFiles.containsKey(LOCAL_FILE_ALIAS),
                                   "File %s should have been localized with the name %s.",
                                   localFilePath, LOCAL_FILE_ALIAS);
          Preconditions.checkState(localFiles.containsKey(LOCAL_ARCHIVE_ALIAS),
                                   "A temporary archive should have been localized with the name %s.",
                                   LOCAL_ARCHIVE_ALIAS);
          boolean localFileFound = false;
          for (File localFile : localFiles.values()) {
            if (LOCAL_FILE_ALIAS.equals(localFile.getName())) {
              localFileFound = true;
              break;
            }
          }
          Preconditions.checkState(localFileFound, "Local file must be found.");
          File localFile = taskLocalizationContext.getLocalFile(LOCAL_FILE_ALIAS);
          Preconditions.checkState(localFile.exists(), "Local file %s must exist.", localFile);
          File localArchive = taskLocalizationContext.getLocalFile(LOCAL_ARCHIVE_ALIAS);
          Preconditions.checkState(localArchive.exists(), "Local archive %s must exist.", LOCAL_ARCHIVE_ALIAS);
          Preconditions.checkState(localArchive.isDirectory(), "Local archive %s should have been extracted to a " +
            "directory.", LOCAL_ARCHIVE_ALIAS);
          Iterator<String> splitter = Splitter.on("=").omitEmptyStrings().trimResults().split(line).iterator();
          Preconditions.checkArgument(splitter.hasNext());
          String key = splitter.next();
          Preconditions.checkArgument(splitter.hasNext());
          String value = splitter.next();
          return new Tuple2<>(Bytes.toBytes(key), Bytes.toBytes(value));
        }
      });

      context.writeToDataset(rows, OUTPUT_DATASET_NAME, byte[].class, byte[].class);
    }
  }

  public static class ScalaSparkUsingLocalFiles extends AbstractSpark {
    @Override
    protected void configure() {
      setMainClass(ScalaSparkProgramUsingLocalFiles.class);
    }

    @Override
    public void beforeSubmit(SparkContext context) throws Exception {
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

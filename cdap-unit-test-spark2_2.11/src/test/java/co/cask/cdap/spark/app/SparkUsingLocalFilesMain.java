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

package co.cask.cdap.spark.app;

import co.cask.cdap.api.TaskLocalizationContext;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.spark.JavaSparkExecutionContext;
import co.cask.cdap.api.spark.JavaSparkMain;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.File;
import java.net.URI;
import java.util.Iterator;
import java.util.Map;

import static co.cask.cdap.spark.app.SparkAppUsingLocalFiles.OUTPUT_DATASET_NAME;

/**
 * Spark program that uses local files in Java.
 */
public class SparkUsingLocalFilesMain implements JavaSparkMain {

  @Override
  public void run(JavaSparkExecutionContext sec) throws Exception {
    JavaSparkContext jsc = new JavaSparkContext();

    Map<String, String> args = sec.getRuntimeArguments();
    Preconditions.checkArgument(args.containsKey(SparkAppUsingLocalFiles.LOCAL_FILE_RUNTIME_ARG),
                                "Runtime argument %s must be set.", SparkAppUsingLocalFiles.LOCAL_FILE_RUNTIME_ARG);
    final String localFilePath = URI.create(args.get(SparkAppUsingLocalFiles.LOCAL_FILE_RUNTIME_ARG)).getPath();
    JavaRDD<String> fileContents = jsc.textFile(localFilePath, 1);
    final TaskLocalizationContext taskLocalizationContext = sec.getLocalizationContext();
    JavaPairRDD<byte[], byte[]> rows = fileContents.mapToPair(new PairFunction<String, byte[], byte[]>() {
      @Override
      public Tuple2<byte[], byte[]> call(String line) throws Exception {
        Map<String, File> localFiles = taskLocalizationContext.getAllLocalFiles();
        Preconditions.checkState(localFiles.containsKey(SparkAppUsingLocalFiles.LOCAL_FILE_ALIAS),
                                 "File %s should have been localized with the name %s.",
                                 localFilePath, SparkAppUsingLocalFiles.LOCAL_FILE_ALIAS);
        Preconditions.checkState(localFiles.containsKey(SparkAppUsingLocalFiles.LOCAL_ARCHIVE_ALIAS),
                                 "A temporary archive should have been localized with the name %s.",
                                 SparkAppUsingLocalFiles.LOCAL_ARCHIVE_ALIAS);
        boolean localFileFound = false;
        for (File localFile : localFiles.values()) {
          if (SparkAppUsingLocalFiles.LOCAL_FILE_ALIAS.equals(localFile.getName())) {
            localFileFound = true;
            break;
          }
        }
        Preconditions.checkState(localFileFound, "Local file must be found.");
        File localFile = taskLocalizationContext.getLocalFile(SparkAppUsingLocalFiles.LOCAL_FILE_ALIAS);
        Preconditions.checkState(localFile.exists(), "Local file %s must exist.", localFile);
        File localArchive = taskLocalizationContext.getLocalFile(SparkAppUsingLocalFiles.LOCAL_ARCHIVE_ALIAS);
        Preconditions.checkState(localArchive.exists(), "Local archive %s must exist.",
                                 SparkAppUsingLocalFiles.LOCAL_ARCHIVE_ALIAS);
        Preconditions.checkState(localArchive.isDirectory(),
                                 "Local archive %s should have been extracted to a directory.",
                                 SparkAppUsingLocalFiles.LOCAL_ARCHIVE_ALIAS);
        Iterator<String> splitter = Splitter.on("=").omitEmptyStrings().trimResults().split(line).iterator();
        Preconditions.checkArgument(splitter.hasNext());
        String key = splitter.next();
        Preconditions.checkArgument(splitter.hasNext());
        String value = splitter.next();
        return new Tuple2<>(Bytes.toBytes(key), Bytes.toBytes(value));
      }
    });

    sec.saveAsDataset(rows, OUTPUT_DATASET_NAME);
  }
}

/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.batch;

import co.cask.cdap.api.ProgramLifecycle;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.data.batch.Input;
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.cdap.api.mapreduce.MapReduceTaskContext;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.lang.jar.BundleJarUtil;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

/**
 * App to test local files in programs.
 */
public class AppWithLocalFiles extends AbstractApplication {
  public static final String MR_INPUT_DATASET = "input";
  public static final String MR_OUTPUT_DATASET = "output";
  public static final String STOPWORDS_FILE_ARG = "stopwords.file";
  public static final String STOPWORDS_FILE_ALIAS = "stopwords.txt";
  private static final String LOCAL_ARCHIVE_ALIAS = "archive.jar";

  @Override
  public void configure() {
    createDataset(MR_INPUT_DATASET, KeyValueTable.class);
    createDataset(MR_OUTPUT_DATASET, KeyValueTable.class);
    addStream("LocalFileStream");
    addMapReduce(new MapReduceWithLocalFiles());
  }

  public static class MapReduceWithLocalFiles extends AbstractMapReduce {

    @Override
    public void beforeSubmit(MapReduceContext context) throws Exception {
      Map<String, String> args = context.getRuntimeArguments();
      if (args.containsKey(STOPWORDS_FILE_ARG)) {
        context.localize(STOPWORDS_FILE_ALIAS, URI.create(args.get(STOPWORDS_FILE_ARG)));
      }
      context.localize(LOCAL_ARCHIVE_ALIAS, createTemporaryArchiveFile(), true);
      context.addInput(Input.ofDataset(args.get(MR_INPUT_DATASET)));
      context.addOutput(Output.ofDataset(args.get(MR_OUTPUT_DATASET)));
      Job job = context.getHadoopJob();
      job.setMapperClass(TokenizerMapper.class);
      job.setReducerClass(IntSumReducer.class);
    }

    private URI createTemporaryArchiveFile() throws IOException {
      File tmpDir1 = com.google.common.io.Files.createTempDir();
      List<File> files = new ArrayList<>();
      for (int i = 0; i < 3; i++) {
        File tmpFile = File.createTempFile("abcd" + i, "txt", tmpDir1);
        files.add(tmpFile);
      }

      File tmpDir2 = com.google.common.io.Files.createTempDir();
      File destArchive = new File(tmpDir2, "myBundle.jar");
      BundleJarUtil.createJar(tmpDir1, destArchive);
      for (File file : files) {
        BundleJarUtil.getEntry(Locations.toLocation(destArchive), file.getName()).getInput().close();
      }
      return destArchive.toURI();
    }

    public static class TokenizerMapper extends Mapper<byte[], byte[], Text, IntWritable>
      implements ProgramLifecycle<MapReduceTaskContext> {

      private static final IntWritable ONE = new IntWritable(1);
      private Text word = new Text();
      private final List<String> stopWords = new ArrayList<>();

      @Override
      public void map(byte[] key, byte[] value, Context context) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(Bytes.toString(value));

        while (itr.hasMoreTokens()) {
          String token = itr.nextToken();
          if (!stopWords.contains(token)) {
            word.set(token);
            context.write(word, ONE);
          }
        }
      }

      @Override
      public void initialize(MapReduceTaskContext context) throws Exception {
        Map<String, File> localFiles = context.getAllLocalFiles();
        Preconditions.checkState(localFiles.size() == 2, "Expected 2 files to have been localized.");
        Map<String, String> args = context.getRuntimeArguments();
        Preconditions.checkArgument(args.containsKey(STOPWORDS_FILE_ARG),
                                    "Runtime argument %s must be set.", STOPWORDS_FILE_ARG);
        String localFilePath = URI.create(args.get(STOPWORDS_FILE_ARG)).getPath();
        // will throw FileNotFoundException if stopwords file does not exist
        File stopWordsFile = context.getLocalFile(STOPWORDS_FILE_ALIAS);
        Preconditions.checkState(stopWordsFile.exists(), "Stopwords file %s must exist", localFilePath);
        File localArchive = context.getLocalFile(LOCAL_ARCHIVE_ALIAS);
        Preconditions.checkState(localArchive.exists(), "Local archive %s must exist", LOCAL_ARCHIVE_ALIAS);
        Preconditions.checkState(localArchive.isDirectory(), "Local archive %s must have been extracted to a " +
          "directory", LOCAL_ARCHIVE_ALIAS);
        try (BufferedReader reader = Files.newBufferedReader(stopWordsFile.toPath(), Charsets.UTF_8)) {
          String line;
          while ((line = reader.readLine()) != null) {
            stopWords.add(line);
          }
        }
      }

      @Override
      public void destroy() {
      }
    }

    /**
     *
     */
    public static class IntSumReducer extends Reducer<Text, IntWritable, byte[], byte[]> {
      @Override
      public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
          sum += val.get();
        }
        context.write(Bytes.toBytes(key.toString()), Bytes.toBytes(sum));
      }
    }
  }
}

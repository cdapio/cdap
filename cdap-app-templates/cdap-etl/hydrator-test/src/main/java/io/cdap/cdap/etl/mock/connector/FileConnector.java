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
 *
 */

package io.cdap.cdap.etl.mock.connector;

import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.InputFormatProvider;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.batch.BatchConnector;
import io.cdap.cdap.etl.api.connector.BrowseDetail;
import io.cdap.cdap.etl.api.connector.BrowseEntity;
import io.cdap.cdap.etl.api.connector.BrowseRequest;
import io.cdap.cdap.etl.api.connector.Connector;
import io.cdap.cdap.etl.api.connector.ConnectorSpec;
import io.cdap.cdap.etl.api.connector.SampleRequest;
import io.cdap.cdap.etl.api.validation.ValidationException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

/**
 * File connector which just reads from a path
 */
@Plugin(type = Connector.PLUGIN_TYPE)
@Name(FileConnector.NAME)
public class FileConnector implements BatchConnector<LongWritable, Text> {
  public static final String NAME = "FileConnector";
  public static final PluginClass PLUGIN_CLASS = getPluginClass();
  public static final Schema DEFAULT_SCHEMA = Schema.recordOf(
    "schema",
    Schema.Field.of("offset", Schema.of(Schema.Type.LONG)),
    Schema.Field.of("body", Schema.of(Schema.Type.STRING))
  );

  @Override
  public InputFormatProvider getInputFormatProvider(SampleRequest request) throws IOException {
    Job job = Job.getInstance();
    File file = new File(request.getPath());
    FileInputFormat.addInputPath(job, new Path(file.toURI()));
    return new InputFormatProvider() {
      @Override
      public String getInputFormatClassName() {
        return TextInputFormat.class.getName();
      }

      @Override
      public Map<String, String> getInputFormatConfiguration() {
        return Collections.singletonMap(FileInputFormat.INPUT_DIR,
                                        job.getConfiguration().get(FileInputFormat.INPUT_DIR));
      }
    };
  }

  @Override
  public StructuredRecord transform(LongWritable key, Text val) {
    return StructuredRecord.builder(DEFAULT_SCHEMA).set("offset", key.get()).set("body", val.toString()).build();
  }

  @Override
  public void test(FailureCollector collector) throws ValidationException {
    // no-op
  }

  @Override
  public BrowseDetail browse(BrowseRequest request) throws IOException {
    File file = new File(request.getPath());
    // if it does not exist, error out
    if (!file.exists()) {
      throw new IOException(String.format("The given path %s does not exist", request.getPath()));
    }

    // if it is not a directory, then it is not browsable, return the path itself
    if (!file.isDirectory()) {
      return BrowseDetail.builder().setTotalCount(1)
               .addEntity(BrowseEntity.builder(file.getName(), request.getPath(), "file").canSample(true).build())
               .build();
    }

    // list the files and classify them with file and directory
    File[] files = file.listFiles();
    // sort the files by the name
    Arrays.sort(files);
    int limit = request.getLimit() == null ? files.length : Math.min(request.getLimit(), files.length);
    BrowseDetail.Builder builder = BrowseDetail.builder().setTotalCount(files.length);
    for (int i = 0; i < limit; i++) {
      File listedFile = files[i];
      if (listedFile.isDirectory()) {
        builder.addEntity(BrowseEntity.builder(listedFile.getName(), listedFile.getCanonicalPath(), "directory")
                            .canSample(true).canBrowse(true).build());
        continue;
      }
      builder.addEntity(BrowseEntity.builder(listedFile.getName(), listedFile.getCanonicalPath(), "file")
                          .canSample(true).build());
    }
    return builder.build();
  }

  @Override
  public ConnectorSpec generateSpec(String path) {
    return ConnectorSpec.builder().addProperty("path", path).build();
  }

  private static PluginClass getPluginClass() {
    return PluginClass.builder().setName(NAME).setType(Connector.PLUGIN_TYPE)
             .setDescription("").setClassName(FileConnector.class.getName()).build();
  }
}

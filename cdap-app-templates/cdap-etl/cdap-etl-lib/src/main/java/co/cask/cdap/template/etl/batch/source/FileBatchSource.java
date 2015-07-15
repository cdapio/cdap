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

package co.cask.cdap.template.etl.batch.source;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.templates.plugins.PluginConfig;
import co.cask.cdap.template.etl.api.Emitter;
import co.cask.cdap.template.etl.api.PipelineConfigurer;
import co.cask.cdap.template.etl.api.batch.BatchSource;
import co.cask.cdap.template.etl.api.batch.BatchSourceContext;
import co.cask.cdap.template.etl.common.BatchFileFilter;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * A {@link BatchSource} to use any distributed file system as a Source.
 */
@Plugin(type = "source")
@Name("FileBatchSource")
@Description("Batch source for File Systems")
public class FileBatchSource extends BatchSource<LongWritable, Object, StructuredRecord> {

  public static final String INPUT_NAME_CONFIG = "input.path.name";
  public static final String INPUT_REGEX_CONFIG = "input.path.regex";
  public static final String LAST_TIME_READ = "last.time.read";
  public static final String CUTOFF_READ_TIME = "cutoff.read.time";
  public static final String USE_TIMEFILTER = "timefilter";
  public static final Schema DEFAULT_SCHEMA = Schema.recordOf(
    "event",
    Schema.Field.of("ts", Schema.of(Schema.Type.LONG)),
    Schema.Field.of("body", Schema.of(Schema.Type.STRING))
  );

  private static final String REGEX_DESCRIPTION = "Regex to filter out filenames in the path. " +
    "To use the TimeFilter, input \"timefilter\". The TimeFilter assumes that it " +
    "is reading in files with the File log naming convention of YYYY-MM-DD-HH-mm-SS-Tag. The TimeFilter " +
    "reads in files from the previous hour if the timeTable field is left blank. So if it's currently " +
    "2015-06-16-15 (June 16th 2015, 3pm), it will read in files that contain 2015-06-16-14 in the filename. " +
    "If the field timeTable is present, then it will read files in that haven't been read yet.";
  private static final String FILESYSTEM_PROPERTIES_DESCRIPTION = "JSON of the properties needed for the " +
    "distributed file system. The formatting needs to be as follows:\n{\n\t\"<property name>\" : " +
    "\"<property value>\", ...\n}. For example, the property names needed for S3 are \"fs.s3n.awsSecretAccessKey\" " +
    "and \"fs.s3n.awsAccessKeyId\".";
  private static final String PATH_DESCRIPTION = "Path to file(s) to be read. If a directory is specified, " +
    "terminate the path name with a \'/\'";
  private static final String TABLE_DESCRIPTION = "Name of the Table that keeps track of the last time files " +
    "were read in.";
  private static final String INPUT_FORMAT_CLASS_DESCRIPTION = "Name of the input format class, which must be a " +
    "subclass of FileInputFormat. Defaults to TextInputFormat";
  private static final String FILESYSTEM_DESCRIPTION = "Distributed file system to read in from.";
  private static final Gson GSON = new Gson();
  private static final Logger LOG = LoggerFactory.getLogger(FileBatchSource.class);
  private static final Type ARRAYLIST_DATE_TYPE  = new TypeToken<ArrayList<Date>>() { }.getType();
  private static final Type MAP_STRING_STRING_TYPE = new TypeToken<Map<String, String>>() { }.getType();

  private final FileBatchConfig config;
  private KeyValueTable table;
  private Date prevHour;
  private String datesToRead;

  public FileBatchSource(FileBatchConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    if (config.timeTable != null) {
      pipelineConfigurer.createDataset(config.timeTable, KeyValueTable.class, DatasetProperties.EMPTY);
    }
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws Exception {
    //SimpleDateFormat needs to be local because it is not threadsafe
    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH");

    //calculate date one hour ago, rounded down to the nearest hour
    prevHour = new Date(context.getLogicalStartTime() - TimeUnit.HOURS.toMillis(1));
    Calendar cal = Calendar.getInstance();
    cal.setTime(prevHour);
    cal.set(Calendar.MINUTE, 0);
    cal.set(Calendar.SECOND, 0);
    cal.set(Calendar.MILLISECOND, 0);
    prevHour = cal.getTime();

    Job job = context.getHadoopJob();
    Configuration conf = job.getConfiguration();
    if (config.fileSystemProperties != null) {
      Map<String, String> properties = GSON.fromJson(config.fileSystemProperties, MAP_STRING_STRING_TYPE);
      for (Map.Entry<String, String> entry : properties.entrySet()) {
        conf.set(entry.getKey(), entry.getValue());
      }
    }

    if (config.fileRegex != null) {
      conf.set(INPUT_REGEX_CONFIG, config.fileRegex);
    }
    conf.set(INPUT_NAME_CONFIG, config.path);

    if (config.timeTable != null) {
      table = context.getDataset(config.timeTable);
      String datesToRead = Bytes.toString(table.read(LAST_TIME_READ));
      if (datesToRead == null) {
        List<Date> firstRun = Lists.newArrayList(new Date(0));
        datesToRead = GSON.toJson(firstRun, ARRAYLIST_DATE_TYPE);
      }
      List<Date> attempted = Lists.newArrayList(prevHour);
      String updatedDatesToRead = GSON.toJson(attempted, ARRAYLIST_DATE_TYPE);
      if (!updatedDatesToRead.equals(datesToRead)) {
        table.write(LAST_TIME_READ, updatedDatesToRead);
      }
      conf.set(LAST_TIME_READ, datesToRead);
    }

    conf.set(CUTOFF_READ_TIME, dateFormat.format(prevHour));
    if (config.inputFormatClass != null) {
      ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
      Class<? extends FileInputFormat> classType = (Class<? extends FileInputFormat>)
        classLoader.loadClass(config.inputFormatClass);
      job.setInputFormatClass(classType);
    }
    FileInputFormat.setInputPathFilter(job, BatchFileFilter.class);
    FileInputFormat.addInputPath(job, new Path(config.path));
  }

  @Override
  public void transform(KeyValue<LongWritable, Object> input, Emitter<StructuredRecord> emitter) throws Exception {
    StructuredRecord output = StructuredRecord.builder(DEFAULT_SCHEMA)
      .set("ts", System.currentTimeMillis())
      .set("body", input.getValue().toString())
      .build();
    emitter.emit(output);
  }

  @Override
  public void onRunFinish(boolean succeeded, BatchSourceContext context) {
    if (!succeeded && table != null && USE_TIMEFILTER.equals(config.fileRegex)) {
      List<Date> existing = GSON.fromJson(Bytes.toString(table.read(LAST_TIME_READ)), ARRAYLIST_DATE_TYPE);
      List<Date> failed = GSON.fromJson(datesToRead, ARRAYLIST_DATE_TYPE);
      failed.add(prevHour);
      failed.addAll(existing);
      table.write(LAST_TIME_READ, GSON.toJson(failed, ARRAYLIST_DATE_TYPE));
    }
  }

  /**
   * Config class that contains all the properties needed for the file source.
   */
  public static class FileBatchConfig extends PluginConfig {

    @Name("fileSystem")
    @Description(FILESYSTEM_DESCRIPTION)
    private String fileSystem;

    @Name("fileSystemProperties")
    @Nullable
    @Description(FILESYSTEM_PROPERTIES_DESCRIPTION)
    private String fileSystemProperties;

    @Name("path")
    @Description(PATH_DESCRIPTION)
    private String path;

    @Name("fileRegex")
    @Nullable
    @Description(REGEX_DESCRIPTION)
    private String fileRegex;

    @Name("timeTable")
    @Nullable
    @Description(TABLE_DESCRIPTION)
    private String timeTable;

    @Name("inputFormatClass")
    @Nullable
    @Description(INPUT_FORMAT_CLASS_DESCRIPTION)
    private String inputFormatClass;

    public FileBatchConfig(String fileSystem, String path, @Nullable String regex, @Nullable String timeTable,
                           @Nullable String inputFormatClass, @Nullable String fileSystemProperties) {
      this.fileSystem = fileSystem;
      this.fileSystemProperties = fileSystemProperties;
      this.path = path;
      this.fileRegex = regex;
      this.timeTable = timeTable;
      this.inputFormatClass = inputFormatClass;
    }
  }
}

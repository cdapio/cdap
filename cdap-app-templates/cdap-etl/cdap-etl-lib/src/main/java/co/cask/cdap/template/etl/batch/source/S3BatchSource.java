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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

/**
 * A {@link BatchSource} to use S3 as a Source.
 */
@SuppressWarnings("unused")
@Plugin(type = "source")
@Name("S3")
@Description("Batch source for S3")
public class S3BatchSource extends BatchSource<LongWritable, Text, StructuredRecord> {

  private static KeyValueTable table;
  private static Date prevMinute;
  private static String currentTime;

  private static final String REGEX_DESCRIPTION = "Regex to filter out filenames in the path. " +
    "To use the TimeFilter, input \"timefilter\". The TimeFilter assumes that we " +
    "are reading in files with the S3 log naming convention of YYYY-MM-DD-HH-mm-SS-Tag. The TimeFilter " +
    "reads in files from the previous hour if the timeTable field is left blank. So if it's currently " +
    "2015-06-16-15, (June 16th 2015, 3pm), it will read in files that contain 2015-06-16-14 in the filename. " +
    "If the field timeTable is present, then it will read files in that haven't been read yet.";

  //length of 'YYYY-MM-dd-HH-mm"
  private static final int DATE_LENGTH = 16;
  private static final Logger LOG = LoggerFactory.getLogger(S3BatchSource.class);
  private S3BatchConfig config;

  private static final Schema DEFAULT_SCHEMA = Schema.recordOf(
    "event",
    Schema.Field.of("ts", Schema.of(Schema.Type.LONG)),
   Schema.Field.of("body", Schema.of(Schema.Type.STRING))
  );

  public S3BatchSource (S3BatchConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    //will create the table if it doesn't exist already
    if (config.timeTable != null) {
      pipelineConfigurer.createDataset(config.timeTable, KeyValueTable.class, DatasetProperties.EMPTY);
    }
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws Exception {
    //calculate date one minute ago, rounded down to the nearest minute
    prevMinute = new Date();
    prevMinute.setTime(System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(1));
    Calendar cal = Calendar.getInstance();
    cal.setTime(prevMinute);
    cal.set(Calendar.SECOND, 0);
    cal.set(Calendar.MILLISECOND, 0);
    prevMinute = cal.getTime();
    currentTime = new SimpleDateFormat("yyyy-MM-dd-HH-mm").format(prevMinute);

    Job job = context.getHadoopJob();
    job.getConfiguration().set("fs.s3n.awsAccessKeyId", config.accessID);
    job.getConfiguration().set("fs.s3n.awsSecretAccessKey", config.accessKey);
    if (config.fileRegex != null) {
      job.getConfiguration().set("input.path.regex", config.fileRegex);
    }
    job.getConfiguration().set("input.path.name", config.path);
    if (config.timeTable != null) {
      table = context.getDataset(config.timeTable);
    }

    FileInputFormat.setInputPathFilter(job, S3Filter.class);
    FileInputFormat.addInputPath(job, new Path(config.path));
  }

  @Override
  public void transform(KeyValue<LongWritable, Text> input, Emitter<StructuredRecord> emitter) throws Exception {
    StructuredRecord output = StructuredRecord.builder(DEFAULT_SCHEMA)
      .set("ts", System.currentTimeMillis())
      .set("body", input.getValue().toString())
      .build();
    emitter.emit(output);
  }

  @Override
  public void onRunFinish(boolean succeeded, BatchSourceContext context) {
    if (succeeded && table != null && config.fileRegex != null && config.fileRegex.equals("timefilter")) {
      table.write("lastTimeRead", currentTime);
    }
  }

  /**
   * Filter class to filter out filenames in the input path.
   */
  public static class S3Filter extends Configured implements PathFilter {
    private boolean useTimeFilter;
    private Pattern regex;
    private String pathName;

    @Override
    public boolean accept(Path path) {
      String filename = path.toString();
      System.out.println("attempt :" + filename);
      //InputPathFilter will first check the directory if a directory is given
      if (filename.equals(pathName) || filename.equals(pathName + "/")) {
        return true;
      }

      if (useTimeFilter) {
        //use stateful time filter
        if (table != null) {
          String lastRead = Bytes.toString(table.read("lastTimeRead"));
          SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd-HH-mm");
          Date dateLastRead;
          try {
            dateLastRead = sdf.parse(lastRead);
          } catch (Exception e) {
            dateLastRead = new Date();
            dateLastRead.setTime(0);
          }

          Date fileDate;
          try {
            System.out.println("FILENAME puppies: " + path.toString());
            System.out.println("PATHNAME :" + pathName);
            fileDate = sdf.parse(path.getName().substring(0, DATE_LENGTH));
          } catch (ParseException pe) {
            //this should never happen
            LOG.warn("Couldn't parse file: " + path.getName());
            fileDate = prevMinute;
          }

          if (fileDate.compareTo(dateLastRead) > 0 && fileDate.compareTo(prevMinute) <= 0) {
            return true;
          }
          return false;
        } else {
          //use hourly time filter
          Date prevHour = new Date();
          prevHour.setTime(System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1));
          String currentTime = new SimpleDateFormat("yyyy-MM-dd-HH").format(prevHour);

          if (filename.contains(currentTime)) {
            return true;
          }
          return false;
        }
      }

      //use regex
      Matcher matcher = regex.matcher(filename);
      return matcher.matches();
    }

    public void setConf(Configuration conf) {
      if (conf != null) {
        pathName = conf.get("input.path.name", "/");

        //path is a directory so remove trailing '/'
        if (pathName.endsWith("/")) {
          pathName = pathName.substring(0, pathName.length() - 1);
        }

        String input = conf.get("input.path.regex", ".*");
        if (input.equals("timefilter")) {
          useTimeFilter = true;
        } else {
          useTimeFilter = false;
          regex = Pattern.compile(input);
        }
      }
    }
  }

  /**
   * Config class that contains all the properties needed for the S3 source.
   */
  public static class S3BatchConfig extends PluginConfig {

    @Name("name")
    @Description("Name of the S3 source")
    private String name;

    @Name("accessID")
    @Description("Access Id for s3")
    private String accessID;

    @Name("accessKey")
    @Description("Access Key for s3")
    private String accessKey;

    @Name("path")
    @Description("Path to file(s) to be read. If a directory is specified, terminate the path name with a \'\\\'")
    private String path;

    @Name("fileRegex")
    @Nullable
    @Description(REGEX_DESCRIPTION)
    private String fileRegex;

    @Name("timeTable")
    @Nullable
    @Description("Name of the Table that keeps track of the last time files were read in.")
    private String timeTable;

    public S3BatchConfig(String name, String accessID, String accessKey, String path, String regex,
                         String timeTable) {
      this.name = name;
      this.accessID = accessID;
      this.accessKey = accessKey;
      this.path = path;
      this.fileRegex = regex;
      this.timeTable = timeTable;
    }

  }
}

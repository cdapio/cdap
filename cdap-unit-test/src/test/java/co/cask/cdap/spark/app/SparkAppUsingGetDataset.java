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
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

/**
 * App to test dataset access/update using getDataset() from a spark program
 */
public class SparkAppUsingGetDataset extends AbstractApplication {

  private static final Pattern CLF_LOG_PATTERN = Pattern.compile(
    //   IP                    id    user      date          request     code     size    referrer
    "^([\\d.]+|[:][:][\\d]) (\\S+) (\\S+) \\[([^\\]]+)\\] \"([^\"]+)\" (\\d{3}) ([-\"\\d]+) \"([^\"]+)\" " +
      // user agent
      "\"([^\"]+)\"");

  @Override
  public void configure() {
    createDataset("logs", FileSet.class, FileSetProperties.builder()
      .setInputFormat(TextInputFormat.class)
      .setOutputFormat(TextOutputFormat.class).build());
    createDataset("logStats", KeyValueTable.class.getName());
    addSpark(new SparkLogParser());
    addSpark(new ScalaSparkLogParser());
  }

  public static final class LogKey implements Serializable {
    private final String ip;
    private final String user;
    private final String request;
    private final int code;

    public LogKey(String ip, String user, String request, int code) {
      this.ip = ip;
      this.user = user;
      this.request = request;
      this.code = code;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      LogKey that = (LogKey) o;

      return Objects.equals(ip, that.ip) &&
        Objects.equals(user, that.user) &&
        Objects.equals(request, that.request) &&
        code == that.code;
    }

    @Override
    public int hashCode() {
      return Objects.hash(ip, user, request, code);
    }

    @Override
    public String toString() {
      return "LogKey{" +
        "ip='" + ip + '\'' +
        ", user='" + user + '\'' +
        ", request='" + request + '\'' +
        ", code=" + code +
        '}';
    }
  }

  public static final class LogStats implements Serializable {
    private final int count;
    private final int size;

    public LogStats(int count, int size) {
      this.count = count;
      this.size = size;
    }

    public LogStats aggregate(LogStats that) {
      return new LogStats(count + that.count, size + that.size);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      SparkAppUsingGetDataset.LogStats that = (SparkAppUsingGetDataset.LogStats) o;

      return count == that.count && size == that.size;
    }

    @Override
    public int hashCode() {
      return Objects.hash(count, size);
    }

    @Override
    public String toString() {
      return "LogStats{" +
        "count=" + count +
        ", size=" + size +
        '}';
    }
  }

  @Nullable
  static Tuple2<LogKey, LogStats> parse(Text log) {
    Matcher matcher = CLF_LOG_PATTERN.matcher(log.toString());
    if (matcher.find()) {
      String ip = matcher.group(1);
      String user = matcher.group(3);
      String request = matcher.group(5);
      int code = Integer.parseInt(matcher.group(6));
      int size = Integer.parseInt(matcher.group(7));
      return new Tuple2<>(new LogKey(ip, user, request, code), new LogStats(1, size));
    }
    return null;
  }
}

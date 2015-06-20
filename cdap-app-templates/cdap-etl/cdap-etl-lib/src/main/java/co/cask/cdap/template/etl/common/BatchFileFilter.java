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

package co.cask.cdap.template.etl.common;

import co.cask.cdap.template.etl.batch.source.FileBatchSource;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Filter class to filter out filenames in the input path.
 */
public class BatchFileFilter extends Configured implements PathFilter {
  private boolean useTimeFilter;
  private Pattern regex;
  private String pathName;
  private String lastRead;
  private Date prevMinute;
  private static final Logger LOG = LoggerFactory.getLogger(FileBatchSource.class);
  //length of 'YYYY-MM-dd-HH-mm"
  private static final int DATE_LENGTH = 16;
  private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd-HH-mm");

  @Override
  public boolean accept(Path path) {
    String filename = path.toString();
    //InputPathFilter will first check the directory if a directory is given
    if (filename.equals(pathName) || filename.equals(pathName + "/")) {
      return true;
    }

    //filter by file name using regex from configuration
    if (!useTimeFilter) {
      Matcher matcher = regex.matcher(filename);
      return matcher.matches();
    }

    //use hourly time filter
    if (lastRead.equals("-1")) {
      Date prevHour = new Date(System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1));
      String currentTime = new SimpleDateFormat("yyyy-MM-dd-HH").format(prevHour);
      return filename.contains(currentTime);
    }

    //use stateful time filter
    Date dateLastRead;
    try {
      dateLastRead = sdf.parse(lastRead);
    } catch (Exception e) {
      dateLastRead = new Date();
      dateLastRead.setTime(0);
    }

    Date fileDate;
    try {
      fileDate = sdf.parse(path.getName().substring(0, DATE_LENGTH));
    } catch (ParseException pe) {
      //this should never happen
      LOG.warn("Couldn't parse file: " + path.getName());
      fileDate = prevMinute;
    }
    return fileDate.compareTo(dateLastRead) > 0 && fileDate.compareTo(prevMinute) <= 0;
  }

  public void setConf(Configuration conf) {
    if (conf == null) {
      return;
    }
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
    lastRead = conf.get("last.time.read", "-1");
    try {
      prevMinute = sdf.parse(conf.get("cutoff.read.time"));
    } catch (ParseException pe) {
      prevMinute = new Date(System.currentTimeMillis());
    }
  }
}


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

package co.cask.cdap.etl.common;

import co.cask.cdap.etl.batch.source.FileBatchSource;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Filter class to filter out filenames in the input path.
 *
 * The stateful time filter works as follows. A list of times are kept in the table in FileBatchSource. The list always
 * denotes ranges of times where files have not been read in yet. The list is read in prepareRun and then converted to
 * a string and passed in as dateRangesToRead, which is described below. The files are assumed to succeed so that files
 * will not be read in multiple times. Thus, a singleton element is written into the table as the list denoting that
 * the only files that haven't been read in are after that element. If the read fails, then dateRangesToRead with
 * prevHour appended to the end is inserted to the beginning of whatever the list is now in the table. This is done
 * because these were exactly the time ranges that we wanted to read from (which failed, so we should reinsert them
 * to try again). If the read succeeds then we don't do anything since we assumed that the read would succeed
 * initially.
 */
public class BatchFileFilter extends Configured implements PathFilter {

  private static final Logger LOG = LoggerFactory.getLogger(FileBatchSource.class);
  private static final Gson GSON = new Gson();
  private static final Type ARRAYLIST_DATE_TYPE  = new TypeToken<ArrayList<Date>>() { }.getType();
  private static final String DATE_FORMAT = "yyyy-MM-dd-HH";
  private static final int DATE_LENGTH = DATE_FORMAT.length();
  private final SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT);
  private boolean useTimeFilter;
  private Pattern regex;
  private String pathName;
  private String lastRead;
  private Date prevHour;

  /*
   * dateRangesToRead is an odd length List of Dates. The non terminal elements are tuples of Dates that
   * denote ranges of time that are acceptable for the file to have been created during. The start times are inclusive
   * and the end times are exclusive. The range of time between the terminal element and prevHour is the final
   * acceptable range for the file to have been created during.
   */
  private List<Date> dateRangesToRead;

  @Override
  public boolean accept(Path path) {
    String filePathName = path.toString();
    //The path filter will first check the directory if a directory is given
    if (filePathName.equals(pathName) || filePathName.equals(pathName + "/")) {
      return true;
    }

    //filter by file name using regex from configuration
    if (!useTimeFilter) {
      Matcher matcher = regex.matcher(filePathName);
      return matcher.matches();
    }

    //use hourly time filter
    if (lastRead.equals("-1")) {
      String currentTime = sdf.format(prevHour);
      return filePathName.contains(currentTime);
    }

    //use stateful time filter
    Date fileDate;
    String filename = path.getName();
    try {
      fileDate = sdf.parse(filename.substring(0, DATE_LENGTH));
    } catch (Exception pe) {
      //Try to parse cloudfront format
      try {
        int startIndex = filename.indexOf(".") + 1;
        fileDate = sdf.parse(filename.substring(startIndex, startIndex + DATE_LENGTH));
      } catch (Exception e) {
        LOG.warn("Couldn't parse file: " + filename);
        return false;
      }
    }
    return isWithinRange(fileDate);
  }

  @Override
  public void setConf(Configuration conf) {
    if (conf == null) {
      return;
    }
    pathName = conf.get(FileBatchSource.INPUT_NAME_CONFIG, "/");

    //path is a directory so remove trailing '/'
    if (pathName.endsWith("/")) {
      pathName = pathName.substring(0, pathName.length() - 1);
    }

    String input = conf.get(FileBatchSource.INPUT_REGEX_CONFIG, ".*");
    if (input.equals(FileBatchSource.USE_TIMEFILTER)) {
      useTimeFilter = true;
    } else {
      useTimeFilter = false;
      regex = Pattern.compile(input);
    }
    lastRead = conf.get(FileBatchSource.LAST_TIME_READ, "-1");

    if (!lastRead.equals("-1")) {
      dateRangesToRead = GSON.fromJson(lastRead, ARRAYLIST_DATE_TYPE);
    }

    try {
      prevHour = sdf.parse(conf.get(FileBatchSource.CUTOFF_READ_TIME));
    } catch (ParseException pe) {
      prevHour = new Date(System.currentTimeMillis());
    }
  }

  /**
   * Determines if a file should be read in
   *
   * Iterates through the list dateRangesToRead and returns true if the filedate falls between one of the tuples, or
   * if the filedate is between the terminal element and prevHour.
   *
   * @param fileDate when the file was created
   * @return true if the file is to be read, false otherwise
   */
  private boolean isWithinRange(Date fileDate) {
    for (int i = 0; i < dateRangesToRead.size() / 2; i++) {
      if (fileDate.compareTo(dateRangesToRead.get(2 * i)) >= 0 &&
        fileDate.compareTo(dateRangesToRead.get(2 * i + 1)) < 0) {
        return true;
      }
    }
    return fileDate.compareTo(dateRangesToRead.get(dateRangesToRead.size() - 1)) >= 0
      && fileDate.compareTo(prevHour) < 0;
  }
}

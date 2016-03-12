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

package co.cask.cdap.logging.read;

import co.cask.cdap.common.logging.LoggingContext;
import co.cask.cdap.common.twill.LocalLocationFactory;
import co.cask.cdap.logging.context.LoggingContextHelper;
import co.cask.cdap.logging.filter.AndFilter;
import co.cask.cdap.logging.filter.Filter;
import co.cask.cdap.logging.filter.FilterParser;
import co.cask.cdap.logging.serialize.LogSchema;
import co.cask.cdap.proto.ProgramType;
import com.google.common.collect.ImmutableList;
import org.apache.avro.Schema;
import org.apache.twill.filesystem.Location;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

/**
 * Created by vinishavyasa on 3/11/16.
 */
public class TestFileReader {



  public static void main(String[] args) throws Exception {

    long fromMillis = 1457747608000L;
    long toMillis = 1457747623000L;
    int maxEvents = 50;
    Filter emptyFilter = FilterParser.parse("");

    //default/apps/TestFour/programs/mapreduce/ETLMapReduce/runs/e9d23201-e7e2-11e5-a512-42010af0001b

    LoggingContext loggingContext =
      LoggingContextHelper.getLoggingContextWithRunId("default", "TestFour", "ETLMapReduce",
                                                      ProgramType.valueOfCategoryName("mapreduce"),
                                                      "3729d3c1-e7f5-11e5-94bb-42010af0001b");
    ReadRange readRange = new ReadRange(TimeUnit.MILLISECONDS.toMillis(fromMillis),
                                        TimeUnit.MILLISECONDS.toMillis(toMillis), LogOffset.INVALID_KAFKA_OFFSET);


    LocalLocationFactory localLocationFactory = new LocalLocationFactory();

    Location file = localLocationFactory.create("Users/vinishavyasa/Desktop/1457747617200.avro");

    Filter logFilter = new AndFilter(ImmutableList.of(LoggingContextHelper.createFilter(loggingContext),
                                                      emptyFilter));

    System.out.println("Filter: " + logFilter.toString());
    System.out.println("system time in mills: " + System.currentTimeMillis());

    long fromTimeMs = readRange.getToMillis() - 1;

    Schema schema = new LogSchema().getAvroSchema();
    AvroFileReader logReader = new AvroFileReader(schema);
    System.out.println("Before reading");
    Collection<LogEvent> events = logReader.readLogPrev(file, logFilter, fromTimeMs, maxEvents);
    System.out.println("Events size: " + events.size());


  }
}

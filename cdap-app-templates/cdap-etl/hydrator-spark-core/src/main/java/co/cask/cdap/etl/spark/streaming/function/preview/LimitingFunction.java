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

package co.cask.cdap.etl.spark.streaming.function.preview;

import org.apache.spark.api.java.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Function used to limit the number of records
 *
 * @param <T> type of object in the rdd.
 */
public class LimitingFunction<T> implements Function<T, Boolean> {
  private static final Logger LOG = LoggerFactory.getLogger(LimitingFunction.class);
  private final int numOfRecordsLimited;
  private int numOfRecordsEmitted;

  public LimitingFunction(int numOfRecordsLimited) {
    LOG.info("Yaojie - limit records: {}, emit records: {}", numOfRecordsLimited, numOfRecordsEmitted);
    this.numOfRecordsLimited = numOfRecordsLimited;
    numOfRecordsEmitted = 0;
  }

  @Override
  public Boolean call(T v1) throws Exception {
    return numOfRecordsEmitted++ < numOfRecordsLimited;
  }
}

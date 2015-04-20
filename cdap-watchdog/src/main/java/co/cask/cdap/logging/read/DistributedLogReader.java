/*
 * Copyright © 2014 Cask Data, Inc.
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
import co.cask.cdap.logging.filter.Filter;
import com.google.inject.Inject;

/**
 * Reads logs in a distributed setup.
 */
public final class DistributedLogReader implements LogReader {
  private final KafkaLogReader kafkaLogReader;
  private final FileLogReader fileLogReader;

  /**
   * Creates a DistributedLogReader object.
   */
  @Inject
  public DistributedLogReader(KafkaLogReader kafkaLogReader, FileLogReader fileLogReader) {
    this.kafkaLogReader = kafkaLogReader;
    this.fileLogReader = fileLogReader;
  }

  @Override
  public void getLogNext(final LoggingContext loggingContext, final LogOffset fromOffset, final int maxEvents,
                              final Filter filter, final Callback callback) {
    kafkaLogReader.getLogNext(loggingContext, fromOffset, maxEvents, filter, callback);
  }

  @Override
  public void getLogPrev(final LoggingContext loggingContext, final LogOffset fromOffset, final int maxEvents,
                              final Filter filter, final Callback callback) {
    kafkaLogReader.getLogPrev(loggingContext, fromOffset, maxEvents, filter, callback);
  }


  @Override
  public void getLog(final LoggingContext loggingContext, final long fromTimeMs, final long toTimeMs,
                          final Filter filter, final Callback callback) {
    fileLogReader.getLog(loggingContext, fromTimeMs, toTimeMs, filter, callback);
  }
}

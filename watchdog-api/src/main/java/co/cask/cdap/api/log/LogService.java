/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap.api.log;

/**
 * Provides access to log messages.
 */
public interface LogService {
  /**
   * Performs ongoing tailing of the logs messages starting from the given position relative to the end.
   * <p>
   *   E.g. tail(query, 1024) will return last 1KB of messages and continue to output as new data appears.
   * </p>
   *
   * @param query specifies which messages to select
   * @param startPositionInBytes position to start tailing from. It is relative to the end position.
   * @return log messages. Iterator has non-blocking behaviour: in case there's no messages to read, it will return
   *         null. In that case client code can try to read messages emitted after last call again after some time.
   * @throws LogServiceException request cannot be completed.
   * @throws UnsupportedLogQueryException when given query is not supported by the back-end.
   */
  Iterable<LogMessage> tail(LogQuery query, int startPositionInBytes)
    throws LogServiceException, UnsupportedLogQueryException;

  /**
   * Selects logs messages of the specified time interval.
   *
   * @param query specifies which messages to select
   * @param startTs start point of the time range selection
   * @param endTs end point of the time range selection
   * @return log messages
   * @throws UnsupportedLogQueryException when given query is not supported by the back-end.
   */
  Iterable<LogMessage> query(LogQuery query, long startTs, long endTs)
    throws LogServiceException, UnsupportedLogQueryException;
}

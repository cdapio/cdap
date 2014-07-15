/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.common.logging;

import java.io.IOException;

/**
 * Represents classes that can write log.
 */
public interface LogWriter {

  /**
   * Configures this writer.
   * @param config specifies what log to write to
   */
  public void configure(LogConfiguration config) throws IOException;

  /**
   * logs a single event. This must ensure that - if desired - the event is
   * persisted.
   */
  public void log(LogEvent event) throws IOException;

  /**
   * return the current write position. This is used as a hint for the length
   * of the current log file - it has not been closed and hence the file system
   * status does not reflect its true size.
   */
  public long getWritePosition() throws IOException;

  /**
   * closes the writer and flushes and closes all open files.
   */
  public void close() throws IOException;
}

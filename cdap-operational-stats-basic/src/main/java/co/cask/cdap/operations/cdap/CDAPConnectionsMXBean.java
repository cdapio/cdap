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

package co.cask.cdap.operations.cdap;

import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import javax.management.MXBean;

/**
 * {@link MXBean} for reporting CDAP Router request statistics.
 */
public interface CDAPConnectionsMXBean {

  /**
   * Returns the total number of requests in the last hour.
   */
  long getTotalRequests();

  /**
   * Returns the number of requests that were responded to with a {@link HttpResponseStatus#OK} in the last hour.
   */
  long getSuccessful();

  /**
   * Returns the number of requests that were responded to with a {@link HttpResponseStatus#BAD_REQUEST}
   * in the last hour.
   */
  long getClientErrors();

  /**
   * Returns the number of requests that were responded to with a {@link HttpResponseStatus#INTERNAL_SERVER_ERROR}
   * in the last hour.
   */
  long getServerErrors();

  /**
   * Returns the number of {@code ERROR} logs in the last hour.
   */
  long getErrorLogs();

  /**
   * Returns the number of {@code WARN} logs in the last hour.
   */
  long getWarnLogs();
}

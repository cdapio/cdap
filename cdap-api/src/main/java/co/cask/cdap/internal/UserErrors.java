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

package co.cask.cdap.internal;

/**
 * Collections of user errors key. 
 */
public final class UserErrors {

  public static final String PROGRAM_NOT_FOUND = "program-not-found";
  public static final String RUNTIME_INFO_NOT_FOUND = "runtime-info-not-found";
  public static final String INVALID_INSTANCES = "invalid-instances";
  public static final String INVALID_FLOWLET_EXISTS = "invalid-flowlet-exists";
  public static final String INVALID_FLOWLET_NULL = "invalid-flowlet-null";
  public static final String INVALID_FLOWLET_NAME = "invalid-flowlet-name";
  public static final String INVALID_STREAM_NULL = "invalid-stream-null";
}

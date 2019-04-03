/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.internal.app.services;

/**
 * An exception to indicate a program run was aborted abnormally without CDAP knowing.
 * This is exception is mainly used by the {@link RunRecordCorrectorService} as the failure reason
 * when it fixes run records for non-running programs.
 */
public class ProgramRunAbortedException extends RuntimeException {

  ProgramRunAbortedException(String msg) {
    super(msg);
  }
}

/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.master.spi.twill;


import org.apache.twill.api.TwillPreparer;

/**
 * An extension of TwillPreparer used to add extra functionalities for CDAP.
 */
public interface ExtendedTwillPreparer extends TwillPreparer {

  /**
   * Set size limit for workdir volume in kube twill application which is an emptydir.
   *
   * @param sizeLimitInMB volume size limit in MB
   */
  ExtendedTwillPreparer setWorkdirSizeLimit(int sizeLimitInMB);
}

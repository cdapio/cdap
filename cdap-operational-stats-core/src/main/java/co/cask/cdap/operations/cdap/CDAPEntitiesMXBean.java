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

import javax.management.MXBean;

/**
 * {@link MXBean} for reporting CDAP entities
 */
public interface CDAPEntitiesMXBean {
  /**
   * Returns the number of namespaces in CDAP
   */
  int getNamespaces();

  /**
   * Returns the number of artifacts in CDAP
   */
  int getArtifacts();

  /**
   * Returns the number of applications in CDAP
   */
  int getApplications();

  /**
   * Returns the number of programs in CDAP
   */
  int getPrograms();

  /**
   * Returns the number of datasets in CDAP
   */
  int getDatasets();

  /**
   * Returns the number of streams in CDAP
   */
  int getStreams();

  /**
   * Returns the number of stream views in CDAP
   */
  int getStreamViews();
}

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

/**
 * Defines APIs for accessing and managing data within CDAP.
 * <p>
 * Implement {@link co.cask.cdap.api.dataset.DatasetDefinition} or {@link co.cask.cdap.api.dataset.Dataset}
 * to define custom data set types.
 * <p>
 * Otherwise, use pre-packaged types available in the {@link co.cask.cdap.api.dataset.lib}  
 * and {@link co.cask.cdap.api.dataset.table} packages.
 */
package co.cask.cdap.api.dataset;

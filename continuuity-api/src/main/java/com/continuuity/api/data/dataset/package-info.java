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

/**
 * Implements DataSets for object stores and indexed, key/value, and time series tables.
 * <ul>
 * <li>{@link com.continuuity.api.data.dataset.IndexedTable} implements a table that can be accessed via a secondary
 * key</li>
 * <li>{@link com.continuuity.api.data.dataset.KeyValueTable} implements a simple key/value dataset on top of
 * a table</li>
 * <li>{@link com.continuuity.api.data.dataset.SimpleTimeseriesTable} implements a time series dataset</li>
 * </ul>
 */
package com.continuuity.api.data.dataset;

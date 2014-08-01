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

package com.continuuity.data2.datafabric.dataset.service.mds;

import com.continuuity.api.dataset.Dataset;
import com.continuuity.data2.dataset2.tx.TxContext;

import java.util.Map;

/**
 * Provides transactional access to datasets storing metadata of datasets
 */
public final class MDSDatasets extends TxContext {
  MDSDatasets(Map<String, ? extends Dataset> datasets) {
    super(datasets);
  }

  public DatasetInstanceMDS getInstanceMDS() {
    return (DatasetInstanceMDS) getDataset("datasets.instance");
  }

  public DatasetTypeMDS getTypeMDS() {
    return (DatasetTypeMDS) getDataset("datasets.type");
  }
}

/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.proto.metadata.lineage;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Class representing the individual record in the {@link FieldLineageSummary}.
 */
public class FieldLineageSummaryRecord {
  private final String namespace;
  private final String dataset;
  private final List<String> fields;

  public FieldLineageSummaryRecord(String namespace, String dataset, List<String> fields) {
    this.namespace = namespace;
    this.dataset = dataset;
    this.fields = Collections.unmodifiableList(new ArrayList<>(fields));
  }

  public String getNamespace() {
    return namespace;
  }

  public String getDataset() {
    return dataset;
  }

  public List<String> getFields() {
    return fields;
  }
}

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

package co.cask.cdap.internal.app.store;

import co.cask.cdap.app.ApplicationSpecification;
import co.cask.cdap.internal.app.ApplicationSpecificationAdapter;
import co.cask.cdap.internal.io.ReflectionSchemaGenerator;
import com.google.common.base.Objects;

/**
 * Holds application metadata
 */
public class ApplicationMeta {
  private static final ApplicationSpecificationAdapter ADAPTER =
    ApplicationSpecificationAdapter.create(new ReflectionSchemaGenerator());

  private final String id;
  private final ApplicationSpecification spec;
  private final String archiveLocation;
  // NOTE: we need lastDeployTs since not all (e.g. persisting app jar) covered with tx,
  //       and currently we use it resolve some races :(
  private final long lastUpdateTs;

  public ApplicationMeta(String id, ApplicationSpecification spec, String archiveLocation) {
    this.id = id;
    this.spec = spec;
    this.archiveLocation = archiveLocation;
    this.lastUpdateTs = System.currentTimeMillis();
  }

  public static ApplicationMeta updateSpec(ApplicationMeta original, ApplicationSpecification newSpec) {
    return new ApplicationMeta(original.id, newSpec, original.archiveLocation);
  }

  public String getId() {
    return id;
  }

  public ApplicationSpecification getSpec() {
    return spec;
  }

  public String getArchiveLocation() {
    return archiveLocation;
  }

  public long getLastUpdateTs() {
    return lastUpdateTs;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("id", id)
      .add("spec", ADAPTER.toJson(spec))
      .add("archiveLocation", archiveLocation)
      .add("lastUpdateTs", lastUpdateTs)
      .toString();
  }
}

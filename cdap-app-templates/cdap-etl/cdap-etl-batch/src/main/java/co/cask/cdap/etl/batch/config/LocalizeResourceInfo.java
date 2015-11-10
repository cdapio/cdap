/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.etl.batch.config;

/**
 * Object to represent resources to localize in an ETL Pipeline Config.
 */
public class LocalizeResourceInfo {
  private final String name;
  private final String uri;
  private final boolean archive;

  public LocalizeResourceInfo(String name, String uri) {
    this(name, uri, false);
  }

  public LocalizeResourceInfo(String name, String uri, boolean archive) {
    this.name = name;
    this.uri = uri;
    this.archive = archive;
  }

  public String getName() {
    return name;
  }

  public String getURI() {
    return uri;
  }

  public boolean isArchive() {
    return archive;
  }
}

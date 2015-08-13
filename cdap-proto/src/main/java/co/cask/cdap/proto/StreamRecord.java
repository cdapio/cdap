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

package co.cask.cdap.proto;

/**
 * Represents a stream in an HTTP response.
 */
public class StreamRecord {

  private String type;
  private String id;
  private String name;
  private String specification;

  public StreamRecord(String name, String specification) {
    this("Stream", name, name, specification);
  }

  @Deprecated
  public StreamRecord(String type, String id, String name, String specification) {
    this.type = type;
    this.id = id;
    this.name = name;
    this.specification = specification;
  }

  public String getType() {
    return type;
  }

  @Deprecated
  public String getId() {
    return id;
  }

  public String getName() {
    return name;
  }

  public String getSpecification() {
    return specification;
  }

}

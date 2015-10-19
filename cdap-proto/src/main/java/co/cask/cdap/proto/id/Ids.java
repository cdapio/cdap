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
package co.cask.cdap.proto.id;

/**
 * Helper methods for constructing {@link EntityId}s.
 */
public class Ids {

  private Ids() {}

  public static NamespaceId namespace(String namespace) {
    return new NamespaceId(namespace);
  }

  public static QueryId query(String query) {
    return new QueryId(query);
  }

  public static SystemServiceId systemService(String systemService) {
    return new SystemServiceId(systemService);
  }
}

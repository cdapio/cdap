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

package co.cask.cdap.data.tools;

import co.cask.cdap.common.namespace.NamespaceQueryAdmin;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceMeta;

import java.util.List;

/**
 * Implementation of {@link NamespaceQueryAdmin} that doesn't support any operations.
 */
public class NoOpNamespaceQueryAdmin implements NamespaceQueryAdmin {

  @Override
  public List<NamespaceMeta> list() throws Exception {
    throw new UnsupportedOperationException("Listing of Namespaces is not supported.");
  }

  @Override
  public NamespaceMeta get(Id.Namespace namespaceId) throws Exception {
    throw new UnsupportedOperationException("Getting NamespaceMeta is not supported.");
  }

  @Override
  public boolean exists(Id.Namespace namespaceId) throws Exception {
    throw new UnsupportedOperationException("Existence check of a Namespace is not supported.");
  }
}

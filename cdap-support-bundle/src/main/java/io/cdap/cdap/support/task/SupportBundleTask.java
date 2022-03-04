/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.support.task;

import io.cdap.cdap.common.NotFoundException;
import io.kubernetes.client.openapi.ApiException;
import org.json.JSONException;

import java.io.IOException;

/** Establishes an interface for support bundle task */
public interface SupportBundleTask {
  /** Collect Log or pipeline info */
  void collect() throws IOException, NotFoundException, JSONException, ApiException;
}

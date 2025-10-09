/*
 * Copyright © 2025 Cask Data, Inc.
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

package io.cdap.cdap.common.encryption;

/**
 * Defines the type of worker that is allowed to decrypt data for a given URI. This is written to a
 * response header to inform the requesting worker.
 */
public enum WorkerDecryptionScope {
  NONE,
  PREVIEW_RUNNER,
  TASK_WORKER,
  ANY_WORKER
}

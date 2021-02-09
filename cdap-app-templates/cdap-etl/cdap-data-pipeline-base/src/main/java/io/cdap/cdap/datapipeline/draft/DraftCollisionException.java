/*
 * Copyright © 2020 Cask Data, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package io.cdap.cdap.datapipeline.draft;

import java.net.HttpURLConnection;

/**
 * Exception for draft collision
 */
public class DraftCollisionException extends CodedException {
  public DraftCollisionException(DraftId draftId) {
    super(HttpURLConnection.HTTP_CONFLICT,
          String.format("The current version of draft '%s' in namespace '%s' conflicts with the new request.",
                        draftId.getId(),
                        draftId.getNamespace()));
  }
}

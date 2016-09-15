/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

package co.cask.cdap.app.verification;

import co.cask.cdap.error.Err;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.EntityId;


/**
 * @param <T> Type of thing to get verified.
 */
public abstract class AbstractVerifier<T> implements Verifier<T> {

  @Override
  public VerifyResult verify(ApplicationId appId, T input) {
    // Checks if DataSet name is an ID
    String name = getName(input);
    if (!EntityId.isValidId(name)) {
      return VerifyResult.failure(Err.NOT_AN_ID, name);
    }
    return VerifyResult.success();
  }
  protected abstract String getName(T input);
}

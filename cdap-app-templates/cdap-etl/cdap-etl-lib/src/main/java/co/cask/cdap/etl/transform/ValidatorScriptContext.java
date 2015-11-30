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

package co.cask.cdap.etl.transform;

import co.cask.cdap.etl.api.LookupConfig;
import co.cask.cdap.etl.api.LookupProvider;
import co.cask.cdap.etl.api.StageMetrics;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * Context passed to {@link co.cask.cdap.etl.transform.ValidatorTransform} script
 */
public class ValidatorScriptContext extends ScriptContext {
  private final Map<String, Object> validators;

  public ValidatorScriptContext(Logger logger, StageMetrics metrics, LookupProvider lookup,
                                @Nullable LookupConfig lookupConfig, JavaTypeConverters js,
                                Map<String, Object> validators) {
    super(logger, metrics, lookup, lookupConfig, js);
    this.validators = validators;
  }

  public Object getValidator(String validatorName) {
    Preconditions.checkArgument(validators.containsKey(validatorName),
                                String.format("Invalid validator name %s", validatorName));
    return validators.get(validatorName);
  }
}

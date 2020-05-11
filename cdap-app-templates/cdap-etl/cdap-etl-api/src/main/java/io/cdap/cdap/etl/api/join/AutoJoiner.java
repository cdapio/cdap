/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.etl.api.join;

import io.cdap.cdap.api.annotation.Beta;

import javax.annotation.Nullable;

/**
 * Joins data from two or more input stages together.
 *
 * For example, the following represents a left outer join on 'purchases'.'user_id' = 'users'.'id':
 *
 * <pre>
 *   {@code
 *     JoinDefinition define(AutoJoinerContext context) {
 *       JoinStage purchases = JoinStage.builder(context.getInputStages().get("purchases"))
 *         .isRequired()
 *         .build();
 *       JoinStage users = JoinStage.builder(context.getInputStages().get("users"))
 *         .isOptional()
 *         .build();
 *
 *       JoinCondition condition = JoinCondition.onKeys()
 *         .addKey(new JoinKey("users", Arrays.asList("id")))
 *         .addKey(new JoinKey("purchases", Arrays.asList("user_id")))
 *         .build();
 *
 *       return JoinDefinition.builder()
 *         // stage name, field name, optional alias
 *         .select(new Field("purchases", "id", "purchase_id"),
 *                 new Field("purchases", "user_id"),
 *                 new Field("users", "email"))
 *         .from(purchases, users)
 *         .on(condition)
 *         .build()
 *     }
 *   }
 * </pre>
 */
@Beta
public interface AutoJoiner {

  /**
   * @return definition about what type of join to execute, or null if the definition cannot be created due to
   *   macro values not being evaluated yet.
   */
  @Nullable
  JoinDefinition define(AutoJoinerContext context);

}

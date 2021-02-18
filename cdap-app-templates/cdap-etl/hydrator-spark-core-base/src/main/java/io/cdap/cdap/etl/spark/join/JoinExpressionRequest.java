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

package io.cdap.cdap.etl.spark.join;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.join.JoinCondition;
import io.cdap.cdap.etl.api.join.JoinDistribution;
import io.cdap.cdap.etl.api.join.JoinField;
import io.cdap.cdap.etl.api.join.JoinStage;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Request to join some collection to another collection on an arbitrary expression.
 */
public class JoinExpressionRequest {
  private final String stageName;
  private final List<JoinField> fields;
  private final JoinCollection left;
  private final JoinCollection right;
  private final JoinCondition.OnExpression condition;
  private final Schema outputSchema;

  public JoinExpressionRequest(String stageName, List<JoinField> fields, JoinCollection left, JoinCollection right,
                               JoinCondition.OnExpression condition, Schema outputSchema) {
    this.stageName = stageName;
    this.fields = fields;
    this.left = left;
    this.right = right;
    this.condition = condition;
    this.outputSchema = outputSchema;
  }

  /**
   * Rename the original names for the left and right sides.
   */
  public JoinExpressionRequest rename(String newLeftName, String newRightName) {
    String originalLeft = left.getStage();
    String originalRight = right.getStage();
    List<JoinField> renamedFields = new ArrayList<>(fields.size());
    for (JoinField field : fields) {
      if (field.getStageName().equals(originalLeft)) {
        renamedFields.add(new JoinField(newLeftName, field.getFieldName(), field.getAlias()));
      } else {
        renamedFields.add(new JoinField(newRightName, field.getFieldName(), field.getAlias()));
      }
    }

    JoinCollection renamedLeft = new JoinCollection(newLeftName, left.getData(), left.getSchema(), left.getKey(),
                                                    left.isRequired(), left.isBroadcast());
    JoinCollection renamedRight = new JoinCollection(newRightName, right.getData(), right.getSchema(), right.getKey(),
                                                     right.isRequired(), right.isBroadcast());

    Map<String, String> renamedAliases = new HashMap<>(condition.getDatasetAliases().size());
    String leftAlias = condition.getDatasetAliases().getOrDefault(originalLeft, originalLeft);
    String rightAlias = condition.getDatasetAliases().getOrDefault(originalRight, originalRight);
    renamedAliases.put(newLeftName, leftAlias);
    renamedAliases.put(newRightName, rightAlias);
    JoinCondition.OnExpression renamedCondition = JoinCondition.onExpression()
      .setExpression(condition.getExpression())
      .setDatasetAliases(renamedAliases)
      .build();

    return new JoinExpressionRequest(stageName, renamedFields, renamedLeft, renamedRight,
                                     renamedCondition, outputSchema);
  }

  public String getStageName() {
    return stageName;
  }

  public List<JoinField> getFields() {
    return fields;
  }

  public JoinCollection getLeft() {
    return left;
  }

  public JoinCollection getRight() {
    return right;
  }

  public JoinCondition.OnExpression getCondition() {
    return condition;
  }

  public Schema getOutputSchema() {
    return outputSchema;
  }
}

/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package co.cask.cdap.internal.app.queue;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.flow.FlowletConnection;
import co.cask.cdap.api.flow.FlowletDefinition;
import co.cask.cdap.app.queue.QueueSpecification;
import co.cask.cdap.app.queue.QueueSpecificationGenerator;
import co.cask.cdap.common.queue.QueueName;
import co.cask.cdap.common.utils.ImmutablePair;
import co.cask.cdap.internal.app.SchemaFinder;
import co.cask.cdap.proto.Id;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableSet;

import java.util.Map;
import java.util.Set;

/**
 * AbstractQueueSpecification builder for extracting commanality across
 * different implementation. We don't know how it would look for this yet :-)
 */
public abstract class AbstractQueueSpecificationGenerator implements QueueSpecificationGenerator {

  /**
   * Finds a equal or compatible schema connection between <code>source</code> and <code>target</code>
   * flowlet.
   */
  protected Set<QueueSpecification> generateQueueSpecification(Id.Application app,
                                                               String flow,
                                                               FlowletConnection connection,
                                                               Map<String, Set<Schema>> inputSchemas,
                                                               Map<String, Set<Schema>> outputSchemas) {

    ImmutableSet.Builder<QueueSpecification> builder = ImmutableSet.builder();

    // Iterate through all the outputs and look for an input with compatible schema.
    // If no such input is found, look for one with ANY_INPUT.
    // If there is exact match of schema then we pick that over the compatible one.
    for (Map.Entry<String, Set<Schema>> entryOutput : outputSchemas.entrySet()) {
      String outputName = entryOutput.getKey();

      Set<Schema> nameOutputSchemas = inputSchemas.get(outputName);
      ImmutablePair<Schema, Schema> schemas =
        (nameOutputSchemas == null) ? null : SchemaFinder.findSchema(entryOutput.getValue(), nameOutputSchemas);

      if (schemas == null) {
        // Try ANY_INPUT
        Set<Schema> anyInputSchemas = inputSchemas.get(FlowletDefinition.ANY_INPUT);
        if (anyInputSchemas != null) {
          schemas = SchemaFinder.findSchema(entryOutput.getValue(), anyInputSchemas);
        }
      }

      if (schemas == null) {
        continue;
      }

      if (connection.getSourceType() == FlowletConnection.Type.STREAM) {
        builder.add(createSpec(QueueName.fromStream(outputName),
                               schemas.getFirst(), schemas.getSecond()));
      } else {
        builder.add(createSpec(QueueName.fromFlowlet(app.getNamespaceId(), app.getId(), flow,
                                                     connection.getSourceName(), outputName),
                               schemas.getFirst(), schemas.getSecond()));
      }
    }

    return builder.build();
  }

  /**
   * @return An instance of {@link QueueSpecification} containing the URI for the queue
   * and the matching {@link Schema}
   */
  protected QueueSpecification createSpec(final QueueName queueName,
                                          final Schema outputSchema,
                                          final Schema inputSchema) {
    return new QueueSpecification() {
      @Override
      public QueueName getQueueName() {
        return queueName;
      }

      @Override
      public Schema getInputSchema() {
        return inputSchema;
      }

      @Override
      public Schema getOutputSchema() {
        return outputSchema;
      }

      @Override
      public int hashCode() {
        return Objects.hashCode(getQueueName(), getInputSchema(), getOutputSchema());
      }

      @Override
      public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof QueueSpecification)) {
          return false;
        }
        QueueSpecification other = (QueueSpecification) obj;
        return Objects.equal(getQueueName(), other.getQueueName())
                && Objects.equal(getInputSchema(), other.getInputSchema())
                && Objects.equal(getOutputSchema(), other.getOutputSchema());
      }

      @Override
      public String toString() {
        return queueName.toString();
      }
    };
  }
}

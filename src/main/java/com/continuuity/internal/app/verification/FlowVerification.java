package com.continuuity.internal.app.verification;

import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.flow.FlowletConnection;
import com.continuuity.api.flow.FlowletDefinition;
import com.continuuity.api.io.Schema;
import com.continuuity.app.verification.AbstractVerifier;
import com.continuuity.app.verification.Verifier;
import com.continuuity.app.verification.VerifyResult;
import com.continuuity.error.Err;

import java.util.Map;
import java.util.Set;

/**
 * This verifies a give {@link com.continuuity.api.flow.Flow}
 *
 * <p>
 *   Following are the checks that are done for a {@link com.continuuity.api.flow.Flow}
 *   <ul>
 *     <li>Verify Flow Meta Information - Name is id</li>
 *     <li>There should be atleast one or two flowlets</li>
 *     <li>Verify information for each Flowlet</li>
 *     <li>There should be atleast one connection</li>
 *     <li>Verify schema's across connections on flowlet</li>
 *   </ul>
 *
 * </p>
 */
public class FlowVerification extends AbstractVerifier implements Verifier<FlowSpecification> {

  /**
   * Verifies a single {@link FlowSpecification} for a {@link com.continuuity.api.flow.Flow}
   * defined within an {@link com.continuuity.api.Application}
   *
   * @param input to be verified
   * @return An instance of {@link VerifyResult} depending of status of verification.
   */
  @Override
  public VerifyResult verify(final FlowSpecification input) {
    String flowName = input.getName();

    // Checks if Flow name is an ID
    if(! isId(flowName)) {
      return VerifyResult.FAILURE(Err.NOT_AN_ID, "Flow");
    }

    // Check if there are no flowlets.
    if(input.getFlowlets().size() == 0) {
      return VerifyResult.FAILURE(Err.Flow.ATLEAST_ONE_FLOWLET, flowName);
    }

    // Check if there no connections.
    if(input.getConnections().size() == 0) {
      return VerifyResult.FAILURE(Err.Flow.ATLEAST_ONE_CONNECTION, flowName);
    }

    // We go through each Flowlet and verify the flowlets.
    for(Map.Entry<String, FlowletDefinition> entry : input.getFlowlets().entrySet()) {
      FlowletDefinition defn = entry.getValue();
      String flowletName = defn.getFlowletSpec().getName();

      // Check if the Flowlet Name is an ID.
      if(!isId(defn.getFlowletSpec().getName())) {
        return VerifyResult.FAILURE(Err.NOT_AN_ID, flowName + ":" + flowletName);
      }

      // We check if all the dataset names used are ids
      for(String dataSet : defn.getDatasets()) {
        if(!isId(dataSet)) {
          return VerifyResult.FAILURE(Err.NOT_AN_ID, flowName + ":" + flowletName + ":" + dataSet);
        }
      }
    }

    // We through connection and make sure the input and output are compatible.
    Map<String, FlowletDefinition> flowlets = input.getFlowlets();
    for(FlowletConnection connection : input.getConnections()) {
      String source = connection.getSourceName();
      String target = connection.getTargetName();

      // Let's start with a simple case, when the type of source is FLOWLET
      if(connection.getSourceType() == FlowletConnection.SourceType.FLOWLET) {
        VerifyResult result = VerifyFlowletConnections(flowlets.get(source), flowlets.get(target));
        // If validation have failed, then we return the status of failure and not proceed further.
        if(result.getStatus() != VerifyResult.Status.SUCCESS) {
          return result;
        }
      } else if(connection.getSourceType() == FlowletConnection.SourceType.STREAM) {
        // We know that output type is a stream, but there should be a input that can handle this.
      }
    }

    return VerifyResult.SUCCESS();
  }

  /**
   * This method verifies output schema of a flowlets is compatible with the input schema of downstream
   * flowlet.Following is how this is done
   * <p>
   *   Assume that we have a flowlet X and flowlet Y and X is connected to Y as per the definitions in
   *   connection. Following have to be true to say that flowlet X is safely connected to Y
   *   <ul>
   *     <li>For each output emitter type of X, there is atleast one input on Y that can accept and process data
   *     (including ANY)</li>
   *     <li>Schema's of the output emitter type of X that matches output type of Y have to be compatible</li>
   *     <li></li>
   *   </ul>
   * </p>
   * @param source flowlet definition
   * @param target flowlet definition
   * @return An instance of {@link VerifyResult}
   */
  private VerifyResult VerifyFlowletConnections(FlowletDefinition source, FlowletDefinition target) {
    Map<String, Set<Schema>> output = source.getOutputs();
    Map<String, Set<Schema>> input  = target.getInputs();

    // We iterate through the outputs of a source flowlet.
    for(Map.Entry<String, Set<Schema>> entrySource : output.entrySet()) {
      String outputName = entrySource.getKey();
      Set<Schema> outputSchema = entrySource.getValue();

      // Check if input connection has same as output connection.
      if(! input.containsKey(outputName)) {
        // Now, we input has defined ANY_INPUT.
        if(! input.containsKey(FlowletDefinition.ANY_INPUT)) {
          return VerifyResult.FAILURE(Err.Flow.NO_INPUT_FOR_OUTPUT, target.getFlowletSpec().getName(),
                                      source.getFlowletSpec().getName());
        }
        // Aha! ANY_INPUT exists, we check if the schema of ANY_INPUT is compatible with schema of output.
        if(! VerifyFlowletConnectionSchema(outputSchema, input.get(FlowletDefinition.ANY_INPUT))) {
          // Schema's of input ANY doesn't match the schema of output. There is no method on input that can handle the
          // Output being emitted. This is a problem.
          return VerifyResult.FAILURE(Err.Flow.NO_INPUT_FOR_OUTPUT, target.getFlowletSpec().getName(),
                                      source.getFlowletSpec().getName());
        }
      } else {
        // We know now that there is a input with the same that matches the output.
        // We need to check and make sure the schemas are compatible.
        if(! VerifyFlowletConnectionSchema(outputSchema, input.get(entrySource.getKey()))) {
          // ERROR: Input connection matches the output connection, but their schema are not
          // compatible.
          return VerifyResult.FAILURE(Err.Flow.INCOMPATIBLE_CONNECTION, target.getFlowletSpec().getName(),
                                      source.getFlowletSpec().getName());
        }
      }
    }
    return VerifyResult.SUCCESS();
  }

  /**
   * Given a outputs of a Flowlet, we compare the outputs with the inputs of a Flowlet
   * @param output {@link Schema} of a {@link com.continuuity.api.flow.flowlet.Flowlet}
   * @param input  {@link Schema} of a {@link com.continuuity.api.flow.flowlet.Flowlet}
   * @return true if they matched; false otherwise.
   * // Max 1 equal and Min 1 compatible.
   */
  private boolean VerifyFlowletConnectionSchema(Set<Schema> output, Set<Schema> input) {
    for(Schema outputSchema : output) {
      int equal = 0;
      int compatible = 0;
      for(Schema inputSchema : input) {
        if(outputSchema.equals(inputSchema)) {
          equal++;
        }
        if(outputSchema.isCompatible(inputSchema)) {
          compatible++;
        }
      }
      // There is max of one output schema that is capable of handling
      // the input.
      if(equal > 1) {
        return false;
      }

      // There is min of 1 compatible in light of none being equal to handle
      // input.
      if(equal < 1 && compatible < 1 ) {
        return false;
      }
    }
    return true;
  }
}

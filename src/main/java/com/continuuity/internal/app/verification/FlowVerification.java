package com.continuuity.internal.app.verification;

import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.flow.FlowletConnection;
import com.continuuity.api.flow.FlowletDefinition;
import com.continuuity.api.io.Schema;
import com.continuuity.app.verification.AbstractVerifier;
import com.continuuity.app.verification.Verifier;
import com.continuuity.app.verification.VerifyResult;
import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.error.Err;
import com.continuuity.internal.app.SchemaFinder;

import java.util.Map;
import java.util.Set;

/**
 * This verifies a give {@link com.continuuity.api.flow.Flow}
 * <p/>
 * <p>
 * Following are the checks that are done for a {@link com.continuuity.api.flow.Flow}
 * <ul>
 * <li>Verify Flow Meta Information - Name is id</li>
 * <li>There should be atleast one or two flowlets</li>
 * <li>Verify information for each Flowlet</li>
 * <li>There should be atleast one connection</li>
 * <li>Verify schema's across connections on flowlet</li>
 * </ul>
 * <p/>
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
    if(!isId(flowName)) {
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
      if(connection.getSourceType() == FlowletConnection.Type.FLOWLET) {
        VerifyResult result = connectionVerification(flowlets.get(source), flowlets.get(target));
        // If validation have failed, then we return the status of failure and not proceed further.
        if(result.getStatus() != VerifyResult.Status.SUCCESS) {
          return result;
        }
      } else if(connection.getSourceType() == FlowletConnection.Type.STREAM) {
        // We know that output type is a stream, but there should be a input that can handle this.
      }
    }

    return VerifyResult.SUCCESS();
  }

  /**
   * This method verifies output schema of a flowlets is compatible with the input schema of downstream
   * flowlet.Following is how this is done
   * <p>
   * Assume that we have a flowlet X and flowlet Y and X is connected to Y as per the definitions in
   * connection. Following have to be true to say that flowlet X is safely connected to Y
   * <ul>
   * <li>For each output emitter type of X, there is atleast one input on Y that can accept and process data
   * (including ANY)</li>
   * <li>Schema's of the output emitter type of X that matches output type of Y have to be compatible</li>
   * <li></li>
   * </ul>
   * </p>
   *
   * @param source flowlet definition
   * @param target flowlet definition
   * @return An instance of {@link VerifyResult}
   */
  private VerifyResult connectionVerification(FlowletDefinition source, FlowletDefinition target) {
    Map<String, Set<Schema>> output = source.getOutputs();
    Map<String, Set<Schema>> input = target.getInputs();

    boolean found = false;
    for(Map.Entry<String, Set<Schema>> entryOutput : output.entrySet()) {
      String outputName = entryOutput.getKey();

      // Check also caught in the configure during creation phase.
      // We restrict number of schema's an output can have.
      if(entryOutput.getValue().size() > 1) {
        return VerifyResult.FAILURE(Err.Flow.MORE_OUTPUT_NOT_ALLOWED, entryOutput.getKey(),
                                    source.getFlowletSpec().getName());
      }

      for(Map.Entry<String, Set<Schema>> entryInput : input.entrySet()) {
        String inputName = entryInput.getKey();

        // When the output name is same as input name - we check if their schema's
        // are same (equal or compatible)
        if(outputName.equals(inputName)) {
          found = SchemaFinder.checkSchema(entryOutput.getValue(), entryInput.getValue());
          ImmutablePair<Schema, Schema> c = SchemaFinder.findSchema(entryOutput.getValue(), entryInput.getValue());
        }

        // If not found there, we do a small optimization where we check directly if
        // the output matches the schema of ANY_INPUT schema. If it doesn't then we
        // have an issue else we are good.
        if(! found && input.containsKey(FlowletDefinition.ANY_INPUT)) {
          found = SchemaFinder.checkSchema(entryOutput.getValue(), input.get(FlowletDefinition.ANY_INPUT));
        }
        // If we found a schema that matches then we are good.
        if(found){
          return VerifyResult.SUCCESS();
        }
      }
    }
    return VerifyResult.FAILURE(
      Err.Flow.NO_INPUT_FOR_OUTPUT, target.getFlowletSpec().getName(), source.getFlowletSpec().getName());
  }
}

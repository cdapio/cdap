package com.continuuity.internal.app.verification;

import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.flow.FlowletDefinition;
import com.continuuity.app.verification.AbstractVerifier;
import com.continuuity.app.verification.Verifier;
import com.continuuity.app.verification.VerifyResult;
import com.continuuity.error.Err;

import java.util.Map;

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
    if(input.getFlowlets().size() == 0) {
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
        if(! isId(dataSet)) {
          return VerifyResult.FAILURE(Err.NOT_AN_ID, flowName + ":" + flowletName + ":" + dataSet);
        }
      }
    }

    // We iterate through connections and make sure we have right schemas for all the interconnects.

    return VerifyResult.SUCCESS();
  }
}

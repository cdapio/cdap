package com.continuuity.app.services;

import com.continuuity.common.service.Abortable;
import com.continuuity.common.service.Stoppable;

import java.util.List;

/**
 * Interface for flow service.
 */
public interface ProgramServiceHandler extends Stoppable, Abortable {
  /**
   * Starts a flow given a {@link FlowDescriptor}
   *
   * @param identifier of a flow.
   * @throws ProgramServiceException
   */
  public RunIdentifier start(FlowIdentifier identifier) throws ProgramServiceException;

  /**
   * Stops a flow given a {@link FlowIdentifier}
   *
   * @param identifier of a flow
   * @throws ProgramServiceException
   */
  public RunIdentifier stop(FlowIdentifier identifier) throws ProgramServiceException;

  /**
   * Status of a flow given a {@link FlowIdentifier}
   *
   * @param identifier of a flow.
   * @return Status of a flow along with their flowlet status.
   * @throws ProgramServiceException
   */
  public FlowStatus status(FlowIdentifier identifier) throws ProgramServiceException;

  /**
   * Sets the number of instances of a flowlet.
   *
   * @param identifier of a flow.
   * @param flowletId specifying the flowlet for which the number of instances is requested to be adjusted.
   * @param instances number of total instances a flowlet should have.
   * @throws ProgramServiceException
   */
  public void setInstances(FlowIdentifier identifier, String flowletId, int instances) throws ProgramServiceException;

  /**
   * Returns the flows associated with the account.
   *
   * @param accountId the flows are associated with.
   * @return list of flows that are active.
   */
  public List<ActiveFlow> getFlows(String accountId);

  /**
   * Retrieves the latest definition of a flow.
   *
   * @param id of a flow specified as {@link FlowIdentifier}
   * @return String representation of a flow.
   * @throws ProgramServiceException
   */
  public String getFlowDefinition(FlowIdentifier id) throws ProgramServiceException;

  /**
   * Retrieves history of flow runs for a given flow. Returns only finished runs.
   * Returned FlowRunRecords are sorted by startTime.
   *
   * @param id of a flow specified by {@link FlowIdentifier}
   * @return list of {@link FlowRunRecord}
   */
  public List<FlowRunRecord> getFlowHistory(FlowIdentifier id) throws ProgramServiceException;

  /**
   * Stops a ll flows and queries for an account
   * @param account the account id
   * @throws ProgramServiceException if something goes wrong
   */
  public void stopAll(String account) throws ProgramServiceException;

}

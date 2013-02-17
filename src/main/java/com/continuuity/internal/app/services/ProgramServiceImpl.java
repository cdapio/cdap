/*
 * Copyright (c) 2012, Continuuity Inc. All rights reserved.
 */

package com.continuuity.internal.app.services;

import com.continuuity.app.services.*;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import org.apache.thrift.TException;

import java.util.List;

/**
 * A basic implementation of the FlowService interface.
 */
public final class ProgramServiceImpl implements ProgramService.Iface {

  /**
   * Our FlowServiceHandler
   */
  private ProgramServiceHandler handlerImpl;

  /**
   * Class Constructor, where we basically store the value of
   * handlerImpl locally.
   *
   * @param handlerImpl  The instance of FLowServiceHandler to use.
   */
  @Inject
  public ProgramServiceImpl(ProgramServiceHandler handlerImpl) {

    // Check our pre conditions
    Preconditions.checkNotNull(handlerImpl);

    this.handlerImpl = handlerImpl;
  }

  /**
   * Starts a Flow
   *
   * @param token       The authentication delegation token to use.
   * @param descriptor  The FlowDescriptor instance wrapping the flow to start.
   *
   * @throws ProgramServiceException
   * @throws org.apache.thrift.TException
   */
  @Override
  public RunIdentifier start(AuthToken token, FlowDescriptor descriptor)
    throws ProgramServiceException, TException {

    // Check our pre conditions
    Preconditions.checkNotNull(token);
    Preconditions.checkNotNull(descriptor);

    // TODO: Validate AuthToken here

    return handlerImpl.start(descriptor.getIdentifier());
  }

  /**
   * Checks the status of a Flow
   *
   * @param token       The authentication delegation token to use.
   * @param identifier  The FlowApplication identifier
   *
   * @throws ProgramServiceException
   * @throws org.apache.thrift.TException
*/
  @Override
  public FlowStatus status(AuthToken token, FlowIdentifier identifier)
    throws ProgramServiceException, TException {

    // Check our pre conditions
    Preconditions.checkNotNull(token);
    Preconditions.checkNotNull(identifier);

    return handlerImpl.status(identifier);
  }

  /**
   * Stops a Flow
   *
   * @param token       The authentication delegation token to use.
   * @param identifier  The FlowApplication identifier
   *
   * @throws ProgramServiceException
   * @throws org.apache.thrift.TException
   */
  @Override
  public RunIdentifier stop(AuthToken token, FlowIdentifier identifier)
    throws ProgramServiceException, TException {

    // Check our pre conditions
    Preconditions.checkNotNull(token);
    Preconditions.checkNotNull(identifier);

    return handlerImpl.stop(identifier);
  }

  /**
   * Set number of instance of a flowlet.
   *
   * @param token       The authentication delegation token to use.
   * @param identifier  The FlowApplication identifier
   * @param flowletId   The ID of flowlet
   * @param instances   The new number of instances of the flowlet
   *
   * @throws ProgramServiceException
   * @throws org.apache.thrift.TException
   */
  @Override
  public void setInstances(AuthToken token, FlowIdentifier identifier,
                           String flowletId, short instances)
    throws ProgramServiceException, TException {

    // Check our pre conditions
    Preconditions.checkNotNull(token);
    Preconditions.checkNotNull(identifier);
    Preconditions.checkNotNull(flowletId);
    Preconditions.checkArgument(instances > 0);

    handlerImpl.setInstances(identifier, flowletId, instances);

  }

  /**
   * Retrieves flows associated with an account.
   *
   * @param accountId of a flow.
   * @return list of active flows for an account.
   * @throws ProgramServiceException
   * @throws org.apache.thrift.TException
   */
  @Override
  public List<ActiveFlow> getFlows(String accountId)
    throws ProgramServiceException, TException {
    return handlerImpl.getFlows(accountId);
  }

  /**
   * Retrieves the latest definition of a flow.
   *
   * @param id of a flow specified as {@link FlowIdentifier}
   * @return String representation of a flow.
   * @throws ProgramServiceException
   * @throws org.apache.thrift.TException
   */
  @Override
  public String getFlowDefinition(FlowIdentifier id)
    throws ProgramServiceException, TException {
    Preconditions.checkNotNull(id);
    return handlerImpl.getFlowDefinition(id);
  }


  /**
   * Retrieves history of flow runs for a given flow.
   *
   * @param id of a flow specified by {@link FlowIdentifier}
   * @return list of {@link FlowRunRecord}
   * @throws org.apache.thrift.TException
   */
  @Override
  public List<FlowRunRecord> getFlowHistory(FlowIdentifier id)
    throws ProgramServiceException, TException {
    Preconditions.checkNotNull(id);
    return handlerImpl.getFlowHistory(id);
  }

  /**
   * Stops all flows and queries for an account
   * @param accountId the account
   * @throws ProgramServiceException if something goes wrong
   * @throws org.apache.thrift.TException in case of a thrift error
   */
  @Override
  public void stopAll(String accountId)
      throws ProgramServiceException, TException {
    Preconditions.checkNotNull(accountId);
    handlerImpl.stopAll(accountId);
  }

}

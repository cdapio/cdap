package com.continuuity.common.twill;

import com.continuuity.common.conf.Constants;
import com.google.common.base.Preconditions;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillRunnerService;

import java.util.concurrent.ExecutionException;

/**
 *
 */
public abstract class AbstractDistributedReactorServiceManagement implements ReactorServiceManagement {

  protected TwillRunnerService twillRunnerService;
  protected String serviceName;

  public AbstractDistributedReactorServiceManagement(String serviceName, TwillRunnerService twillRunnerService) {
    this.serviceName = serviceName;
    this.twillRunnerService = twillRunnerService;
  }

  @Override
  public int getInstanceCount() {
    Iterable<TwillController> twillControllerList = twillRunnerService.lookup(Constants.Service.REACTOR_SERVICES);
    int instances = 0;
    if (twillControllerList != null) {
      for (TwillController twillController : twillControllerList) {
        instances = twillController.getResourceReport().getRunnableResources(serviceName).size();
      }
    }
    return instances;
  }

  @Override
  public boolean setInstanceCount(int instanceCount) {
    Preconditions.checkArgument(instanceCount > 0);
    try {
      Iterable<TwillController> twillControllerList = twillRunnerService.lookup(Constants.Service.REACTOR_SERVICES);
      if (twillControllerList != null) {
        for (TwillController twillController : twillControllerList) {
          twillController.changeInstances(serviceName, instanceCount).get();
        }
      }
      return true;
    } catch (InterruptedException e) {
      e.printStackTrace();
      return false;
    } catch (ExecutionException e) {
      e.printStackTrace();
      return false;
    }
  }
}

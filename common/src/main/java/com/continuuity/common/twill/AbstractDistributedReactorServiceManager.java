package com.continuuity.common.twill;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.ning.http.client.Response;
import com.ning.http.client.SimpleAsyncHttpClient;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillRunnerService;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Abstract class that can be extended by individual Reactor Services to implement their management methods.
 */
public abstract class AbstractDistributedReactorServiceManager implements ReactorServiceManager {
  private static final long SERVICE_PING_RESPONSE_TIMEOUT = TimeUnit.MILLISECONDS.convert(5, TimeUnit.SECONDS);
  protected static final long DISCOVERY_TIMEOUT_SECONDS = 3;

  protected CConfiguration cConf;
  protected TwillRunnerService twillRunnerService;
  protected String serviceName;

  public AbstractDistributedReactorServiceManager(CConfiguration cConf, String serviceName,
                                                  TwillRunnerService twillRunnerService) {
    this.cConf = cConf;
    this.serviceName = serviceName;
    this.twillRunnerService = twillRunnerService;
  }

  @Override
  public int getInstances() {
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
  public boolean setInstances(int instanceCount) {
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

  @Override
  public int getMinInstances() {
    return 1;
  }

  @Override
  public boolean canCheckStatus() {
    return true;
  }

  @Override
  public boolean isLogAvailable() {
    return true;
  }

  protected HttpResponseStatus checkGetStatus(String url) throws Exception {
    SimpleAsyncHttpClient client = new SimpleAsyncHttpClient.Builder()
      .setUrl(url)
      .setRequestTimeoutInMs((int) SERVICE_PING_RESPONSE_TIMEOUT)
      .build();

    try {
      Future<Response> future = client.get();
      Response response = future.get(SERVICE_PING_RESPONSE_TIMEOUT, TimeUnit.MILLISECONDS);
      return HttpResponseStatus.valueOf(response.getStatusCode());
    } catch (Exception e) {
      Throwables.propagate(e);
    } finally {
      client.close();
    }
    return HttpResponseStatus.NOT_FOUND;
  }

}

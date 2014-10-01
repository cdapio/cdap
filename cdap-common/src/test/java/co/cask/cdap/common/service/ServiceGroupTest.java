package co.cask.cdap.common.service;

import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Service;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

/**
 * Test for {@link ServiceGroup}.
 */
public class ServiceGroupTest {

  private static final Logger LOG = LoggerFactory.getLogger(ServiceGroupTest.class);

  @Test
  public void test() {
    FakeTransactionService txService = new FakeTransactionService();
    FakeDatasetService datasetService = new FakeDatasetService();
    FakeMetricsQueryService metricsQueryService = new FakeMetricsQueryService();
    FakeMetricsCollectionService metricsCollectionService = new FakeMetricsCollectionService();

    ServiceGroup serviceGroup = new ServiceGroup(ImmutableSet.<ManagedService>of(
      txService, datasetService, metricsQueryService, metricsCollectionService));

    Assert.assertEquals(3, serviceGroup.getPhases().size());
    Assert.assertEquals(1, serviceGroup.getPhases().get(0).getServices().size());
    Assert.assertEquals(txService, serviceGroup.getPhases().get(0).getServices().get(0));

    Assert.assertEquals(1, serviceGroup.getPhases().get(1).getServices().size());
    Assert.assertEquals(datasetService, serviceGroup.getPhases().get(1).getServices().get(0));

    Assert.assertEquals(2, serviceGroup.getPhases().get(2).getServices().size());
    Assert.assertTrue(serviceGroup.getPhases().get(2).getServices().contains(metricsQueryService));
    Assert.assertTrue(serviceGroup.getPhases().get(2).getServices().contains(metricsCollectionService));

    serviceGroup.startAndWait();
    Assert.assertEquals(Service.State.RUNNING, txService.state());
    Assert.assertEquals(Service.State.RUNNING, datasetService.state());
    Assert.assertEquals(Service.State.RUNNING, metricsQueryService.state());
    Assert.assertEquals(Service.State.RUNNING, metricsCollectionService.state());

    serviceGroup.stopAndWait();
    Assert.assertEquals(Service.State.TERMINATED, txService.state());
    Assert.assertEquals(Service.State.TERMINATED, datasetService.state());
    Assert.assertEquals(Service.State.TERMINATED, metricsQueryService.state());
    Assert.assertEquals(Service.State.TERMINATED, metricsCollectionService.state());
  }

  public static final class FakeTransactionService extends FakeService implements ManagedService {

    public static final ServiceId ID = new ServiceId("tx");

    @Override
    public ServiceId getServiceId() {
      return ID;
    }

    @Override
    public Set<ServiceId> getServiceDependencies() {
      return ImmutableSet.of();
    }
  }

  public static final class FakeDatasetService extends FakeService implements ManagedService {

    public static final ServiceId ID = new ServiceId("dataset");

    @Override
    public ServiceId getServiceId() {
      return ID;
    }

    @Override
    public Set<ServiceId> getServiceDependencies() {
      return ImmutableSet.of(FakeTransactionService.ID);
    }
  }

  public static final class FakeMetricsQueryService extends FakeService implements ManagedService {

    public static final ServiceId ID = new ServiceId("metrics.query");

    @Override
    public ServiceId getServiceId() {
      return ID;
    }

    @Override
    public Set<ServiceId> getServiceDependencies() {
      return ImmutableSet.of(FakeTransactionService.ID, FakeDatasetService.ID);
    }
  }

  public static final class FakeMetricsCollectionService extends FakeService implements ManagedService {

    public static final ServiceId ID = new ServiceId("metrics.collect");

    @Override
    public ServiceId getServiceId() {
      return ID;
    }

    @Override
    public Set<ServiceId> getServiceDependencies() {
      return ImmutableSet.of(FakeTransactionService.ID, FakeDatasetService.ID);
    }
  }

  private static class FakeService extends AbstractIdleService {

    @Override
    protected void startUp() throws Exception {
      LOG.info("Starting {}", this.getClass().getSimpleName());
      Thread.sleep(1500);
      LOG.info("Started {}", this.getClass().getSimpleName());
    }

    @Override
    protected void shutDown() throws Exception {
      LOG.info("Stopping {}", this.getClass().getSimpleName());
      Thread.sleep(1500);
      LOG.info("Stopped {}", this.getClass().getSimpleName());
    }

  }

}

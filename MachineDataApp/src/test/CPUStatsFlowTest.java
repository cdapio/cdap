


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.continuuity.machinedata.CPUStatsWriterFlowlet;
import com.continuuity.machinedata.MachineDataApp;
import com.continuuity.machinedata.data.CPUStat;
import com.continuuity.machinedata.data.CPUStatsTable;
import com.continuuity.test.ApplicationManager;
import com.continuuity.test.RuntimeMetrics;
import com.continuuity.test.RuntimeStats;
import com.continuuity.test.StreamWriter;
import org.junit.Before;
import com.continuuity.test.AppFabricTestBase;
import org.junit.Test;
import com.continuuity.machinedata.CPUStatsFlow;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.TimeUnit;


/**
 *
 */
public class CPUStatsFlowTest extends AppFabricTestBase {

  @Test(timeout = 2000000)
  public void testCPUStatsFlow() throws Exception {

    // Clear first...
    this.clearAppFabric();

    ApplicationManager applicationManager = deployApplication(MachineDataApp.class);


    applicationManager.startFlow(CPUStatsFlow.NAME);

    StreamWriter s1 = applicationManager.getStreamWriter(MachineDataApp.CPU_STATS_STREAM);

    long ts_from = System.currentTimeMillis() - 1000 * 60 * 60; // 1 hour ago

    Random rand = new Random();
    int min = 0 , max = 100;
    int cpu = 0;
    String hostname = "hostname";

    int numMetrics = 10;

    // Write n number of metrics
    for (int i = 0; i < numMetrics; i++) {
      cpu = rand.nextInt(max - min + 1) + min;
      this.writeMetric(s1, System.currentTimeMillis(), cpu, hostname);
    }

    // Wait for all tuples to be processed.
    RuntimeMetrics m1 =
      RuntimeStats.getFlowletMetrics(MachineDataApp.NAME, CPUStatsFlow.NAME, CPUStatsWriterFlowlet.NAME);
    System.out.println("Waiting for parsing flowlet to process tuple");
    m1.waitForProcessed(numMetrics, 5, TimeUnit.SECONDS);

    // Read values back from Dataset
    CPUStatsTable cpuStatsTable = (CPUStatsTable)applicationManager.getDataSet(MachineDataApp.CPU_STATS_TABLE);

    CPUStat stat = new CPUStat(System.currentTimeMillis(), 10, "hostname");

    cpuStatsTable.write(stat);


    ArrayList<CPUStat> cpuStats =  cpuStatsTable.read(hostname, ts_from, System.currentTimeMillis() + 1000 * 60 * 60);

    assertTrue(cpuStats.size() == numMetrics);
  }

  public void writeMetric(StreamWriter stream, long ts, int cpu, String hostname) {
    String metric = Long.toString(ts) + ", " + Integer.toString(cpu) + ", " + hostname;

    try {
    stream.send(metric);
    } catch (IOException ioe) {
      System.out.println(ioe.getLocalizedMessage());
    }
  }
}

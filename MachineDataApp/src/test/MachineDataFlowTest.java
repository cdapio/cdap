import static org.junit.Assert.assertTrue;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.dataset.SimpleTimeseriesTable;
import com.continuuity.api.data.dataset.TimeseriesTable;
import com.continuuity.machinedata.CPUStatsFlowlet;
import com.continuuity.machinedata.MachineDataApp;
import com.continuuity.machinedata.MachineDataFlow;
import com.continuuity.test.ApplicationManager;
import com.continuuity.test.ProcedureManager;
import com.continuuity.test.RuntimeMetrics;
import com.continuuity.test.RuntimeStats;
import com.continuuity.test.StreamWriter;
import com.continuuity.test.AppFabricTestBase;
import org.junit.Test;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;


/**
 *
 */
public class MachineDataFlowTest extends AppFabricTestBase {

  @Test(timeout = 2000000)
  public void testCPUStatsFlow() throws Exception {

    // Clear first...
    this.clearAppFabric();

    ApplicationManager applicationManager = deployApplication(MachineDataApp.class);


    applicationManager.startFlow(MachineDataFlow.NAME);

    StreamWriter cpuStream = applicationManager.getStreamWriter(MachineDataApp.CPU_STATS_STREAM);
    StreamWriter memoryStream = applicationManager.getStreamWriter(MachineDataApp.MEM_STATS_STREAM);
    StreamWriter diskStream = applicationManager.getStreamWriter(MachineDataApp.DISK_STATS_STREAM);

    long ts_from = System.currentTimeMillis() - 1000 * 60 * 60; // 1 hour ago

    Random rand = new Random();
    int min = 0 , max = 100;
    long cpu = 0;
    long memory = 0;
    long disk = 0;
    String hostname = "hostname";

    int numMetrics = 10;

    // Write n number of cpu metrics
    for (int i = 0; i < numMetrics; i++) {
      Thread.sleep(100);
      cpu = rand.nextInt(max - min + 1) + min;   // 0 - 100
      this.writeMetric(cpuStream, System.currentTimeMillis(), cpu, hostname);
    }

    // Write n number of memory metrics
    for (int i = 0; i < numMetrics; i++) {
      Thread.sleep(100);
      memory = Math.abs(rand.nextLong()%400000000);
      this.writeMetric(memoryStream, System.currentTimeMillis(), memory, hostname);
    }

    // Write n number of disk  metrics
    for (int i = 0; i < numMetrics; i++) {
      Thread.sleep(100);
      disk = Math.abs(rand.nextLong()%1000000000);
      this.writeMetric(diskStream, System.currentTimeMillis(), disk, hostname);
    }

    Thread.sleep(3000);

    // Wait for all tuples to be processed.
    RuntimeMetrics m1 =
      RuntimeStats.getFlowletMetrics(MachineDataApp.NAME, MachineDataFlow.NAME, CPUStatsFlowlet.NAME);
    System.out.println("Waiting on: " + MachineDataApp.CPU_STATS_STREAM);
    //m1.waitForProcessed(numMetrics, 10, TimeUnit.SECONDS); //TODO: Check with Terence, wait doesn't work synchronously

    // Read values back from Dataset
    SimpleTimeseriesTable cpuStatsTable = (SimpleTimeseriesTable)applicationManager.getDataSet(MachineDataApp.MACHINE_STATS_TABLE);

    // Read entries for the last hours
    List<TimeseriesTable.Entry> entries = cpuStatsTable.read(Bytes.toBytes("cpu"), ts_from, System.currentTimeMillis() + 1000 * 60 * 60, Bytes.toBytes(hostname));
    assertTrue(entries.size() == numMetrics);
    entries.clear();

    entries = cpuStatsTable.read(Bytes.toBytes("memory"), ts_from, System.currentTimeMillis() + 1000 * 60 * 60, Bytes.toBytes(hostname));
    assertTrue(entries.size() == numMetrics);
    entries.clear();

    entries = cpuStatsTable.read(Bytes.toBytes("disk"), ts_from, System.currentTimeMillis() + 1000 * 60 * 60, Bytes.toBytes(hostname));
    assertTrue(entries.size() == numMetrics);
    entries.clear();



    // test Procedure
    ProcedureManager procedureManager = applicationManager.startProcedure("MachineDataProcedure");

    HashMap<String, String> argEcho = new HashMap<String, String>();
    String queryEcho = procedureManager.getClient().query("echo", argEcho);

    // getRange type = <cpu, memory, disk>
    HashMap<String, String> args = new HashMap<String, String>();
    args.put("type", "cpu");
    args.put("hostname", "hostname");
    args.put("timestamp_from", String.valueOf(ts_from));
    args.put("timestamp_to", String.valueOf(System.currentTimeMillis() + 1000 * 60 * 60));
    String  query = procedureManager.getClient().query("getRange", args);
    assertTrue(!query.isEmpty());
    query = "";

    // getLastHour: cpu
    args.clear();
    args.put("type", "cpu");
    args.put("hostname", hostname);
    query = procedureManager.getClient().query("getLastHour", args);
    assertTrue(!query.isEmpty());

    args.clear();
    args.put("type", "memory");
    args.put("hostname", hostname);
    query = procedureManager.getClient().query("getLastHour", args);
    assertTrue(!query.isEmpty());

    args.clear();
    args.put("type", "disk");
    args.put("hostname", hostname);
    query = procedureManager.getClient().query("getLastHour", args);
    assertTrue(!query.isEmpty());
  }

  public void writeMetric(StreamWriter stream, long ts, long value, String hostname) {
    String metric = Long.toString(ts) + ", " + Long.toString(value) + ", " + hostname;
    try {
    stream.send(metric);
    } catch (IOException ioe) {
      System.out.println(ioe.getLocalizedMessage());
    }
  }
}

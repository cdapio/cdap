package io.cdap.cdap.app.runtime.spark.submit;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import io.cdap.cdap.app.runtime.spark.SparkRuntimeContext;
import io.cdap.cdap.runtime.spi.runtimejob.LaunchMode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.twill.filesystem.LocationFactory;
import org.jetbrains.annotations.Nullable;
import io.cdap.cdap.internal.app.runtime.distributed.LocalizeResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ServerlessDataprocSubmitter extends DistributedSparkSubmitter {

  private static final Logger LOG = LoggerFactory.getLogger(ServerlessDataprocSubmitter.class);

  private static final Function<LocalizeResource, String> RESOURCE_TO_PATH = input ->
    input.getURI().toString().split("#")[0];
  private static final Pattern LOCAL_MASTER_PATTERN = Pattern.compile("local\\[([0-9]+|\\*)\\]");


  public ServerlessDataprocSubmitter(Configuration hConf, LocationFactory locationFactory,
                                     String hostname, SparkRuntimeContext runtimeContext,
                                     @Nullable String schedulerQueueName, LaunchMode launchMode) {
    super(hConf, locationFactory, hostname, runtimeContext, schedulerQueueName, launchMode);
  }

  @Override
  protected void addMaster(Map<String, String> configs, ImmutableList.Builder<String> argBuilder) {
    // Use at least two threads for Spark Streaming
    String masterArg = "local[2]";

    String master = configs.get("spark.master");
    if (master != null) {
      Matcher matcher = LOCAL_MASTER_PATTERN.matcher(master);
      if (matcher.matches()) {
        masterArg = "local[" + matcher.group(1) + "]";
      }
    }
    argBuilder.add("--master").add(masterArg);
  }

  @Override
  protected Map<String, String> generateSubmitConf(Map<String, String> appConf) {
    Map<String, String> config = new HashMap<>();
    config.put("spark.executorEnv.CDAP_LOG_DIR", ApplicationConstants.LOG_DIR_EXPANSION_VAR);
    // TODO : Error : for distributed spark : $destFile exists and does not match contents
    config.put("spark.files","");
    config.put("spark.jars","");
    config.put("spark.repl.local.jars","");
    // TODO : Error : DataprocMetricsListener is not a subclass of org.apache.spark.scheduler.SparkListenerInterface
    config.put("spark.dataproc.listeners","");

    // Make Spark UI runs on random port. By default, Spark UI runs on port 4040 and it will do a sequential search
    // of the next port if 4040 is already occupied. However, during the process, it unnecessarily logs big stacktrace
    // as WARN, which pollute the logs a lot if there are concurrent Spark job running (e.g. a fork in Workflow).
    config.put("spark.ui.port", "0");

    return config;
  }

  @Override
  protected Function<LocalizeResource, String> getLocalizeResourceToURIFunc() {
    return RESOURCE_TO_PATH;
  }

}

/*
 * Copyright © 2018 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.app.runtime.spark;

import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.api.spark.Spark;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.program.ProgramDescriptor;
import co.cask.cdap.app.program.Programs;
import co.cask.cdap.app.runtime.Arguments;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.lang.InstantiatorFactory;
import co.cask.cdap.common.lang.jar.BundleJarUtil;
import co.cask.cdap.common.utils.DirUtils;
import co.cask.cdap.data.stream.StreamCoordinatorClient;
import co.cask.cdap.internal.app.ApplicationSpecificationAdapter;
import co.cask.cdap.internal.app.runtime.AbstractListener;
import co.cask.cdap.internal.app.runtime.BasicArguments;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.SimpleProgramOptions;
import co.cask.cdap.internal.app.runtime.batch.MapReduceProgramRunner;
import co.cask.cdap.internal.app.runtime.batch.dataproc.DistributedProgramRunnableModule;
import co.cask.cdap.internal.app.runtime.codec.ArgumentsCodec;
import co.cask.cdap.internal.app.runtime.codec.ProgramOptionsCodec;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.ProgramId;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.io.Files;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.Service;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.common.Threads;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.internal.Services;
import org.apache.twill.kafka.client.BrokerService;
import org.apache.twill.kafka.client.KafkaClientService;
import org.apache.twill.zookeeper.ZKClientService;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 *
 */
public class DSPRMain {
    private static final Gson GSON = ApplicationSpecificationAdapter.addTypeAdapters(new GsonBuilder())
            .registerTypeAdapter(Arguments.class, new ArgumentsCodec())
            .registerTypeAdapter(ProgramOptions.class, new ProgramOptionsCodec())
            .create();


    private static ProgramId programId = new ProgramId("default", "appname", ProgramType.SPARK,
            "phase-1");
    private static final String runId = RunIds.generate().getId();

    private static final String appSpecString = "{\"name\":\"FileSourceText\",\"appVersion\":\"-SNAPSHOT\",\"descripti"
            + "on\":\"Data Pipeline Application\",\"configuration\":\"{\\\"engine\\\""
            + ":\\\"SPARK\\\",\\\"schedule\\\":\\\"* * * * *\\\",\\\"postActions\\\":[],\\\"s"
            + "tages\\\":[{\\\"name\\\":\\\"sink\\\",\\\"plugin\\\":{\\\"name\\\":\\\"HDFS\\\",\\\""
            + "type\\\":\\\"batchsink\\\",\\\"properties\\\":{\\\"path\\\":\\\"/tmp/hdfs-si"
            + "nk-output-dir\\\",\\\"referenceName\\\":\\\"HDFSinkTest\\\",\\\"delimite"
            + "r\\\":\\\"|\\\"}}},{\\\"name\\\":\\\"source\\\",\\\"plugin\\\":{\\\"name\\\":\\\"Fil"
            + "e\\\",\\\"type\\\":\\\"batchsource\\\",\\\"properties\\\":{\\\"referenceName"
            + "\\\":\\\"TestFile\\\",\\\"fileSystem\\\":\\\"Text\\\",\\\"path\\\":\\\"/tmp/input.txt"
            + "\\\",\\\"format\\\":\\\""
            + "text\\\",\\\"ignoreNonExistingFolders\\\":\\\"false\\\",\\\"pathField\\\":"
            + "\\\"file\\\",\\\"schema\\\":\\\"{\\\\\\\"type\\\\\\\":\\\\\\\"record\\\\\\\",\\\\\\\"name\\"
            + "\\\\\":\\\\\\\"file.record\\\\\\\",\\\\\\\"fields\\\\\\\":[{\\\\\\\"name\\\\\\\":\\\\\\\"of"
            + "fset\\\\\\\",\\\\\\\"type\\\\\\\":\\\\\\\"long\\\\\\\"},{\\\\\\\"name\\\\\\\":\\\\\\\"body\\\\"
            + "\\\",\\\\\\\"type\\\\\\\":[\\\\\\\"string\\\\\\\",\\\\\\\"null\\\\\\\"]},{\\\\\\\"name\\\\\\\""
            + ":\\\\\\\"file\\\\\\\",\\\\\\\"type\\\\\\\":[\\\\\\\"string\\\\\\\",\\\\\\\"null\\\\\\\"]}]}\\"
            + "\"}}}],\\\"connections\\\":[{\\\"from\\\":\\\"source\\\",\\\"to\\\":\\\"sink\\\"}"
            + "],\\\"resources\\\":{\\\"virtualCores\\\":1.0,\\\"memoryMB\\\":1024.0},\\"
            + "\"driverResources\\\":{\\\"virtualCores\\\":1.0,\\\"memoryMB\\\":1024.0"
            + "},\\\"clientResources\\\":{\\\"virtualCores\\\":1.0,\\\"memoryMB\\\":102"
            + "4.0},\\\"stageLoggingEnabled\\\":true,\\\"processTimingEnabled\\\":t"
            + "rue,\\\"numOfRecordsPreview\\\":0.0,\\\"properties\\\":{},\\\"sinks\\\":"
            + "[],\\\"transforms\\\":[]}\",\"artifactId\":{\"name\":\"data-pipeline\","
            + "\"version\":{\"version\":\"3.4.0-SNAPSHOT\",\"major\":3,\"minor\":4,\"f"
            + "ix\":0,\"suffix\":\"SNAPSHOT\"},\"scope\":\"USER\"},\"streams\":{},\"dat"
            + "asetModules\":{},\"datasetInstances\":{},\"flows\":{},\"mapReduces"
            + "\":{},\"sparks\":{\"phase-1\":{\"className\":\"co.cask.cdap.etl.spar"
            + "k.batch.ETLSpark\",\"name\":\"phase-1\",\"description\":\"Sources \\u"
            + "0027source\\u0027 to sinks \\u0027sink\\u0027.\",\"mainClassName\""
            + ":\"co.cask.cdap.etl.spark.batch.BatchSparkPipelineDriver\",\"da"
            + "tasets\":[],\"properties\":{\"pipeline\":\"{\\\"phaseName\\\":\\\"phase-"
            + "1\\\",\\\"phase\\\":{\\\"stagesByType\\\":{\\\"batchsource\\\":[{\\\"name\\\":"
            + "\\\"source\\\",\\\"plugin\\\":{\\\"type\\\":\\\"batchsource\\\",\\\"name\\\":\\\"F"
            + "ile\\\",\\\"properties\\\":{\\\"referenceName\\\":\\\"TestFile\\\",\\\"fileS"
            + "ystem\\\":\\\"Text\\\",\\\"path\\\":\\\"/tmp/input.txt"
            + "\\\",\\\"format\\\":\\\"text\\\",\\\"ignoreNonExist"
            + "ingFolders\\\":\\\"false\\\",\\\"pathField\\\":\\\"file\\\",\\\"schema\\\":\\\"{"
            + "\\\\\\\"type\\\\\\\":\\\\\\\"record\\\\\\\",\\\\\\\"name\\\\\\\":\\\\\\\"file.record\\\\\\\""
            + ",\\\\\\\"fields\\\\\\\":[{\\\\\\\"name\\\\\\\":\\\\\\\"offset\\\\\\\",\\\\\\\"type\\\\\\\":\\"
            + "\\\\\"long\\\\\\\"},{\\\\\\\"name\\\\\\\":\\\\\\\"body\\\\\\\",\\\\\\\"type\\\\\\\":[\\\\\\\"st"
            + "ring\\\\\\\",\\\\\\\"null\\\\\\\"]},{\\\\\\\"name\\\\\\\":\\\\\\\"file\\\\\\\",\\\\\\\"type\\"
            + "\\\\\":[\\\\\\\"string\\\\\\\",\\\\\\\"null\\\\\\\"]}]}\\\"},\\\"artifact\\\":{\\\"name"
            + "\\\":\\\"core-plugins\\\",\\\"version\\\":{\\\"version\\\":\\\"4.0.0\\\",\\\"maj"
            + "or\\\":4,\\\"minor\\\":0,\\\"fix\\\":0},\\\"scope\\\":\\\"USER\\\"}},\\\"inputSc"
            + "hemas\\\":{},\\\"outputPorts\\\":{\\\"sink\\\":{\\\"schema\\\":{\\\"type\\\":\\"
            + "\"record\\\",\\\"name\\\":\\\"file.record\\\",\\\"fields\\\":[{\\\"name\\\":\\\"o"
            + "ffset\\\",\\\"type\\\":\\\"long\\\"},{\\\"name\\\":\\\"body\\\",\\\"type\\\":[\\\"st"
            + "ring\\\",\\\"null\\\"]},{\\\"name\\\":\\\"file\\\",\\\"type\\\":[\\\"string\\\",\\\""
            + "null\\\"]}]}}},\\\"outputSchema\\\":{\\\"type\\\":\\\"record\\\",\\\"name\\\":"
            + "\\\"file.record\\\",\\\"fields\\\":[{\\\"name\\\":\\\"offset\\\",\\\"type\\\":\\\""
            + "long\\\"},{\\\"name\\\":\\\"body\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{"
            + "\\\"name\\\":\\\"file\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]}]},\\\"stageL"
            + "oggingEnabled\\\":true,\\\"processTimingEnabled\\\":true,\\\"inputs\\"
            + "\":[],\\\"outputs\\\":[\\\"sink\\\"]}],\\\"batchsink\\\":[{\\\"name\\\":\\\"sin"
            + "k\\\",\\\"plugin\\\":{\\\"type\\\":\\\"batchsink\\\",\\\"name\\\":\\\"HDFS\\\",\\\"p"
            + "roperties\\\":{\\\"path\\\":\\\"/tmp/hdfs-sink-output-dir\\\",\\\"refere"
            + "nceName\\\":\\\"HDFSinkTest\\\",\\\"delimiter\\\":\\\"|\\\"},\\\"artifact\\\":"
            + "{\\\"name\\\":\\\"hdfs-plugins\\\",\\\"version\\\":{\\\"version\\\":\\\"1.0.0\\"
            + "\",\\\"major\\\":1,\\\"minor\\\":0,\\\"fix\\\":0},\\\"scope\\\":\\\"USER\\\"}},\\\""
            + "inputSchemas\\\":{\\\"source\\\":{\\\"type\\\":\\\"record\\\",\\\"name\\\":\\\"f"
            + "ile.record\\\",\\\"fields\\\":[{\\\"name\\\":\\\"offset\\\",\\\"type\\\":\\\"lon"
            + "g\\\"},{\\\"name\\\":\\\"body\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"n"
            + "ame\\\":\\\"file\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]}]}},\\\"outputPo"
            + "rts\\\":{},\\\"errorSchema\\\":{\\\"type\\\":\\\"record\\\",\\\"name\\\":\\\"fil"
            + "e.record\\\",\\\"fields\\\":[{\\\"name\\\":\\\"offset\\\",\\\"type\\\":\\\"long\\"
            + "\"},{\\\"name\\\":\\\"body\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"nam"
            + "e\\\":\\\"file\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]}]},\\\"stageLoggin"
            + "gEnabled\\\":true,\\\"processTimingEnabled\\\":true,\\\"inputs\\\":[\\\""
            + "source\\\"],\\\"outputs\\\":[]}]},\\\"stagesByName\\\":{\\\"sink\\\":{\\\"na"
            + "me\\\":\\\"sink\\\",\\\"plugin\\\":{\\\"type\\\":\\\"batchsink\\\",\\\"name\\\":\\\""
            + "HDFS\\\",\\\"properties\\\":{\\\"path\\\":\\\"/tmp/hdfs-sink-output-dir\\"
            + "\",\\\"referenceName\\\":\\\"HDFSinkTest\\\",\\\"delimiter\\\":\\\"|\\\"},\\\"a"
            + "rtifact\\\":{\\\"name\\\":\\\"hdfs-plugins\\\",\\\"version\\\":{\\\"version\\"
            + "\":\\\"1.0.0\\\",\\\"major\\\":1,\\\"minor\\\":0,\\\"fix\\\":0},\\\"scope\\\":\\\"U"
            + "SER\\\"}},\\\"inputSchemas\\\":{\\\"source\\\":{\\\"type\\\":\\\"record\\\",\\\""
            + "name\\\":\\\"file.record\\\",\\\"fields\\\":[{\\\"name\\\":\\\"offset\\\",\\\"ty"
            + "pe\\\":\\\"long\\\"},{\\\"name\\\":\\\"body\\\",\\\"type\\\":[\\\"string\\\",\\\"nul"
            + "l\\\"]},{\\\"name\\\":\\\"file\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]}]}},"
            + "\\\"outputPorts\\\":{},\\\"errorSchema\\\":{\\\"type\\\":\\\"record\\\",\\\"na"
            + "me\\\":\\\"file.record\\\",\\\"fields\\\":[{\\\"name\\\":\\\"offset\\\",\\\"type"
            + "\\\":\\\"long\\\"},{\\\"name\\\":\\\"body\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\"
            + "\"]},{\\\"name\\\":\\\"file\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]}]},\\\"s"
            + "tageLoggingEnabled\\\":true,\\\"processTimingEnabled\\\":true,\\\"in"
            + "puts\\\":[\\\"source\\\"],\\\"outputs\\\":[]},\\\"source\\\":{\\\"name\\\":\\\"s"
            + "ource\\\",\\\"plugin\\\":{\\\"type\\\":\\\"batchsource\\\",\\\"name\\\":\\\"File"
            + "\\\",\\\"properties\\\":{\\\"referenceName\\\":\\\"TestFile\\\",\\\"fileSyst"
            + "em\\\":\\\"Text\\\",\\\"path\\\":\\\"/tmp/input.txt"
            + "\\\",\\\"format\\\":\\\"text\\\",\\\"ignoreNonExisting"
            + "Folders\\\":\\\"false\\\",\\\"pathField\\\":\\\"file\\\",\\\"schema\\\":\\\"{\\\\\\"
            + "\"type\\\\\\\":\\\\\\\"record\\\\\\\",\\\\\\\"name\\\\\\\":\\\\\\\"file.record\\\\\\\",\\\\"
            + "\\\"fields\\\\\\\":[{\\\\\\\"name\\\\\\\":\\\\\\\"offset\\\\\\\",\\\\\\\"type\\\\\\\":\\\\\\\""
            + "long\\\\\\\"},{\\\\\\\"name\\\\\\\":\\\\\\\"body\\\\\\\",\\\\\\\"type\\\\\\\":[\\\\\\\"strin"
            + "g\\\\\\\",\\\\\\\"null\\\\\\\"]},{\\\\\\\"name\\\\\\\":\\\\\\\"file\\\\\\\",\\\\\\\"type\\\\\\\""
            + ":[\\\\\\\"string\\\\\\\",\\\\\\\"null\\\\\\\"]}]}\\\"},\\\"artifact\\\":{\\\"name\\\":"
            + "\\\"core-plugins\\\",\\\"version\\\":{\\\"version\\\":\\\"4.0.0\\\",\\\"major\\"
            + "\":4,\\\"minor\\\":0,\\\"fix\\\":0},\\\"scope\\\":\\\"USER\\\"}},\\\"inputSchem"
            + "as\\\":{},\\\"outputPorts\\\":{\\\"sink\\\":{\\\"schema\\\":{\\\"type\\\":\\\"re"
            + "cord\\\",\\\"name\\\":\\\"file.record\\\",\\\"fields\\\":[{\\\"name\\\":\\\"offs"
            + "et\\\",\\\"type\\\":\\\"long\\\"},{\\\"name\\\":\\\"body\\\",\\\"type\\\":[\\\"strin"
            + "g\\\",\\\"null\\\"]},{\\\"name\\\":\\\"file\\\",\\\"type\\\":[\\\"string\\\",\\\"nul"
            + "l\\\"]}]}}},\\\"outputSchema\\\":{\\\"type\\\":\\\"record\\\",\\\"name\\\":\\\"f"
            + "ile.record\\\",\\\"fields\\\":[{\\\"name\\\":\\\"offset\\\",\\\"type\\\":\\\"lon"
            + "g\\\"},{\\\"name\\\":\\\"body\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"n"
            + "ame\\\":\\\"file\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]}]},\\\"stageLogg"
            + "ingEnabled\\\":true,\\\"processTimingEnabled\\\":true,\\\"inputs\\\":["
            + "],\\\"outputs\\\":[\\\"sink\\\"]}},\\\"dag\\\":{\\\"nodes\\\":[\\\"sink\\\",\\\"so"
            + "urce\\\"],\\\"sources\\\":[\\\"source\\\"],\\\"sinks\\\":[\\\"sink\\\"],\\\"outg"
            + "oingConnections\\\":{\\\"map\\\":{\\\"source\\\":[\\\"sink\\\"]}},\\\"incomi"
            + "ngConnections\\\":{\\\"map\\\":{\\\"sink\\\":[\\\"source\\\"]}}}},\\\"resour"
            + "ces\\\":{\\\"virtualCores\\\":1,\\\"memoryMB\\\":1024},\\\"driverResourc"
            + "es\\\":{\\\"virtualCores\\\":1,\\\"memoryMB\\\":1024},\\\"clientResource"
            + "s\\\":{\\\"virtualCores\\\":1,\\\"memoryMB\\\":1024},\\\"isStageLoggingE"
            + "nabled\\\":true,\\\"isProcessTimingEnabled\\\":true,\\\"connectorDat"
            + "asets\\\":{},\\\"pipelineProperties\\\":{},\\\"description\\\":\\\"Sourc"
            + "es \\\\u0027source\\\\u0027 to sinks \\\\u0027sink\\\\u0027.\\\",\\\"num"
            + "OfRecordsPreview\\\":0,\\\"isPipelineContainsCondition\\\":false}\""
            + "},\"clientResources\":{\"virtualCores\":1,\"memoryMB\":1024},\"driv"
            + "erResources\":{\"virtualCores\":1,\"memoryMB\":1024},\"executorRes"
            + "ources\":{\"virtualCores\":1,\"memoryMB\":1024},\"handlers\":[]}},\""
            + "workflows\":{\"DataPipelineWorkflow\":{\"className\":\"co.cask.cda"
            + "p.datapipeline.SmartWorkflow\",\"name\":\"DataPipelineWorkflow\","
            + "\"description\":\"Data Pipeline Workflow\",\"properties\":{\"pipeli"
            + "ne.spec\":\"{\\\"endingActions\\\":[],\\\"stages\\\":[{\\\"name\\\":\\\"sour"
            + "ce\\\",\\\"plugin\\\":{\\\"type\\\":\\\"batchsource\\\",\\\"name\\\":\\\"File\\\","
            + "\\\"properties\\\":{\\\"referenceName\\\":\\\"TestFile\\\",\\\"fileSystem\\"
            + "\":\\\"Text\\\",\\\"path\\\":\\\"/tmp/input.txt"
            + "\\\",\\\"format\\\":\\\"text\\\",\\\"ignoreNonExistingFol"
            + "ders\\\":\\\"false\\\",\\\"pathField\\\":\\\"file\\\",\\\"schema\\\":\\\"{\\\\\\\"ty"
            + "pe\\\\\\\":\\\\\\\"record\\\\\\\",\\\\\\\"name\\\\\\\":\\\\\\\"file.record\\\\\\\",\\\\\\\"f"
            + "ields\\\\\\\":[{\\\\\\\"name\\\\\\\":\\\\\\\"offset\\\\\\\",\\\\\\\"type\\\\\\\":\\\\\\\"lon"
            + "g\\\\\\\"},{\\\\\\\"name\\\\\\\":\\\\\\\"body\\\\\\\",\\\\\\\"type\\\\\\\":[\\\\\\\"string\\\\"
            + "\\\",\\\\\\\"null\\\\\\\"]},{\\\\\\\"name\\\\\\\":\\\\\\\"file\\\\\\\",\\\\\\\"type\\\\\\\":[\\"
            + "\\\\\"string\\\\\\\",\\\\\\\"null\\\\\\\"]}]}\\\"},\\\"artifact\\\":{\\\"name\\\":\\\"c"
            + "ore-plugins\\\",\\\"version\\\":{\\\"version\\\":\\\"4.0.0\\\",\\\"major\\\":4"
            + ",\\\"minor\\\":0,\\\"fix\\\":0},\\\"scope\\\":\\\"USER\\\"}},\\\"inputSchemas\\"
            + "\":{},\\\"outputPorts\\\":{\\\"sink\\\":{\\\"schema\\\":{\\\"type\\\":\\\"recor"
            + "d\\\",\\\"name\\\":\\\"file.record\\\",\\\"fields\\\":[{\\\"name\\\":\\\"offset\\"
            + "\",\\\"type\\\":\\\"long\\\"},{\\\"name\\\":\\\"body\\\",\\\"type\\\":[\\\"string\\\""
            + ",\\\"null\\\"]},{\\\"name\\\":\\\"file\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\""
            + "]}]}}},\\\"outputSchema\\\":{\\\"type\\\":\\\"record\\\",\\\"name\\\":\\\"file"
            + ".record\\\",\\\"fields\\\":[{\\\"name\\\":\\\"offset\\\",\\\"type\\\":\\\"long\\\""
            + "},{\\\"name\\\":\\\"body\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name"
            + "\\\":\\\"file\\\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]}]},\\\"stageLogging"
            + "Enabled\\\":true,\\\"processTimingEnabled\\\":true,\\\"inputs\\\":[],\\"
            + "\"outputs\\\":[\\\"sink\\\"]},{\\\"name\\\":\\\"sink\\\",\\\"plugin\\\":{\\\"type"
            + "\\\":\\\"batchsink\\\",\\\"name\\\":\\\"HDFS\\\",\\\"properties\\\":{\\\"path\\\":"
            + "\\\"/tmp/hdfs-sink-output-dir\\\",\\\"referenceName\\\":\\\"HDFSinkTes"
            + "t\\\",\\\"delimiter\\\":\\\"|\\\"},\\\"artifact\\\":{\\\"name\\\":\\\"hdfs-plugi"
            + "ns\\\",\\\"version\\\":{\\\"version\\\":\\\"1.0.0\\\",\\\"major\\\":1,\\\"minor\\"
            + "\":0,\\\"fix\\\":0},\\\"scope\\\":\\\"USER\\\"}},\\\"inputSchemas\\\":{\\\"sour"
            + "ce\\\":{\\\"type\\\":\\\"record\\\",\\\"name\\\":\\\"file.record\\\",\\\"fields\\"
            + "\":[{\\\"name\\\":\\\"offset\\\",\\\"type\\\":\\\"long\\\"},{\\\"name\\\":\\\"body\\"
            + "\",\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"file\\\",\\\"type\\"
            + "\":[\\\"string\\\",\\\"null\\\"]}]}},\\\"outputPorts\\\":{},\\\"errorSchema"
            + "\\\":{\\\"type\\\":\\\"record\\\",\\\"name\\\":\\\"file.record\\\",\\\"fields\\\":"
            + "[{\\\"name\\\":\\\"offset\\\",\\\"type\\\":\\\"long\\\"},{\\\"name\\\":\\\"body\\\","
            + "\\\"type\\\":[\\\"string\\\",\\\"null\\\"]},{\\\"name\\\":\\\"file\\\",\\\"type\\\":"
            + "[\\\"string\\\",\\\"null\\\"]}]},\\\"stageLoggingEnabled\\\":true,\\\"proc"
            + "essTimingEnabled\\\":true,\\\"inputs\\\":[\\\"source\\\"],\\\"outputs\\\":"
            + "[]}],\\\"connections\\\":[{\\\"from\\\":\\\"source\\\",\\\"to\\\":\\\"sink\\\"}]"
            + ",\\\"resources\\\":{\\\"virtualCores\\\":1,\\\"memoryMB\\\":1024},\\\"driv"
            + "erResources\\\":{\\\"virtualCores\\\":1,\\\"memoryMB\\\":1024},\\\"clien"
            + "tResources\\\":{\\\"virtualCores\\\":1,\\\"memoryMB\\\":1024},\\\"stageL"
            + "oggingEnabled\\\":true,\\\"processTimingEnabled\\\":true,\\\"numOfRe"
            + "cordsPreview\\\":0,\\\"properties\\\":{}}\"},\"nodes\":[{\"program\":{\""
            + "programName\":\"phase-1\",\"programType\":\"SPARK\"},\"nodeId\":\"phas"
            + "e-1\",\"nodeType\":\"ACTION\"}],\"localDatasetSpecs\":{}}},\"service"
            + "s\":{},\"programSchedules\":{\"dataPipelineSchedule\":{\"name\":\"da"
            + "taPipelineSchedule\",\"description\":\"Data pipeline schedule\",\""
            + "programName\":\"DataPipelineWorkflow\",\"properties\":{},\"trigger"
            + "\":{\"cronExpression\":\"* * * * *\",\"type\":\"TIME\"},\"constraints\""
            + ":[],\"timeoutMillis\":86400000}},\"workers\":{},\"plugins\":{\"sink"
            + "\":{\"parents\":[],\"artifactId\":{\"name\":\"hdfs-plugins\",\"version"
            + "\":{\"version\":\"1.0.0\",\"major\":1,\"minor\":0,\"fix\":0},\"scope\":\"U"
            + "SER\"},\"pluginClass\":{\"type\":\"batchsink\",\"name\":\"HDFS\",\"descr"
            + "iption\":\"Batch HDFS Sink\",\"className\":\"co.cask.hydrator.plug"
            + "in.HDFSSink\",\"configFieldName\":\"config\",\"properties\":{\"path\""
            + ":{\"name\":\"path\",\"description\":\"HDFS Destination Path Prefix."
            + " For example, \\u0027hdfs://mycluster.net:8020/output\",\"type\""
            + ":\"string\",\"required\":true,\"macroSupported\":true,\"macroEscapi"
            + "ngEnabled\":false},\"jobProperties\":{\"name\":\"jobProperties\",\"d"
            + "escription\":\"Advanced feature to specify any additional prop"
            + "erties that should be used with the sink, specified as a JSO"
            + "N object of string to string. These properties are set on th"
            + "e job.\",\"type\":\"string\",\"required\":false,\"macroSupported\":tr"
            + "ue,\"macroEscapingEnabled\":false},\"delimiter\":{\"name\":\"delimi"
            + "ter\",\"description\":\"The delimiter to use when concatenating "
            + "record fields. Defaults to a comma (\\u0027,\\u0027).\",\"type\":"
            + "\"string\",\"required\":false,\"macroSupported\":true,\"macroEscapi"
            + "ngEnabled\":false},\"suffix\":{\"name\":\"suffix\",\"description\":\"T"
            + "ime Suffix used for destination directory for each run. For "
            + "example, \\u0027YYYY-MM-dd-HH-mm\\u0027. By default, no time s"
            + "uffix is used.\",\"type\":\"string\",\"required\":false,\"macroSuppo"
            + "rted\":true,\"macroEscapingEnabled\":false},\"referenceName\":{\"n"
            + "ame\":\"referenceName\",\"description\":\"This will be used to uni"
            + "quely identify this source/sink for lineage, annotating meta"
            + "data, etc.\",\"type\":\"string\",\"required\":true,\"macroSupported\""
            + ":false,\"macroEscapingEnabled\":false}},\"endpoints\":[]},\"prope"
            + "rties\":{\"properties\":{\"path\":\"/tmp/hdfs-sink-output-dir\",\"de"
            + "limiter\":\"|\",\"referenceName\":\"HDFSinkTest\"},\"macros\":{\"looku"
            + "pProperties\":[],\"macroFunctions\":[]}}},\"source\":{\"parents\":["
            + "],\"artifactId\":{\"name\":\"core-plugins\",\"version\":{\"version\":\""
            + "4.0.0\",\"major\":4,\"minor\":0,\"fix\":0},\"scope\":\"USER\"},\"pluginC"
            + "lass\":{\"type\":\"batchsource\",\"name\":\"File\",\"description\":\"Bat"
            + "ch source for File Systems\",\"className\":\"co.cask.hydrator.pl"
            + "ugin.batch.source.FileBatchSource\",\"configFieldName\":\"config"
            + "\",\"properties\":{\"schema\":{\"name\":\"schema\",\"description\":\"Sch"
            + "ema for the source\",\"type\":\"string\",\"required\":false,\"macroS"
            + "upported\":false,\"macroEscapingEnabled\":false},\"fileRegex\":{\""
            + "name\":\"fileRegex\",\"description\":\"Regex to filter out files i"
            + "n the path. It accepts regular expression which is applied t"
            + "o the complete path and returns the list of files that match"
            + " the specified pattern.To use the TimeFilter, input \\\"timefi"
            + "lter\\\". The TimeFilter assumes that it is reading in files w"
            + "ith the File log naming convention of \\u0027YYYY-MM-DD-HH-mm"
            + "-SS-Tag\\u0027. The TimeFilter reads in files from the previo"
            + "us hour if the field \\u0027timeTable\\u0027 is left blank. If"
            + " it\\u0027s currently 2015-06-16-15 (June 16th 2015, 3pm), it"
            + " will read in files that contain \\u00272015-06-16-14\\u0027 i"
            + "n the filename. If the field \\u0027timeTable\\u0027 is presen"
            + "t, then it will read in files that have not yet been read. D"
            + "efaults to \\u0027.*\\u0027, which indicates that no files wil"
            + "l be filtered.\",\"type\":\"string\",\"required\":false,\"macroSuppo"
            + "rted\":true,\"macroEscapingEnabled\":false},\"inputFormatClass\":"
            + "{\"name\":\"inputFormatClass\",\"description\":\"Name of the input "
            + "format class, which must be a subclass of FileInputFormat. D"
            + "efaults to a CombinePathTrackingInputFormat, which is a cust"
            + "omized version of CombineTextInputFormat that records the fi"
            + "le path each record was read from.\",\"type\":\"string\",\"require"
            + "d\":false,\"macroSupported\":true,\"macroEscapingEnabled\":false}"
            + ",\"format\":{\"name\":\"format\",\"description\":\"Format of the file"
            + ". Must be \\u0027text\\u0027, \\u0027avro\\u0027 or \\u0027parque"
            + "t\\u0027. Defaults to \\u0027text\\u0027.\",\"type\":\"string\",\"req"
            + "uired\":false,\"macroSupported\":false,\"macroEscapingEnabled\":f"
            + "alse},\"ignoreNonExistingFolders\":{\"name\":\"ignoreNonExistingF"
            + "olders\",\"description\":\"Identify if path needs to be ignored "
            + "or not, for case when directory or file does not exists. If "
            + "set to true it will treat the not present folder as zero inp"
            + "ut and log a warning. Default is false.\",\"type\":\"boolean\",\"r"
            + "equired\":false,\"macroSupported\":false,\"macroEscapingEnabled\""
            + ":false},\"timeTable\":{\"name\":\"timeTable\",\"description\":\"Name "
            + "of the Table that keeps track of the last time files were re"
            + "ad in. If this is null or empty, the Regex is used to filter"
            + " filenames.\",\"type\":\"string\",\"required\":false,\"macroSupporte"
            + "d\":true,\"macroEscapingEnabled\":false},\"pathField\":{\"name\":\"p"
            + "athField\",\"description\":\"If specified, each output record wi"
            + "ll include a field with this name that contains the file URI"
            + " that the record was read from. Requires a customized versio"
            + "n of CombineFileInputFormat, so it cannot be used if an inpu"
            + "tFormatClass is given.\",\"type\":\"string\",\"required\":false,\"ma"
            + "croSupported\":false,\"macroEscapingEnabled\":false},\"recursive"
            + "\":{\"name\":\"recursive\",\"description\":\"Boolean value to determ"
            + "ine if files are to be read recursively from the path. Defau"
            + "lt is false.\",\"type\":\"boolean\",\"required\":false,\"macroSuppor"
            + "ted\":false,\"macroEscapingEnabled\":false},\"filenameOnly\":{\"na"
            + "me\":\"filenameOnly\",\"description\":\"If true and a pathField is"
            + " specified, only the filename will be used. If false, the fu"
            + "ll URI will be used. Defaults to false.\",\"type\":\"boolean\",\"r"
            + "equired\":false,\"macroSupported\":false,\"macroEscapingEnabled\""
            + ":false},\"path\":{\"name\":\"path\",\"description\":\"Path to file(s)"
            + " to be read. If a directory is specified, terminate the path"
            + " name with a \\u0027/\\u0027. For distributed file system such"
            + " as HDFS, file system name should come from \\u0027fs.Default"
            + "FS\\u0027 property in the \\u0027core-site.xml\\u0027. For exam"
            + "ple, \\u0027hdfs://mycluster.net:8020/input\\u0027, where valu"
            + "e of the property \\u0027fs.DefaultFS\\u0027 in the \\u0027core"
            + "-site.xml\\u0027 is \\u0027hdfs://mycluster.net:8020\\u0027. Th"
            + "e path uses filename expansion (globbing) to read files.\",\"t"
            + "ype\":\"string\",\"required\":true,\"macroSupported\":true,\"macroEs"
            + "capingEnabled\":false},\"maxSplitSize\":{\"name\":\"maxSplitSize\","
            + "\"description\":\"Maximum split-size for each mapper in the Map"
            + "Reduce Job. Defaults to 128MB.\",\"type\":\"long\",\"required\":fal"
            + "se,\"macroSupported\":true,\"macroEscapingEnabled\":false},\"file"
            + "SystemProperties\":{\"name\":\"fileSystemProperties\",\"descriptio"
            + "n\":\"A JSON string representing a map of properties needed fo"
            + "r the distributed file system.\",\"type\":\"string\",\"required\":f"
            + "alse,\"macroSupported\":true,\"macroEscapingEnabled\":false},\"re"
            + "ferenceName\":{\"name\":\"referenceName\",\"description\":\"This wil"
            + "l be used to uniquely identify this source/sink for lineage,"
            + " annotating metadata, etc.\",\"type\":\"string\",\"required\":true,"
            + "\"macroSupported\":false,\"macroEscapingEnabled\":false}},\"endpo"
            + "ints\":[]},\"properties\":{\"properties\":{\"schema\":\"{\\\"type\\\":\\\""
            + "record\\\",\\\"name\\\":\\\"file.record\\\",\\\"fields\\\":[{\\\"name\\\":\\\"of"
            + "fset\\\",\\\"type\\\":\\\"long\\\"},{\\\"name\\\":\\\"body\\\",\\\"type\\\":[\\\"str"
            + "ing\\\",\\\"null\\\"]},{\\\"name\\\":\\\"file\\\",\\\"type\\\":[\\\"string\\\",\\\"n"
            + "ull\\\"]}]}\",\"fileSystem\":\"Text\",\"path\":\"/tmp/input.txt"
            + "\",\"format\":\"text\",\"ignoreNon"
            + "ExistingFolders\":\"false\",\"pathField\":\"file\",\"referenceName\":"
            + "\"TestFile\"},\"macros\":{\"lookupProperties\":[],\"macroFunctions\""
            + ":[]}}}}}";

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        CConfiguration cConf = CConfiguration.create();
        Configuration hConf = new Configuration();

        Injector injector = createInjector(cConf, hConf, programId);
        SparkProgramRunner sparkProgramRunner = injector.getInstance(SparkProgramRunner.class);


        Location jarLocation =
                //new LocalLocationFactory().create("/tmp/1.0.0.40bffbce-d285-4342-9a82-7555e0150a89.jar"); // TODO
                new LocalLocationFactory().create("/tmp/3.4.0-SNAPSHOT.440ff5f9-55d8-4356-b35d-c52d525199d4.jar");


        Preconditions.checkArgument(jarLocation.exists());



        File tempDir = new File("/tmp/dsprmain_tmp");
        File programJarUnpacked = new File(tempDir, "unpacked");
        //programJarUnpacked.mkdirs();
        //try {
        //    File programJar = Locations.linkOrCopy(jarLocation, new File(tempDir, "program.jar"));
        //    // Unpack the JAR file
        //    BundleJarUtil.unJar(Files.newInputStreamSupplier(programJar), programJarUnpacked);
        //} catch (IOException ioe) {
        //    throw ioe;
        //} catch (Exception e) {
        //    // should not happen
        //    throw Throwables.propagate(e);
        //}

        ProgramDescriptor programDescriptor =
                new ProgramDescriptor(programId, GSON.fromJson(appSpecString, ApplicationSpecification.class));
        // See AbstractProgramRuntimeService#run
        Program program = Programs.create(cConf, sparkProgramRunner, programDescriptor,
                jarLocation, programJarUnpacked);


        Map<String, String> optionsMap = new HashMap<>();
        optionsMap.put(ProgramOptionConstants.RUN_ID, runId);
        ArtifactId artifactId = new ArtifactId("default", "artifact", "1");
        optionsMap.put(ProgramOptionConstants.ARTIFACT_ID, Joiner.on(':').join(artifactId.toIdParts()));
        optionsMap.put(ProgramOptionConstants.HOST, "localhost"); // reqd by SparkProgramRunner
        optionsMap.put(ProgramOptionConstants.PLUGIN_DIR, "/tmp/plugins_dir");
        SimpleProgramOptions options = new SimpleProgramOptions(programId, new BasicArguments(optionsMap),
                new BasicArguments());





        List<Service> coreServices = new ArrayList<>();
        coreServices.add(injector.getInstance(ZKClientService.class));
        coreServices.add(injector.getInstance(KafkaClientService.class));
        coreServices.add(injector.getInstance(BrokerService.class));
        coreServices.add(injector.getInstance(MetricsCollectionService.class));
        coreServices.add(injector.getInstance(StreamCoordinatorClient.class));

        //LogAppenderInitializer logAppenderInitializer = injector.getInstance(LogAppenderInitializer.class);
        //logAppenderInitializer.initialize();

        Futures.getUnchecked(
                Services.chainStart(coreServices.get(0),
                        coreServices.subList(1, coreServices.size()).toArray(new Service[coreServices.size() - 1])));





        ProgramController controller = sparkProgramRunner.run(program, options);

        System.out.println("output: " + waitForCompletion(controller));

        //DirUtils.deleteDirectoryContents(tempDir);
    }

    private static boolean waitForCompletion(ProgramController controller) throws InterruptedException {
        final AtomicBoolean success = new AtomicBoolean(false);
        final CountDownLatch completion = new CountDownLatch(1);
        controller.addListener(new AbstractListener() {
            @Override
            public void completed() {
                success.set(true);
                completion.countDown();
            }

            @Override
            public void error(Throwable cause) {
                System.out.println("error!!!");
                cause.printStackTrace();
                completion.countDown();
            }
        }, Threads.SAME_THREAD_EXECUTOR);

        // MR tests can run for long time.
        completion.await(10, TimeUnit.MINUTES);
        return success.get();
    }

    private static Injector createInjector(CConfiguration cConf, Configuration hConf, ProgramId programId) {
        //MapReduceContextConfig mapReduceContextConfig = new MapReduceContextConfig(hConf);
        // principal will be null if running on a kerberos distributed cluster
        //Arguments arguments = mapReduceContextConfig.getProgramOptions().getArguments();
        //String principal = arguments.getOption(ProgramOptionConstants.PRINCIPAL);
        String instanceId = "instanceId"; // arguments.getOption(ProgramOptionConstants.INSTANCE_ID);
        return Guice.createInjector(new DistributedProgramRunnableModule(cConf, hConf)
                // Is this the same runId here?
                .createModule(programId, runId, instanceId, null));
    }

}

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

package co.cask.cdap.internal.app.runtime.batch.dataproc;

import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.app.program.DefaultProgram;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.program.ProgramDescriptor;
import co.cask.cdap.app.program.Programs;
import co.cask.cdap.app.runtime.Arguments;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.data.stream.StreamCoordinatorClient;
import co.cask.cdap.internal.app.ApplicationSpecificationAdapter;
import co.cask.cdap.internal.app.runtime.AbstractListener;
import co.cask.cdap.internal.app.runtime.BasicArguments;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.SimpleProgramOptions;
import co.cask.cdap.internal.app.runtime.batch.MapReduceProgramRunner;
import co.cask.cdap.internal.app.runtime.codec.ArgumentsCodec;
import co.cask.cdap.internal.app.runtime.codec.ProgramOptionsCodec;
import co.cask.cdap.logging.appender.LogAppenderInitializer;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.ProgramId;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
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
public class DMRPRMain {
    private static final Gson GSON = ApplicationSpecificationAdapter.addTypeAdapters(new GsonBuilder())
            .registerTypeAdapter(Arguments.class, new ArgumentsCodec())
            .registerTypeAdapter(ProgramOptions.class, new ProgramOptionsCodec())
            .create();


    private static ProgramId programId = new ProgramId("default", "appname", ProgramType.MAPREDUCE,
            "PurchaseHistoryBuilder");
    private static final String runId = RunIds.generate().getId();

    private static final String appSpecString = "{  \n"
            + "   \"name\":\"appName\",\n"
            + "   \"appVersion\":\"-SNAPSHOT\",\n"
            + "   \"description\":\"Purchase history application\",\n"
            + "   \"configuration\":\"\",\n"
            + "   \"artifactId\":{  \n"
            + "      \"name\":\"a\",\n"
            + "      \"version\":{  \n"
            + "         \"version\":\"1\",\n"
            + "         \"major\":1\n"
            + "      },\n"
            + "      \"scope\":\"USER\"\n"
            + "   },\n"
            + "   \"mapReduces\":{  \n"
            + "      \"PurchaseHistoryBuilder\":{  \n"
            + "         \"className\":\"co.cask.cdap.examples.purchase.PurchaseHistoryBuilder\",\n"
            + "         \"name\":\"PurchaseHistoryBuilder\",\n"
            + "         \"description\":\"Purchase History Builder\",\n"
            + "         \"driverResources\":{  \n"
            + "            \"virtualCores\":1,\n"
            + "            \"memoryMB\":1024\n"
            + "         },\n"
            + "         \"mapperResources\":{  \n"
            + "            \"virtualCores\":1,\n"
            + "            \"memoryMB\":1024\n"
            + "         },\n"
            + "         \"reducerResources\":{  \n"
            + "            \"virtualCores\":1,\n"
            + "            \"memoryMB\":1024\n"
            + "         }"
            + "      }\n"
            + "   }\n"
            + "}";


    public static void main(String[] args) throws IOException, InterruptedException {
        CConfiguration cConf = CConfiguration.create();
        Configuration hConf = new Configuration();

        Injector injector = createInjector(cConf, hConf, programId);
        MapReduceProgramRunner mapReduceProgramRunner = injector.getInstance(MapReduceProgramRunner.class);


        Location jarLocation = new LocalLocationFactory().create("/tmp/Purchase-5.0.0-SNAPSHOT.jar"); // TODO

        Preconditions.checkArgument(jarLocation.exists());

        ProgramDescriptor programDescriptor =
                new ProgramDescriptor(programId, GSON.fromJson(appSpecString, ApplicationSpecification.class));

        Program program = Programs.create(cConf, mapReduceProgramRunner, programDescriptor,
                jarLocation, new File("/tmp/purchase_unpacked"));


        Map<String, String> optionsMap = new HashMap<>();
        optionsMap.put(ProgramOptionConstants.RUN_ID, runId);
        ArtifactId artifactId = new ArtifactId("default", "artifact", "1");
        optionsMap.put(ProgramOptionConstants.ARTIFACT_ID, Joiner.on(':').join(artifactId.toIdParts()));
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





        ProgramController controller = mapReduceProgramRunner.run(program, options);

        System.out.println("output: " + waitForCompletion(controller));
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

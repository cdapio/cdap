/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.examples.wikipedia;

import co.cask.cdap.api.Predicate;
import co.cask.cdap.api.workflow.AbstractWorkflow;
import co.cask.cdap.api.workflow.Value;
import co.cask.cdap.api.workflow.WorkflowContext;
import co.cask.cdap.api.workflow.WorkflowToken;

import java.util.Map;

/**
 * Workflow for Wikipedia data pipeline
 */
public class WikipediaPipelineWorkflow extends AbstractWorkflow {

  static final String NAME = WikipediaPipelineWorkflow.class.getSimpleName();
  static final String MIN_PAGES_THRESHOLD_KEY = "min.pages.threshold";
  static final String MODE_KEY = "mode";
  static final String ONLINE_MODE = "online";

  @Override
  protected void configure() {
    setName(NAME);
    setDescription("A workflow that demonstrates a typical data pipeline to process Wikipedia data.");
    addMapReduce(WikipediaPipelineApp.LIKES_TO_DATASET_MR_NAME);
    condition(new EnoughDataToProceed())
      .condition(new IsWikipediaSourceOnline())
        .addMapReduce(WikipediaDataDownloader.NAME)
      .otherwise()
        .addMapReduce(WikipediaPipelineApp.WIKIPEDIA_TO_DATASET_MR_NAME)
      .end()
      .addMapReduce(WikiContentValidatorAndNormalizer.NAME)
      .fork()
        .addSpark(SparkWikipediaAnalyzer.NAME)
      .also()
        .addMapReduce(TopNMapReduce.NAME)
      .join()
    .otherwise()
    .end();
  }

  static class EnoughDataToProceed implements Predicate<WorkflowContext> {

    @Override
    public boolean apply(WorkflowContext context) {
      Map<String, String> runtimeArguments = context.getRuntimeArguments();
      int threshold = 10;
      if (runtimeArguments.containsKey(MIN_PAGES_THRESHOLD_KEY)) {
        threshold = Integer.parseInt(runtimeArguments.get(MIN_PAGES_THRESHOLD_KEY));
      }
      WorkflowToken token = context.getToken();
      Value result = token.get("result", WikipediaPipelineApp.LIKES_TO_DATASET_MR_NAME);
      Value numPages = token.get("custom.num.records", WorkflowToken.Scope.SYSTEM);
      boolean conditionResult = result != null && result.getAsBoolean() &&
        numPages != null && numPages.getAsLong() > threshold;
      token.put("result", Value.of(conditionResult));

      // Also add information in the token on whether to download wikipedia data over the internet
      // NOTE: The following predicate can even consume this information through runtimeArguments. However,
      // we want to demonstrate the usage of workflow token through this example, so adding this indirection.
      if (runtimeArguments.containsKey(MODE_KEY)) {
        token.put(ONLINE_MODE, Value.of(runtimeArguments.get(MODE_KEY).equalsIgnoreCase(ONLINE_MODE)));
      }
      return conditionResult;
    }
  }

  static class IsWikipediaSourceOnline implements Predicate<WorkflowContext> {

    @Override
    public boolean apply(WorkflowContext context) {
      WorkflowToken token = context.getToken();
      Value online = token.get("online", EnoughDataToProceed.class.getSimpleName(), WorkflowToken.Scope.USER);
      return online != null && online.getAsBoolean();
    }
  }
}

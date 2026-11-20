/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.etl.common;

import io.cdap.cdap.api.Admin;
import io.cdap.cdap.api.data.batch.Input;
import io.cdap.cdap.api.data.batch.InputFormatProvider;
import io.cdap.cdap.api.data.batch.Output;
import io.cdap.cdap.api.data.batch.OutputFormatProvider;
import io.cdap.cdap.api.dataset.DatasetManagementException;
import io.cdap.cdap.api.dataset.DatasetProperties;
import java.util.Collections;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit tests for {@link ExternalDatasets}.
 */
public class ExternalDatasetsTest {

  @Test
  public void testMakeTrackableInputCreatesDatasetAndPropagatesException() throws Exception {
    InputFormatProvider provider = new InputFormatProvider() {
      @Override
      public String getInputFormatClassName() {
        return "com.example.TestInputFormat";
      }

      @Override
      public Map<String, String> getInputFormatConfiguration() {
        return Collections.singletonMap("path", "/tmp/in");
      }
    };

    Admin admin = Mockito.mock(Admin.class);
    Mockito.when(admin.datasetExists("extIn")).thenReturn(false);

    Input trackable = ExternalDatasets.makeTrackable(admin, Input.of("extIn", provider).alias("inAlias"));
    Assert.assertTrue(trackable instanceof Input.DatasetInput);
    Assert.assertEquals("inAlias", trackable.getAlias());
    Mockito.verify(admin).createDataset(
        Mockito.eq("extIn"), Mockito.eq("externalDataset"), Mockito.any(DatasetProperties.class));

    DatasetManagementException dsFailure = new DatasetManagementException("input create failed");
    Mockito.doThrow(dsFailure).when(admin)
        .createDataset(Mockito.eq("failIn"), Mockito.anyString(), Mockito.any(DatasetProperties.class));
    try {
      ExternalDatasets.makeTrackable(admin, Input.of("failIn", provider));
      Assert.fail("Expected RuntimeException wrapping DatasetManagementException");
    } catch (RuntimeException e) {
      Assert.assertSame(dsFailure, e.getCause());
    }
  }

  @Test
  public void testMakeTrackableOutputCreatesDatasetAndPropagatesException() throws Exception {
    OutputFormatProvider provider = new OutputFormatProvider() {
      @Override
      public String getOutputFormatClassName() {
        return "com.example.TestOutputFormat";
      }

      @Override
      public Map<String, String> getOutputFormatConfiguration() {
        return Collections.singletonMap("path", "/tmp/out");
      }
    };

    Admin admin = Mockito.mock(Admin.class);
    Mockito.when(admin.datasetExists("extOut")).thenReturn(false);

    Output trackable = ExternalDatasets.makeTrackable(admin, Output.of("extOut", provider).alias("outAlias"));
    Assert.assertTrue(trackable instanceof Output.DatasetOutput);
    Assert.assertEquals("outAlias", trackable.getAlias());
    Mockito.verify(admin).createDataset(
        Mockito.eq("extOut"), Mockito.eq("externalDataset"), Mockito.any(DatasetProperties.class));

    DatasetManagementException dsFailure = new DatasetManagementException("output exists check failed");
    Mockito.when(admin.datasetExists("failOut")).thenThrow(dsFailure);
    try {
      ExternalDatasets.makeTrackable(admin, Output.of("failOut", provider));
      Assert.fail("Expected RuntimeException wrapping DatasetManagementException");
    } catch (RuntimeException e) {
      Assert.assertSame(dsFailure, e.getCause());
    }
  }
}

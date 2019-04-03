/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.partitioned.concatenate;

import co.cask.cdap.api.dataset.lib.PartitionKey;
import co.cask.cdap.api.dataset.lib.PartitionOutput;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import co.cask.cdap.api.dataset.lib.PartitionedFileSetProperties;
import co.cask.cdap.api.dataset.lib.Partitioning;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.base.TestFrameworkTestBase;
import com.google.common.collect.Iterables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.orc.CompressionKind;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcNewOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.Writer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.twill.filesystem.Location;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Tests functionality to concatenate files within a partition of a PartitionedFileSet.
 */
public class PartitionConcatenateTest extends TestFrameworkTestBase {

  /**
   * 1. Write 100 small files (orc format) to a Partition of a PartitionedFileSet.
   * 2. Execute a partition concatenate operation.
   * 3. As compared to before the concatenate operation, validate that the number of files is reduced, while
   *    the contents of the files remains the same.
   */
  @Test
  public void testConcatenate() throws Exception {
    String orcPFS = "orcPFS";
    addDatasetInstance(PartitionedFileSet.class.getName(), orcPFS, PartitionedFileSetProperties.builder()
      // Properties for partitioning
      .setPartitioning(Partitioning.builder().addLongField("time").build())
      // Properties for file set
      .setOutputFormat(OrcNewOutputFormat.class)
      // Properties for Explore (to create a partitioned Hive table)
      .setEnableExploreOnCreate(true)
      .setSerDe("org.apache.hadoop.hive.ql.io.orc.OrcSerde")
      .setExploreInputFormat("org.apache.hadoop.hive.ql.io.orc.OrcInputFormat")
      .setExploreOutputFormat("org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat")
      .setExploreSchema("record STRING")
      .build());

    // 1. create 100 small files in the input FileSet
    DataSetManager<PartitionedFileSet> cleanRecordsManager = getDataset(orcPFS);
    PartitionedFileSet cleanRecords = cleanRecordsManager.get();

    PartitionKey outputPartition = PartitionKey.builder().addLongField("time", 5000).build();
    PartitionOutput partitionOutput = cleanRecords.getPartitionOutput(outputPartition);
    Location partitionLocation = partitionOutput.getLocation();
    int numInputFiles = 100;
    List<String> writtenData = writeSmallOrcFiles(partitionLocation, numInputFiles);
    partitionOutput.addPartition();

    Assert.assertEquals(writtenData, getExploreResults(orcPFS));

    // this is a timestamp before concatenating, but after writing the files
    long beforeConcatTime = System.currentTimeMillis();

    List<Location> dataFiles = listFilteredChildren(partitionLocation);
    // each input file will result in one output file, due to the FileInputFormat class and FileOutputFormat class
    // being used
    Assert.assertEquals(numInputFiles, dataFiles.size());
    for (Location dataFile : dataFiles) {
      // all the files should have a lastModified smaller than now
      Assert.assertTrue(dataFile.lastModified() < beforeConcatTime);
    }

    // 2. run the concatenate operation
    cleanRecords.concatenatePartition(outputPartition).get();

    // 3. check that the data files' lastModified timestamp is updated, and there should be fewer of them
    dataFiles = listFilteredChildren(partitionLocation);
    Assert.assertTrue(dataFiles.size() < numInputFiles);
    // should have a lastModified larger than now
    Assert.assertTrue(Iterables.getOnlyElement(dataFiles).lastModified() > beforeConcatTime);

    // even though the files were concatenated, the explore results should be unchanged
    Assert.assertEquals(writtenData, getExploreResults(orcPFS));
  }

  private List<String> writeSmallOrcFiles(Location baseLocation, int numInputFiles) throws IOException {
    TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString("struct<key:string>");
    ObjectInspector objectInspector = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(typeInfo);

    Configuration hConf = new Configuration();
    FileSystem fileSystem = FileSystem.get(hConf);
    long stripeSize = HiveConf.getLongVar(hConf, HiveConf.ConfVars.HIVE_ORC_DEFAULT_STRIPE_SIZE);
    CompressionKind compressionKind =
      CompressionKind.valueOf(HiveConf.getVar(hConf, HiveConf.ConfVars.HIVE_ORC_DEFAULT_COMPRESS));
    int bufferSize = HiveConf.getIntVar(hConf, HiveConf.ConfVars.HIVE_ORC_DEFAULT_BUFFER_SIZE);
    int rowIndexStride = HiveConf.getIntVar(hConf, HiveConf.ConfVars.HIVE_ORC_DEFAULT_ROW_INDEX_STRIDE);

    List<String> writtenData = new ArrayList<>();
    for (int i = 0; i < numInputFiles; i++) {
      Location childFile = baseLocation.append("child_" + i);

      Writer orcWriter = OrcFile.createWriter(fileSystem, new Path(childFile.toURI()), hConf, objectInspector,
                                              stripeSize, compressionKind, bufferSize, rowIndexStride);
      try {
        String toWrite = "outputData" + i;
        orcWriter.addRow(Collections.singletonList(toWrite));
        writtenData.add(toWrite);
      } finally {
        orcWriter.close();
      }
    }
    Collections.sort(writtenData);
    return writtenData;
  }

  private List<String> getExploreResults(String datasetName) throws Exception {
    ResultSet resultSet =
      getQueryClient().prepareStatement("select * from dataset_" + datasetName).executeQuery();
    List<String> strings = new ArrayList<>();
    while (resultSet.next()) {
      // the schema is such that the record contents are all in the first column
      strings.add(resultSet.getString(1));
    }
    Collections.sort(strings);
    return strings;
  }

  // Lists children files, and filters "_SUCCESS" files and files that begin with dot (".")
  private List<Location> listFilteredChildren(Location location) throws IOException {
    List<Location> children = new ArrayList<>();
    for (Location child : location.list()) {
      if (!child.getName().startsWith(".") && !FileOutputCommitter.SUCCEEDED_FILE_NAME.equals(child.getName())) {
        children.add(child);
      }
    }
    return children;
  }
}

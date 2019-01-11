package co.cask.cdap.data2.metadata.store;

import co.cask.cdap.api.metadata.MetadataEntity;
import co.cask.cdap.api.metadata.MetadataScope;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.StreamId;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * Implementation-agnostic tests for the metadata store.
 */
public abstract class MetadataStoreTest {

  protected static MetadataStore store;

  private final MetadataEntity app1 = new ApplicationId("ns1", "app1").toMetadataEntity();
  private final MetadataEntity appNs2 = new ApplicationId("ns2", "app1").toMetadataEntity();
  // Have to use Id.Program for comparison here because the MetadataDataset APIs return Id.Program.
  private final MetadataEntity program1 = new ProgramId("ns1", "app1", ProgramType.WORKER, "wk1").toMetadataEntity();
  private final MetadataEntity dataset1 = new DatasetId("ns1", "ds1").toMetadataEntity();
  private final MetadataEntity stream1 = new StreamId("ns1", "s1").toMetadataEntity();
  private final MetadataEntity artifact1 = new ArtifactId("ns1", "a1", "1.0.0").toMetadataEntity();
  private final MetadataEntity fileEntity = MetadataEntity.builder().append(MetadataEntity.NAMESPACE, "ns1")
    .append(MetadataEntity.DATASET, "ds1").appendAsType("file", "f1").build();
  private final MetadataEntity partitionFileEntity = MetadataEntity.builder().append(MetadataEntity.NAMESPACE, "ns1")
    .append(MetadataEntity.DATASET, "ds1").append("partition", "p1")
    .appendAsType("file", "f1").build();
  private final MetadataEntity jarEntity = MetadataEntity.builder().append(MetadataEntity.NAMESPACE, "ns1")
    .appendAsType("jar", "jar1").build();

  @Test
  public void testProperties() {
    MetadataScope scope = MetadataScope.SYSTEM;

    Assert.assertEquals(0, store.getProperties(scope, app1).size());
    Assert.assertEquals(0, store.getProperties(scope, program1).size());
    Assert.assertEquals(0, store.getProperties(scope, dataset1).size());
    Assert.assertEquals(0, store.getProperties(scope, stream1).size());
    Assert.assertEquals(0, store.getProperties(scope, artifact1).size());
    Assert.assertEquals(0, store.getProperties(scope, fileEntity).size());
    Assert.assertEquals(0, store.getProperties(scope, partitionFileEntity).size());
    Assert.assertEquals(0, store.getProperties(scope, jarEntity).size());

    // Set some properties
    store.setProperty(scope, app1, "akey1", "avalue1");
    // store.setProperties(program1, Collections.emptyMap());
    // Assert.assertEquals(metadataChange.getExisting(), new Metadata(program1, Collections.emptyMap(),
    //                                                                Collections.emptySet()));
    // Assert.assertEquals(metadataChange.getLatest(), new Metadata(program1, Collections.emptyMap(),
    //                                                              Collections.emptySet()));
    store.setProperty(scope, program1, "fkey1", "fvalue1");
    // assert the metadata change which happens on setting property for the first time
    // Assert.assertEquals(new Metadata(program1), metadataChange.getExisting());
    // Assert.assertEquals(new Metadata(program1, ImmutableMap.of("fkey1", "fvalue1"), Collections.emptySet()),
    //                     metadataChange.getLatest());
    store.setProperty(scope, program1, "fK", "fV");
    // assert the metadata change which happens when setting property with existing property
    // Assert.assertEquals(new Metadata(program1, ImmutableMap.of("fkey1", "fvalue1"), Collections.emptySet()),
    //                     metadataChange.getExisting());
    // Assert.assertEquals(new Metadata(program1, ImmutableMap.of("fkey1", "fvalue1", "fK", "fV"),
    //                                  Collections.emptySet()),
    //                     metadataChange.getLatest());
    store.setProperty(scope, dataset1, "dkey1", "dvalue1");
    store.setProperty(scope, stream1, "skey1", "svalue1");
    store.setProperty(scope, stream1, "skey2", "svalue2");
    store.setProperty(scope, artifact1, "rkey1", "rvalue1");
    store.setProperty(scope, artifact1, "rkey2", "rvalue2");
    store.setProperty(scope, fileEntity, "fkey2", "fvalue2");
    store.setProperty(scope, partitionFileEntity, "pfkey2", "pfvalue2");
    store.setProperty(scope, jarEntity, "jkey2", "jvalue2");

    // verify
    Map<String, String> properties = store.getProperties(scope, app1);
    Assert.assertEquals(ImmutableMap.of("akey1", "avalue1"), properties);

    store.removeProperties(scope, app1, Collections.singleton("akey1"));

    properties = store.getProperties(scope, jarEntity);
    Assert.assertEquals(ImmutableMap.of("jkey2", "jvalue2"), properties);

    store.removeProperties(scope, jarEntity, ImmutableSet.of("jkey2"));

    Assert.assertNull(store.getProperties(scope, app1).get("akey1"));
    Assert.assertEquals(ImmutableMap.of("fkey1", "fvalue1", "fK", "fV"), store.getProperties(scope, program1));

    store.removeProperties(scope, program1, Collections.singleton("fkey1"));
    properties = store.getProperties(scope, program1);
    Assert.assertEquals(1, properties.size());
    Assert.assertEquals("fV", properties.get("fK"));

    store.removeProperties(scope, program1);
    Assert.assertEquals(0, store.getProperties(scope, program1).size());
    Assert.assertEquals(0, store.getProperties(scope, jarEntity).size());
    Assert.assertEquals("dvalue1", store.getProperties(scope, dataset1).get("dkey1"));
    Assert.assertEquals(ImmutableMap.of("skey1", "svalue1", "skey2", "svalue2"), store.getProperties(scope, stream1));
    properties = store.getProperties(scope, artifact1);
    Assert.assertEquals(ImmutableMap.of("rkey1", "rvalue1", "rkey2", "rvalue2"), properties);
    Assert.assertEquals("rvalue2", store.getProperties(scope, artifact1).get("rkey2"));
    Assert.assertEquals("fvalue2", store.getProperties(scope, fileEntity).get("fkey2"));
    Assert.assertEquals("pfvalue2", store.getProperties(scope, partitionFileEntity).get("pfkey2"));

    // reset a property
    store.setProperties(scope, stream1, ImmutableMap.of("skey1", "sv1"));
    Assert.assertEquals(ImmutableMap.of("skey1", "sv1", "skey2", "svalue2"),
                        store.getProperties(scope, stream1));

    // cleanup
    store.removeProperties(scope, app1);
    store.removeProperties(scope, program1);
    store.removeProperties(scope, dataset1);
    store.removeProperties(scope, stream1);
    store.removeProperties(scope, artifact1);
    store.removeProperties(scope, fileEntity);
    store.removeProperties(scope, partitionFileEntity);

    Assert.assertEquals(0, store.getProperties(scope, app1).size());
    Assert.assertEquals(0, store.getProperties(scope, program1).size());
    Assert.assertEquals(0, store.getProperties(scope, dataset1).size());
    Assert.assertEquals(0, store.getProperties(scope, stream1).size());
    Assert.assertEquals(0, store.getProperties(scope, artifact1).size());
    Assert.assertEquals(0, store.getProperties(scope, fileEntity).size());
    Assert.assertEquals(0, store.getProperties(scope, partitionFileEntity).size());
    Assert.assertEquals(0, store.getProperties(scope, jarEntity).size());
  }

  @Test
  public void testTags() {
    MetadataScope scope = MetadataScope.SYSTEM;

    Assert.assertEquals(0, store.getTags(scope, app1).size());
    Assert.assertEquals(0, store.getTags(scope, program1).size());
    Assert.assertEquals(0, store.getTags(scope, dataset1).size());
    Assert.assertEquals(0, store.getTags(scope, stream1).size());
    Assert.assertEquals(0, store.getTags(scope, artifact1).size());
    Assert.assertEquals(0, store.getTags(scope, fileEntity).size());
    Assert.assertEquals(0, store.getTags(scope, partitionFileEntity).size());
    Assert.assertEquals(0, store.getTags(scope, jarEntity).size());

    store.addTags(scope, app1, ImmutableSet.of("tag1", "tag2", "tag3"));
    // MetadataChange metadataChange =
    store.addTags(scope, program1, Collections.emptySet());
    // Assert.assertEquals(metadataChange.getExisting(), new Metadata(program1, Collections.emptyMap(),
    //                                                                Collections.emptySet()));
    // Assert.assertEquals(metadataChange.getLatest(), new Metadata(program1, Collections.emptyMap(),
    //                                                              Collections.emptySet()));
    // metadataChange =
    store.addTags(scope, program1, ImmutableSet.of("tag1"));
    // assert the metadata change which happens on setting tag for the first time
    // Assert.assertEquals(new Metadata(program1), metadataChange.getExisting());
    // Assert.assertEquals(new Metadata(program1, Collections.emptyMap(), ImmutableSet.of("tag1")),
    //                     metadataChange.getLatest());
    // metadataChange =
    store.addTags(scope, program1, ImmutableSet.of("tag2"));
    // assert the metadata change which happens on setting tag when a tag exists
    // Assert.assertEquals(new Metadata(program1, Collections.emptyMap(), ImmutableSet.of("tag1")),
    //                     metadataChange.getExisting());
    // Assert.assertEquals(new Metadata(program1, Collections.emptyMap(), ImmutableSet.of("tag1", "tag2")),
    //                     metadataChange.getLatest());
    store.addTags(scope, dataset1, ImmutableSet.of("tag3", "tag2"));
    store.addTags(scope, stream1, ImmutableSet.of("tag2"));
    store.addTags(scope, artifact1, ImmutableSet.of("tag3"));
    store.addTags(scope, fileEntity, ImmutableSet.of("tag5"));
    store.addTags(scope, partitionFileEntity, ImmutableSet.of("tag6"));
    store.addTags(scope, jarEntity, ImmutableSet.of("tag5", "tag7"));

    Set<String> tags = store.getTags(scope, app1);
    Assert.assertEquals(3, tags.size());
    Assert.assertTrue(tags.contains("tag1"));
    Assert.assertTrue(tags.contains("tag2"));
    Assert.assertTrue(tags.contains("tag3"));

    Assert.assertEquals(3, store.getTags(scope, app1).size());
    // adding an existing tags should not be added
    store.addTags(scope, app1, ImmutableSet.of("tag2"));
    tags = store.getTags(scope, app1);
    Assert.assertEquals(3, tags.size());
    Assert.assertTrue(tags.contains("tag1"));
    Assert.assertTrue(tags.contains("tag2"));
    Assert.assertTrue(tags.contains("tag3"));

    // add the same tag again
    store.addTags(scope, app1, ImmutableSet.of("tag1"));
    Assert.assertEquals(3, store.getTags(scope, app1).size());
    tags = store.getTags(scope, program1);
    Assert.assertEquals(2, tags.size());
    Assert.assertTrue(tags.containsAll(ImmutableSet.of("tag1", "tag2")));
    tags = store.getTags(scope, dataset1);
    Assert.assertEquals(2, tags.size());
    Assert.assertTrue(tags.contains("tag3"));
    Assert.assertTrue(tags.contains("tag2"));
    tags = store.getTags(scope, stream1);
    Assert.assertEquals(1, tags.size());
    Assert.assertTrue(tags.contains("tag2"));
    tags = store.getTags(scope, fileEntity);
    Assert.assertEquals(1, tags.size());
    Assert.assertTrue(tags.contains("tag5"));
    tags = store.getTags(scope, partitionFileEntity);
    Assert.assertEquals(1, tags.size());
    Assert.assertTrue(tags.contains("tag6"));
    tags = store.getTags(scope, jarEntity);
    Assert.assertEquals(2, tags.size());
    Assert.assertTrue(tags.contains("tag5"));
    Assert.assertTrue(tags.contains("tag7"));
    store.removeTags(scope, app1, ImmutableSet.of("tag1", "tag2"));
    tags = store.getTags(scope, app1);
    Assert.assertEquals(1, tags.size());
    Assert.assertTrue(tags.contains("tag3"));
    store.removeTags(scope, dataset1, ImmutableSet.of("tag3"));
    store.removeTags(scope, jarEntity, ImmutableSet.of("tag5"));

    tags = store.getTags(scope, dataset1);
    Assert.assertEquals(1, tags.size());
    Assert.assertTrue(tags.contains("tag2"));
    tags = store.getTags(scope, artifact1);
    Assert.assertEquals(1, tags.size());
    Assert.assertTrue(tags.contains("tag3"));
    tags = store.getTags(scope, jarEntity);
    Assert.assertEquals(1, tags.size());
    Assert.assertTrue(tags.contains("tag7"));

    // cleanup
    store.removeTags(scope, app1);
    store.removeTags(scope, program1);
    store.removeTags(scope, dataset1);
    store.removeTags(scope, stream1);
    store.removeTags(scope, artifact1);
    store.removeTags(scope, fileEntity);
    store.removeTags(scope, partitionFileEntity);
    store.removeTags(scope, jarEntity);

    Assert.assertEquals(0, store.getTags(scope, app1).size());
    Assert.assertEquals(0, store.getTags(scope, program1).size());
    Assert.assertEquals(0, store.getTags(scope, dataset1).size());
    Assert.assertEquals(0, store.getTags(scope, stream1).size());
    Assert.assertEquals(0, store.getTags(scope, artifact1).size());
    Assert.assertEquals(0, store.getTags(scope, fileEntity).size());
    Assert.assertEquals(0, store.getTags(scope, partitionFileEntity).size());
    Assert.assertEquals(0, store.getTags(scope, jarEntity).size());
  }
}

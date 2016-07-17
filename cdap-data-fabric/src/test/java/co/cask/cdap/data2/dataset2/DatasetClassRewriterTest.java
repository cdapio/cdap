/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.data2.dataset2;

import co.cask.cdap.api.Predicate;
import co.cask.cdap.api.dataset.DataSetException;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.common.dataset.DatasetClassRewriter;
import co.cask.cdap.data2.dataset2.customds.CustomDatasetApp;
import co.cask.cdap.data2.dataset2.customds.CustomOperations;
import co.cask.cdap.data2.dataset2.customds.DefaultTopLevelExtendsDataset;
import co.cask.cdap.data2.dataset2.customds.TopLevelDataset;
import co.cask.cdap.data2.dataset2.customds.TopLevelDirectDataset;
import co.cask.cdap.data2.dataset2.customds.TopLevelExtendsDataset;
import co.cask.cdap.data2.metadata.lineage.AccessType;
import co.cask.cdap.internal.asm.ByteCodeClassLoader;
import co.cask.cdap.internal.asm.ClassDefinition;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.security.spi.authorization.AuthorizationEnforcer;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;
import org.objectweb.asm.Type;

import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import javax.annotation.Nullable;

/**
 * This test {@link DatasetClassRewriter} in cdap-common. It is here because it uses classes defined in
 * data-fabric.
 */
public class DatasetClassRewriterTest {

  @Test
  public void testDatasetACL() throws Exception {
    ByteCodeClassLoader classLoader = new ByteCodeClassLoader(getClass().getClassLoader());
    classLoader.addClass(rewrite(TopLevelExtendsDataset.class));
    classLoader.addClass(rewrite(TopLevelDirectDataset.class));
    classLoader.addClass(rewrite(TopLevelDataset.class));
    classLoader.addClass(rewrite(DefaultTopLevelExtendsDataset.class));
    classLoader.addClass(rewrite(CustomDatasetApp.InnerStaticInheritDataset.class));
    classLoader.addClass(rewrite(CustomDatasetApp.InnerDataset.class));

    InMemoryAccessRecorder accessRecorder = new InMemoryAccessRecorder();
    testDatasetMethods(accessRecorder, createDataset(accessRecorder, TopLevelDataset.class.getName(), classLoader));

    accessRecorder.clear();
    testDatasetMethods(accessRecorder,
                       createDataset(accessRecorder, DefaultTopLevelExtendsDataset.class.getName(), classLoader));

    accessRecorder.clear();
    testDatasetMethods(accessRecorder,
                       createDataset(accessRecorder, CustomDatasetApp.InnerStaticInheritDataset.class.getName(),
                                     classLoader));

    accessRecorder.clear();
    testDatasetMethods(accessRecorder,
                       createDataset(accessRecorder, CustomDatasetApp.InnerDataset.class.getName(), classLoader,
                                     new CustomDatasetApp()));
  }

  private <T extends Dataset & CustomOperations> T createDataset(InMemoryAccessRecorder accessRecorder,
                                                                 final String className,
                                                                 final ClassLoader classLoader,
                                                                 final Object...constructorParams) throws Exception {
    return DefaultDatasetRuntimeContext.execute(new AlwaysAllowAuthorizationEnforcer(),
                                                accessRecorder,
                                                new Principal("cdap", Principal.PrincipalType.USER),
                                                NamespaceId.DEFAULT.dataset("custom"), new Callable<T>() {
        @Override
        public T call() throws Exception {
          Class<?> cls = classLoader.loadClass(className);
          if (constructorParams.length == 0) {
            return (T) cls.newInstance();
          }
          Class<?>[] paramTypes = new Class<?>[constructorParams.length];
          for (int i = 0; i < constructorParams.length; i++) {
            paramTypes[i] = constructorParams[i].getClass();
          }
          return (T) cls.getConstructor(paramTypes).newInstance(constructorParams);
        }
      });
  }

  private <T extends Dataset & CustomOperations> void testDatasetMethods(InMemoryAccessRecorder accessRecorder,
                                                                         T dataset) throws Exception {
    dataset.read();
    dataset.write();
    dataset.readWrite();
    try {
      dataset.invalidRead();
      Assert.fail("Exception expected");
    } catch (Exception e) {
      Assert.assertTrue(Throwables.getRootCause(e) instanceof DataSetException);
    }
    try {
      dataset.invalidWrite();
      Assert.fail("Exception expected");
    } catch (Exception e) {
      Assert.assertTrue(Throwables.getRootCause(e) instanceof DataSetException);
    }
    try {
      dataset.invalidReadFromWriteOnly();
      Assert.fail("Exception expected");
    } catch (Exception e) {
      Assert.assertTrue(Throwables.getRootCause(e) instanceof DataSetException);
    }
    try {
      dataset.invalidWriteFromReadOnly();
      Assert.fail("Exception expected");
    } catch (Exception e) {
      Assert.assertTrue(Throwables.getRootCause(e) instanceof DataSetException);
    }

    dataset.close();

    // The access being recorded should be
    // unknown (constructor), read, write and read_write
    Assert.assertEquals(ImmutableList.of(AccessType.UNKNOWN, AccessType.READ, AccessType.WRITE, AccessType.READ_WRITE),
                        accessRecorder.getAccessRecorded());
  }

  private ClassDefinition rewrite(Class<? extends Dataset> dataset) throws Exception {
    DatasetClassRewriter rewriter = new DatasetClassRewriter();
    URL url = dataset.getClassLoader().getResource(dataset.getName().replace('.', '/') + ".class");
    Assert.assertNotNull(url);
    try (InputStream is = url.openStream()) {
      return new ClassDefinition(rewriter.rewriteClass(dataset.getName(), is), Type.getInternalName(dataset));
    }
  }

  private static final class AlwaysAllowAuthorizationEnforcer implements AuthorizationEnforcer {

    @Override
    public void enforce(EntityId entity, Principal principal, Action action) throws Exception {
      // no-op
    }

    @Override
    public void enforce(EntityId entity, Principal principal, Set<Action> actions) throws Exception {
      // no-op
    }

    @Override
    public Predicate<EntityId> createFilter(Principal principal) throws Exception {
      return new Predicate<EntityId>() {
        @Override
        public boolean apply(@Nullable EntityId input) {
          return true;
        }
      };
    }
  }

  private static final class InMemoryAccessRecorder implements DefaultDatasetRuntimeContext.DatasetAccessRecorder {

    // Use a list because in the unit-test we would like to verify there is no duplicated access information
    // being written from DefaultDatasetRuntimeContext
    private final List<AccessType> accessRecorded = new ArrayList<>();

    @Override
    public void recordAccess(AccessType accessType) {
      accessRecorded.add(accessType);
    }

    void clear() {
      accessRecorded.clear();
    }

    List<AccessType> getAccessRecorded() {
      return accessRecorded;
    }
  }
}

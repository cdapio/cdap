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
import co.cask.cdap.data2.dataset2.customds.DelegatingDataset;
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
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;
import org.objectweb.asm.Type;

import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.EnumSet;
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
  public void testDatasetAccessRecorder() throws Exception {
    ByteCodeClassLoader classLoader = new ByteCodeClassLoader(getClass().getClassLoader());
    classLoader.addClass(rewrite(TopLevelExtendsDataset.class));
    classLoader.addClass(rewrite(TopLevelDirectDataset.class));
    classLoader.addClass(rewrite(TopLevelDataset.class));
    classLoader.addClass(rewrite(DefaultTopLevelExtendsDataset.class));
    classLoader.addClass(rewrite(CustomDatasetApp.InnerStaticInheritDataset.class));
    classLoader.addClass(rewrite(CustomDatasetApp.InnerDataset.class));

    InMemoryAccessRecorder accessRecorder = new InMemoryAccessRecorder();
    TestAuthorizationEnforcer authEnforcer = new TestAuthorizationEnforcer(EnumSet.allOf(Action.class));
    testDatasetAccessRecord(accessRecorder,
                            createDataset(accessRecorder, authEnforcer, TopLevelDataset.class.getName(), classLoader));

    accessRecorder.clear();
    testDatasetAccessRecord(accessRecorder,
                            createDataset(accessRecorder, authEnforcer,
                                          DefaultTopLevelExtendsDataset.class.getName(), classLoader));

    accessRecorder.clear();
    Dataset delegate = createDataset(accessRecorder, authEnforcer, TopLevelDataset.class.getName(), classLoader);
    testDatasetAccessRecord(accessRecorder,
                            createDataset(accessRecorder, authEnforcer, DelegatingDataset.class.getName(), classLoader,
                                          new Class<?>[] { CustomOperations.class },
                                          new Object[] { delegate }));

    accessRecorder.clear();
    testDatasetAccessRecord(accessRecorder,
                            createDataset(accessRecorder, authEnforcer,
                                          CustomDatasetApp.InnerStaticInheritDataset.class.getName(), classLoader));

    accessRecorder.clear();
    testDatasetAccessRecord(accessRecorder,
                            createDataset(accessRecorder, authEnforcer,
                                          CustomDatasetApp.InnerDataset.class.getName(), classLoader,
                                          new Class<?>[] { CustomDatasetApp.class },
                                          new Object[] { new CustomDatasetApp()}));
  }

  @Test
  public void testDatasetAuthorization() throws Exception {
    ByteCodeClassLoader classLoader = new ByteCodeClassLoader(getClass().getClassLoader());
    classLoader.addClass(rewrite(TopLevelExtendsDataset.class));
    classLoader.addClass(rewrite(TopLevelDirectDataset.class));
    classLoader.addClass(rewrite(TopLevelDataset.class));
    classLoader.addClass(rewrite(DefaultTopLevelExtendsDataset.class));
    classLoader.addClass(rewrite(CustomDatasetApp.InnerStaticInheritDataset.class));
    classLoader.addClass(rewrite(CustomDatasetApp.InnerDataset.class));

    InMemoryAccessRecorder accessRecorder = new InMemoryAccessRecorder();

    // Test no access
    TestAuthorizationEnforcer authEnforcer = new TestAuthorizationEnforcer(EnumSet.noneOf(Action.class));
    testNoAccess(createDataset(accessRecorder, authEnforcer, TopLevelDataset.class.getName(), classLoader));
    testNoAccess(createDataset(accessRecorder, authEnforcer,
                               DefaultTopLevelExtendsDataset.class.getName(), classLoader));
    Dataset delegate = createDataset(accessRecorder, authEnforcer, TopLevelDataset.class.getName(), classLoader);
    testNoAccess(createDataset(accessRecorder, authEnforcer, DelegatingDataset.class.getName(), classLoader,
                               new Class<?>[] { CustomOperations.class }, new Object[] { delegate }));
    testNoAccess(createDataset(accessRecorder, authEnforcer,
                               CustomDatasetApp.InnerStaticInheritDataset.class.getName(), classLoader));
    testNoAccess(createDataset(accessRecorder, authEnforcer,
                               CustomDatasetApp.InnerDataset.class.getName(), classLoader,
                               new Class<?>[] { CustomDatasetApp.class }, new Object[] { new CustomDatasetApp()}));

    // Test read only access
    authEnforcer = new TestAuthorizationEnforcer(EnumSet.of(Action.READ));
    testReadOnlyAccess(createDataset(accessRecorder, authEnforcer, TopLevelDataset.class.getName(), classLoader));
    testReadOnlyAccess(createDataset(accessRecorder, authEnforcer,
                                     DefaultTopLevelExtendsDataset.class.getName(), classLoader));
    delegate = createDataset(accessRecorder, authEnforcer, TopLevelDataset.class.getName(), classLoader);
    testReadOnlyAccess(createDataset(accessRecorder, authEnforcer, DelegatingDataset.class.getName(), classLoader,
                                     new Class<?>[] { CustomOperations.class }, new Object[] { delegate }));
    testReadOnlyAccess(createDataset(accessRecorder, authEnforcer,
                                     CustomDatasetApp.InnerStaticInheritDataset.class.getName(), classLoader));
    testReadOnlyAccess(createDataset(accessRecorder, authEnforcer,
                                     CustomDatasetApp.InnerDataset.class.getName(), classLoader,
                                     new Class<?>[] { CustomDatasetApp.class },
                                     new Object[] { new CustomDatasetApp()}));

    // Test write only access
    authEnforcer = new TestAuthorizationEnforcer(EnumSet.of(Action.WRITE));
    testWriteOnlyAccess(createDataset(accessRecorder, authEnforcer, TopLevelDataset.class.getName(), classLoader));
    testWriteOnlyAccess(createDataset(accessRecorder, authEnforcer,
                                      DefaultTopLevelExtendsDataset.class.getName(), classLoader));
    delegate = createDataset(accessRecorder, authEnforcer, TopLevelDataset.class.getName(), classLoader);
    testWriteOnlyAccess(createDataset(accessRecorder, authEnforcer, DelegatingDataset.class.getName(), classLoader,
                                      new Class<?>[] { CustomOperations.class }, new Object[] { delegate }));
    testWriteOnlyAccess(createDataset(accessRecorder, authEnforcer,
                                      CustomDatasetApp.InnerStaticInheritDataset.class.getName(), classLoader));
    testWriteOnlyAccess(createDataset(accessRecorder, authEnforcer,
                                      CustomDatasetApp.InnerDataset.class.getName(), classLoader,
                                      new Class<?>[] { CustomDatasetApp.class },
                                      new Object[] { new CustomDatasetApp()}));
  }

  private <T extends Dataset & CustomOperations> T createDataset(InMemoryAccessRecorder accessRecorder,
                                                                 AuthorizationEnforcer authEnforcer,
                                                                 final String className,
                                                                 final ClassLoader classLoader) throws Exception {
    return createDataset(accessRecorder, authEnforcer, className, classLoader, new Class<?>[0], new Object[0]);
  }

  private <T extends Dataset & CustomOperations> T createDataset(InMemoryAccessRecorder accessRecorder,
                                                                 AuthorizationEnforcer authEnforcer,
                                                                 final String className,
                                                                 final ClassLoader classLoader,
                                                                 final Class<?>[] paramTypes,
                                                                 final Object[] constructorParams) throws Exception {
    return DefaultDatasetRuntimeContext.execute(authEnforcer,
                                                accessRecorder,
                                                new Principal("cdap", Principal.PrincipalType.USER),
                                                NamespaceId.DEFAULT.dataset("custom"), new Callable<T>() {
        @Override
        public T call() throws Exception {
          Class<?> cls = classLoader.loadClass(className);
          if (paramTypes.length == 0) {
            return (T) cls.newInstance();
          }
          return (T) cls.getConstructor(paramTypes).newInstance(constructorParams);
        }
      });
  }

  private <T extends Dataset & CustomOperations> void testDatasetAccessRecord(InMemoryAccessRecorder accessRecorder,
                                                                              T dataset) throws Exception {

    // Expect an unknown from the constructor
    Assert.assertEquals(ImmutableList.of(AccessType.UNKNOWN), accessRecorder.getLineageRecorded());
    Assert.assertEquals(ImmutableList.of(AccessType.UNKNOWN), accessRecorder.getAuditRecorded());

    accessRecorder.clear();
    dataset.noDataOp();
    // Since UNKNOWN is already record, expect no new lineage and audit being emitted
    Assert.assertTrue(accessRecorder.getLineageRecorded().isEmpty());
    Assert.assertTrue(accessRecorder.getAuditRecorded().isEmpty());


    accessRecorder.clear();
    dataset.lineageWriteActualReadWrite();
    // Expect an WRITE lineage, but a WRITE and READ audit
    Assert.assertEquals(ImmutableList.of(AccessType.WRITE), accessRecorder.getLineageRecorded());
    Assert.assertEquals(ImmutableList.of(AccessType.WRITE, AccessType.READ), accessRecorder.getAuditRecorded());

    accessRecorder.clear();
    dataset.read();
    dataset.write();
    dataset.readWrite();
    // Expect a READ, READ_WRITE for lineage and a READ_WRITE for audit.
    // This is because only UNKNOWN and WRITE were encountered for lineage,
    // while UNKNOW, READ and WRITE were encountered for audit
    Assert.assertEquals(ImmutableList.of(AccessType.READ, AccessType.READ_WRITE), accessRecorder.getLineageRecorded());
    Assert.assertEquals(ImmutableList.of(AccessType.READ_WRITE), accessRecorder.getAuditRecorded());

    dataset.close();
  }

  private <T extends Dataset & CustomOperations> void testNoAccess(T dataset) throws Exception {
    dataset.noDataOp();

    try {
      dataset.read();
      Assert.fail("Expected an DataSetException");
    } catch (DataSetException e) {
      Assert.assertTrue(e.getCause() instanceof UnauthorizedException);
    }

    try {
      dataset.write();
      Assert.fail("Expected an DataSetException");
    } catch (DataSetException e) {
      Assert.assertTrue(e.getCause() instanceof UnauthorizedException);
    }

    try {
      dataset.readWrite();
      Assert.fail("Expected an DataSetException");
    } catch (DataSetException e) {
      Assert.assertTrue(e.getCause() instanceof UnauthorizedException);
    }

    try {
      dataset.lineageWriteActualReadWrite();
      Assert.fail("Expected an DataSetException");
    } catch (DataSetException e) {
      Assert.assertTrue(e.getCause() instanceof UnauthorizedException);
    }
  }

  private <T extends Dataset & CustomOperations> void testReadOnlyAccess(T dataset) throws Exception {
    dataset.read();
    dataset.noDataOp();

    try {
      dataset.write();
      Assert.fail("Expected an DataSetException");
    } catch (DataSetException e) {
      Assert.assertTrue(e.getCause() instanceof UnauthorizedException);
    }

    try {
      dataset.readWrite();
      Assert.fail("Expected an DataSetException");
    } catch (DataSetException e) {
      Assert.assertTrue(e.getCause() instanceof UnauthorizedException);
    }

    try {
      dataset.lineageWriteActualReadWrite();
      Assert.fail("Expected an DataSetException");
    } catch (DataSetException e) {
      Assert.assertTrue(e.getCause() instanceof UnauthorizedException);
    }
  }

  private <T extends Dataset & CustomOperations> void testWriteOnlyAccess(T dataset) throws Exception {
    dataset.write();
    dataset.noDataOp();

    try {
      dataset.read();
      Assert.fail("Expected an DataSetException");
    } catch (DataSetException e) {
      Assert.assertTrue(e.getCause() instanceof UnauthorizedException);
    }

    try {
      dataset.readWrite();
      Assert.fail("Expected an DataSetException");
    } catch (DataSetException e) {
      Assert.assertTrue(e.getCause() instanceof UnauthorizedException);
    }

    try {
      // Even the method is annotated with @WriteOnly, it actually performs read operation, hence the failure
      dataset.lineageWriteActualReadWrite();
      Assert.fail("Expected an DataSetException");
    } catch (DataSetException e) {
      Assert.assertTrue(e.getCause() instanceof UnauthorizedException);
    }
  }

  private ClassDefinition rewrite(Class<? extends Dataset> dataset) throws Exception {
    DatasetClassRewriter rewriter = new DatasetClassRewriter();
    URL url = dataset.getClassLoader().getResource(dataset.getName().replace('.', '/') + ".class");
    Assert.assertNotNull(url);
    try (InputStream is = url.openStream()) {
      return new ClassDefinition(rewriter.rewriteClass(dataset.getName(), is), Type.getInternalName(dataset));
    }
  }

  private static final class TestAuthorizationEnforcer implements AuthorizationEnforcer {

    private final Set<Action> allowedActions;

    private TestAuthorizationEnforcer(Set<Action> allowedActions) {
      this.allowedActions = allowedActions;
    }

    @Override
    public void enforce(EntityId entity, Principal principal, Action action) throws Exception {
      if (!allowedActions.contains(action)) {
        throw new UnauthorizedException("Not allow to perform " + action + " " + entity + " by " + principal);
      }
    }

    @Override
    public void enforce(EntityId entity, Principal principal, Set<Action> actions) throws Exception {
      if (!allowedActions.containsAll(actions)) {
        throw new UnauthorizedException("Not allow to perform " + actions + " " + entity + " by " + principal);
      }
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
    private final List<AccessType> lineageRecorded = new ArrayList<>();
    private final List<AccessType> auditRecorded = new ArrayList<>();

    void clear() {
      lineageRecorded.clear();
      auditRecorded.clear();
    }

    List<AccessType> getLineageRecorded() {
      return lineageRecorded;
    }

    List<AccessType> getAuditRecorded() {
      return auditRecorded;
    }

    @Override
    public void recordLineage(AccessType accessType) {
      lineageRecorded.add(accessType);
    }

    @Override
    public void emitAudit(AccessType accessType) {
      auditRecorded.add(accessType);
    }
  }
}

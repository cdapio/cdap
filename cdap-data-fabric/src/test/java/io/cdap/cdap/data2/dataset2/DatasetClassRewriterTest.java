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

package io.cdap.cdap.data2.dataset2;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.api.annotation.ReadOnly;
import io.cdap.cdap.api.annotation.ReadWrite;
import io.cdap.cdap.api.annotation.WriteOnly;
import io.cdap.cdap.api.dataset.DataSetException;
import io.cdap.cdap.api.dataset.Dataset;
import io.cdap.cdap.common.dataset.DatasetClassRewriter;
import io.cdap.cdap.data2.dataset2.customds.CustomDatasetApp;
import io.cdap.cdap.data2.dataset2.customds.CustomOperations;
import io.cdap.cdap.data2.dataset2.customds.DefaultTopLevelExtendsDataset;
import io.cdap.cdap.data2.dataset2.customds.DelegatingDataset;
import io.cdap.cdap.data2.dataset2.customds.TopLevelDataset;
import io.cdap.cdap.data2.dataset2.customds.TopLevelDirectDataset;
import io.cdap.cdap.data2.dataset2.customds.TopLevelExtendsDataset;
import io.cdap.cdap.data2.metadata.lineage.AccessType;
import io.cdap.cdap.internal.asm.ByteCodeClassLoader;
import io.cdap.cdap.internal.asm.ClassDefinition;
import io.cdap.cdap.proto.element.EntityType;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.security.Authorizable;
import io.cdap.cdap.proto.security.GrantedPermission;
import io.cdap.cdap.proto.security.Permission;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.proto.security.StandardPermission;
import io.cdap.cdap.security.spi.authorization.AccessEnforcer;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import javax.annotation.Nullable;
import org.junit.Assert;
import org.junit.Test;
import org.objectweb.asm.Type;

/**
 * This test {@link DatasetClassRewriter} in cdap-common. It is here because it uses classes defined in
 * data-fabric.
 */
public class DatasetClassRewriterTest {

  private static final DatasetId DATASET_ID = NamespaceId.DEFAULT.dataset("custom");

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
    TestAccessEnforcer authEnforcer = new TestAccessEnforcer(EnumSet.allOf(StandardPermission.class));
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
    TestAccessEnforcer authEnforcer = new TestAccessEnforcer(EnumSet.noneOf(StandardPermission.class));
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
    authEnforcer = new TestAccessEnforcer(EnumSet.of(StandardPermission.GET));
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
    authEnforcer = new TestAccessEnforcer(EnumSet.of(StandardPermission.UPDATE));
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

  @Test
  public void testConstructorDefaultAnnotation() throws Exception {
    ByteCodeClassLoader classLoader = new ByteCodeClassLoader(getClass().getClassLoader());
    classLoader.addClass(rewrite(TopLevelDirectDataset.class));

    InMemoryAccessRecorder accessRecorder = new InMemoryAccessRecorder();
    AuthorizationRecorder authorizationRecorder = new AuthorizationRecorder();

    // Test constructor no default
    createDataset(accessRecorder, authorizationRecorder, TopLevelDirectDataset.class.getName(), classLoader,
                  new Class<?>[0], new Object[0], null);
    Assert.assertEquals(ImmutableList.of(AccessType.UNKNOWN), accessRecorder.getLineageRecorded());
    Assert.assertEquals(ImmutableList.of(AccessType.UNKNOWN), accessRecorder.getAuditRecorded());
    Assert.assertEquals(1, authorizationRecorder.getGrantedPermissions().size());
    // Expects the enforcer still get called
    Assert.assertNull(authorizationRecorder.getGrantedPermissions().get(0));

    accessRecorder.clear();
    authorizationRecorder.clear();

    // Test constructor default ReadOnly
    createDataset(accessRecorder, authorizationRecorder, TopLevelDirectDataset.class.getName(), classLoader,
                  new Class<?>[0], new Object[0], ReadOnly.class);
    Assert.assertEquals(ImmutableList.of(AccessType.READ), accessRecorder.getLineageRecorded());
    Assert.assertEquals(ImmutableList.of(AccessType.READ), accessRecorder.getAuditRecorded());
    Assert.assertEquals(ImmutableList.of(new GrantedPermission(DATASET_ID, StandardPermission.GET)),
                        authorizationRecorder.getGrantedPermissions());

    accessRecorder.clear();
    authorizationRecorder.clear();

    // Test constructor default WriteOnly
    createDataset(accessRecorder, authorizationRecorder, TopLevelDirectDataset.class.getName(), classLoader,
                  new Class<?>[0], new Object[0], WriteOnly.class);
    Assert.assertEquals(ImmutableList.of(AccessType.WRITE), accessRecorder.getLineageRecorded());
    Assert.assertEquals(ImmutableList.of(AccessType.WRITE), accessRecorder.getAuditRecorded());
    Assert.assertEquals(ImmutableList.of(new GrantedPermission(DATASET_ID, StandardPermission.UPDATE)),
                        authorizationRecorder.getGrantedPermissions());

    accessRecorder.clear();
    authorizationRecorder.clear();

    // Test constructor default ReadWrite
    createDataset(accessRecorder, authorizationRecorder, TopLevelDirectDataset.class.getName(), classLoader,
                  new Class<?>[0], new Object[0], ReadWrite.class);
    Assert.assertEquals(ImmutableList.of(AccessType.READ_WRITE), accessRecorder.getLineageRecorded());
    Assert.assertEquals(ImmutableList.of(AccessType.READ_WRITE), accessRecorder.getAuditRecorded());
    Assert.assertTrue(ImmutableSet.of(new GrantedPermission(DATASET_ID, StandardPermission.GET),
                                      new GrantedPermission(DATASET_ID, StandardPermission.UPDATE))
                        .containsAll(authorizationRecorder.getGrantedPermissions()));
  }

  private <T extends Dataset & CustomOperations> T createDataset(InMemoryAccessRecorder accessRecorder,
                                                                 AccessEnforcer authEnforcer,
                                                                 final String className,
                                                                 final ClassLoader classLoader) throws Exception {
    return createDataset(accessRecorder, authEnforcer, className, classLoader, new Class<?>[0], new Object[0]);
  }

  private <T extends Dataset & CustomOperations> T createDataset(InMemoryAccessRecorder accessRecorder,
                                                                 AccessEnforcer authEnforcer,
                                                                 final String className,
                                                                 final ClassLoader classLoader,
                                                                 final Class<?>[] paramTypes,
                                                                 final Object[] constructorParams) throws Exception {
    return createDataset(accessRecorder, authEnforcer, className, classLoader, paramTypes, constructorParams, null);
  }

  private <T extends Dataset & CustomOperations> T createDataset(InMemoryAccessRecorder accessRecorder,
                                                                 AccessEnforcer authEnforcer,
                                                                 final String className,
                                                                 final ClassLoader classLoader,
                                                                 final Class<?>[] paramTypes,
                                                                 final Object[] constructorParams,
                                                                 @Nullable Class<? extends Annotation> defaultAnno)
    throws Exception {

    return DefaultDatasetRuntimeContext.execute(authEnforcer,
                                                accessRecorder,
                                                new Principal("cdap", Principal.PrincipalType.USER),
                                                DATASET_ID, defaultAnno, new Callable<T>() {
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

  private static final class TestAccessEnforcer implements AccessEnforcer {

    private final Set<? extends Permission> allowedPermissions;

    private TestAccessEnforcer(Set<? extends Permission> allowedPermissions) {
      this.allowedPermissions = allowedPermissions;
    }

    @Override
    public void enforce(EntityId entity, Principal principal, Permission permission) throws UnauthorizedException {
      if (!allowedPermissions.contains(permission)) {
        throw new UnauthorizedException("Not allow to perform " + permission + " " + entity + " by " + principal);
      }
    }

    @Override
    public void enforce(EntityId entity, Principal principal, Set<? extends Permission> permissions)
      throws UnauthorizedException {
      if (!allowedPermissions.containsAll(permissions)) {
        throw new UnauthorizedException("Not allow to perform " + permissions + " " + entity + " by " + principal);
      }
    }

    @Override
    public void enforceOnParent(EntityType entityType, EntityId parentId, Principal principal, Permission permission)
      throws UnauthorizedException {
      if (!allowedPermissions.contains(permission)) {
        throw new UnauthorizedException("Not allow to perform " + permission + " " + parentId + "/" + entityType
                                          + " by " + principal);
      }
    }

    @Override
    public Set<? extends EntityId> isVisible(Set<? extends EntityId> entityIds, Principal principal) {
      return entityIds;
    }
  }

  private static final class AuthorizationRecorder implements AccessEnforcer {

    private final List<GrantedPermission> grantedPermissions = new ArrayList<>();

    @Override
    public void enforce(EntityId entity, Principal principal, Permission permission) {
      enforce(entity, principal, Collections.singleton(permission));
    }

    @Override
    public void enforce(EntityId entity, Principal principal, Set<? extends Permission> permissions) {
      if (permissions.isEmpty()) {
        // Put a null to the list so that we know this method has been called.
        grantedPermissions.add(null);
        return;
      }
      for (Permission permission : permissions) {
        grantedPermissions.add(new GrantedPermission(entity, permission));
      }
    }

    @Override
    public void enforceOnParent(EntityType entityType, EntityId parentId, Principal principal, Permission permission) {
      grantedPermissions.add(new GrantedPermission(Authorizable.fromEntityId(parentId, entityType), permission));
    }

    @Override
    public Set<? extends EntityId> isVisible(Set<? extends EntityId> entityIds, Principal principal) {
      return entityIds;
    }

    List<GrantedPermission> getGrantedPermissions() {
      return grantedPermissions;
    }

    void clear() {
      grantedPermissions.clear();
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

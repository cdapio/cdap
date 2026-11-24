/*
 * Copyright © 2015-2017 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.artifact;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.ByteStreams;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.artifact.ApplicationClass;
import io.cdap.cdap.api.artifact.ArtifactClasses;
import io.cdap.cdap.api.dataset.lib.KeyValueTable;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.api.plugin.PluginPropertyField;
import io.cdap.cdap.api.plugin.Requirements;
import io.cdap.cdap.app.program.ManifestFields;
import io.cdap.cdap.common.InvalidArtifactException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.common.test.AppJarHelper;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.internal.app.runtime.artifact.app.InvalidConfigApp;
import io.cdap.cdap.internal.app.runtime.artifact.app.inspection.InspectionApp;
import io.cdap.cdap.internal.app.runtime.artifact.plugin.nested.NestedConfigPlugin;
import io.cdap.cdap.internal.io.ReflectionSchemaGenerator;
import io.cdap.cdap.security.impersonation.DefaultImpersonator;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.objectweb.asm.AnnotationVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

/**
 *
 */
public class DefaultArtifactInspectorTest {
  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

  private static ArtifactClassLoaderFactory classLoaderFactory;
  private static DefaultArtifactInspector artifactInspector;

  @BeforeClass
  public static void setup() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TMP_FOLDER.newFolder().getAbsolutePath());

    classLoaderFactory = new ArtifactClassLoaderFactory(cConf);
    artifactInspector = new DefaultArtifactInspector(cConf, classLoaderFactory,
                                                     new DefaultImpersonator(cConf, null));
  }

  @Test(expected = InvalidArtifactException.class)
  @Ignore
  // TODO: (CDAP-16919) Re-enable this test once schema mapping for Java Object is fixed.
  public void testInvalidConfigApp() throws Exception {
    Manifest manifest = new Manifest();
    File appFile =
      createJar(InvalidConfigApp.class, new File(TMP_FOLDER.newFolder(), "InvalidConfigApp-1.0.0.jar"), manifest);

    Id.Artifact artifactId = Id.Artifact.from(Id.Namespace.DEFAULT, "InvalidConfigApp", "1.0.0");
    Location artifactLocation = Locations.toLocation(appFile);
    List<ArtifactDescriptor> parentDescriptor = new ArrayList<>();
    parentDescriptor.add(new ArtifactDescriptor(artifactId.getNamespace().getId(),
                                                artifactId.toArtifactId(), artifactLocation));
    artifactInspector.inspectArtifact(artifactId, appFile, parentDescriptor, Collections.emptySet());
  }

  @Test
  public void testGetPluginRequirements() {
    // check that if a plugin does not specify requirement annotation then it has empty requirements
    Assert.assertTrue(artifactInspector.getArtifactRequirements(InspectionApp.AppPlugin.class).isEmpty());

    // check that if a plugin specify a requirement it is captured
    Assert.assertEquals(new Requirements(ImmutableSet.of(Table.TYPE)),
                        artifactInspector.getArtifactRequirements(InspectionApp.SingleRequirementPlugin.class));

    // check if a plugin specify a requirement annotation but it is empty the requirement captured is no requirement
    Assert
      .assertTrue(artifactInspector.getArtifactRequirements(InspectionApp.EmptyRequirementPlugin.class).isEmpty());

    // check if a plugin specify multiple requirement all of them are captured
    Assert.assertEquals(new Requirements(ImmutableSet.of(Table.TYPE, KeyValueTable.TYPE)),
                        artifactInspector.getArtifactRequirements(InspectionApp.MultipleRequirementsPlugin.class));

    // check if a plugin has specified empty string as requirement is captured as no requirements
    Assert.assertTrue(artifactInspector
                        .getArtifactRequirements(InspectionApp.SingleEmptyRequirementPlugin.class).isEmpty());

    // check if a plugin has specified empty string with a valid requirement only the valid requirement is captured
    Assert.assertEquals(new Requirements(ImmutableSet.of(Table.TYPE)),
                        artifactInspector
                          .getArtifactRequirements(InspectionApp.ValidAndEmptyRequirementsPlugin.class));

    // test that duplicate requirements are only stored once and the beginning and ending white spaces are trimmed
    Assert.assertEquals(new Requirements(ImmutableSet.of(Table.TYPE, "duplicate")),
                        artifactInspector.getArtifactRequirements(InspectionApp.DuplicateRequirementsPlugin.class));

    //Test that capabilities in the Requirements annotation is being captured
    Assert.assertEquals(new Requirements(ImmutableSet.of(), ImmutableSet.of("cdc")),
                        artifactInspector.getArtifactRequirements(InspectionApp.CapabilityPlugin.class));
    Assert.assertEquals(new Requirements(ImmutableSet.of(), ImmutableSet.of("cdc", "healthcare")),
                        artifactInspector.getArtifactRequirements(InspectionApp.MultipleCapabilityPlugin.class));
    Assert.assertEquals(new Requirements(ImmutableSet.of(Table.TYPE, "sometype"), ImmutableSet.of("cdc", "healthcare")),
                        artifactInspector.getArtifactRequirements(InspectionApp.DatasetAndCapabilityPlugin.class));
  }

  @Test
  public void inspectAppsAndPlugins() throws Exception {
    File appFile = getAppFile();
    Id.Artifact artifactId = Id.Artifact.from(Id.Namespace.DEFAULT, "InspectionApp", "1.0.0");
    Location artifactLocation = Locations.toLocation(appFile);
    List<ArtifactDescriptor> parentDescriptor = new ArrayList<>();
    parentDescriptor.add(new ArtifactDescriptor(artifactId.getNamespace().getId(),
                                                artifactId.toArtifactId(), artifactLocation));
    ArtifactClasses classes = artifactInspector.inspectArtifact(artifactId, appFile, parentDescriptor,
                                                                Collections.emptySet()).getArtifactClasses();

    // check app classes
    Set<ApplicationClass> expectedApps = ImmutableSet.of(new ApplicationClass(
      InspectionApp.class.getName(), "", new ReflectionSchemaGenerator(false).generate(InspectionApp.AConfig.class),
      new Requirements(Collections.emptySet(), Collections.singleton("cdc"))));
    Assert.assertEquals(expectedApps, classes.getApps());

    // check plugin classes
    PluginClass expectedPlugin =
      PluginClass.builder().setName(InspectionApp.PLUGIN_NAME).setType(InspectionApp.PLUGIN_TYPE)
        .setDescription(InspectionApp.PLUGIN_DESCRIPTION).setClassName(InspectionApp.AppPlugin.class.getName())
        .setConfigFieldName("pluginConf").setProperties(ImmutableMap.of(
        "y", new PluginPropertyField("y", "", "double", true, true),
        "isSomething", new PluginPropertyField("isSomething", "", "boolean", true, false))).build();

    PluginClass multipleRequirementPlugin = PluginClass.builder()
      .setName(InspectionApp.MULTIPLE_REQUIREMENTS_PLUGIN)
      .setType(InspectionApp.PLUGIN_TYPE)
      .setCategory(InspectionApp.PLUGIN_CATEGORY)
      .setClassName(InspectionApp.MultipleRequirementsPlugin.class.getName())
      .setConfigFieldName("pluginConf")
      .setProperties(ImmutableMap.of(
        "y", new PluginPropertyField("y", "", "double", true, true),
        "isSomething", new PluginPropertyField("isSomething", "", "boolean", true, false)))
      .setRequirements(new Requirements(ImmutableSet.of(Table.TYPE, KeyValueTable.TYPE)))
      .setDescription(InspectionApp.PLUGIN_DESCRIPTION)
      .build();
    Assert.assertTrue(classes.getPlugins().containsAll(ImmutableSet.of(expectedPlugin, multipleRequirementPlugin)));
  }

  @Test
  public void testInspectNestedConfigPlugin() throws Exception {
    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(ManifestFields.EXPORT_PACKAGE, NestedConfigPlugin.class.getPackage().getName());
    File artifactFile = createJar(NestedConfigPlugin.class, new File(TMP_FOLDER.newFolder(), "NestedPlugin-1.0.0.jar"),
                                  manifest);
    Id.Artifact artifactId = Id.Artifact.from(Id.Namespace.DEFAULT, "NestedPlugin", "1.0.0");
    Location artifactLocation = Locations.toLocation(artifactFile);
    List<ArtifactDescriptor> parentDescriptor = new ArrayList<>();
    parentDescriptor.add(new ArtifactDescriptor(artifactId.getNamespace().getId(),
                                                artifactId.toArtifactId(), artifactLocation));
    ArtifactClasses classes = artifactInspector.inspectArtifact(artifactId, artifactFile, parentDescriptor,
                                                                Collections.emptySet()).getArtifactClasses();
    Set<PluginClass> plugins = classes.getPlugins();

    Map<String, PluginPropertyField> expectedFields = ImmutableMap.of(
      "X", new PluginPropertyField("X", "", "int", true, false),
      "Nested", new PluginPropertyField("Nested", "", "nestedconfig", true, true, false,
                                        ImmutableSet.of("Nested1", "Nested2")),
      "Nested1", new PluginPropertyField("Nested1", "", "string", true, true),
      "Nested2", new PluginPropertyField("Nested2", "", "string", true, true)
    );

    PluginClass expected = PluginClass.builder()
      .setName("nested").setType("dummy").setDescription("Nested config")
      .setClassName(NestedConfigPlugin.class.getName()).setConfigFieldName("config")
      .setProperties(expectedFields).build();

    Assert.assertEquals(Collections.singleton(expected), plugins);
  }

  @Test(expected = InvalidArtifactException.class)
  public void inspectAdditionaPluginClasses() throws Exception {
    File artifactFile = createJar(InspectionApp.class, new File(TMP_FOLDER.newFolder(), "InspectionApp-1.0.0.jar"),
                                  new Manifest());
    Id.Artifact artifactId = Id.Artifact.from(Id.Namespace.DEFAULT, "InspectionApp", "1.0.0");
    Location artifactLocation = Locations.toLocation(artifactFile);
    List<ArtifactDescriptor> parentDescriptor = new ArrayList<>();
    parentDescriptor.add(new ArtifactDescriptor(artifactId.getNamespace().getId(),
                                                artifactId.toArtifactId(), artifactLocation));
      // PluginClass contains a non existing classname that is not present in the artifact jar being used
      PluginClass pluginClass =
        PluginClass.builder().setName("plugin_name").setType("plugin_type")
          .setDescription("").setClassName("non-existing-class")
          .setConfigFieldName("pluginConf").setProperties(ImmutableMap.of()).build();
      // Inspects the jar and ensures that additional plugin classes can be loaded from the artifact jar
      artifactInspector.inspectArtifact(artifactId, artifactFile, parentDescriptor,
                                        ImmutableSet.of(pluginClass));
  }

  @Test
  public void testGetClassNameCheckPredicate() {
    Assert.assertTrue(DefaultArtifactInspector.getClassNameCheckPredicate(ImmutableList.of(
      "my.package"
    )).test("my/package/my.class"));
    Assert.assertTrue(DefaultArtifactInspector.getClassNameCheckPredicate(ImmutableList.of(
      "blah", "my.package", "blah2"
    )).test("my/package/my.class"));
    Assert.assertTrue(DefaultArtifactInspector.getClassNameCheckPredicate(ImmutableList.of(
      "my.package", "my.package.subpackage"
    )).test("my/package/subpackage/my.class"));
    Assert.assertFalse(DefaultArtifactInspector.getClassNameCheckPredicate(ImmutableList.of(
      "my.package"
    )).test("my/package/subpackage/my.class"));
    Assert.assertFalse(DefaultArtifactInspector.getClassNameCheckPredicate(ImmutableList.of(
      "my.package"
    )).test("prefix/my/package/my.class"));
    Assert.assertFalse(DefaultArtifactInspector.getClassNameCheckPredicate(ImmutableList.of(
      "my.package"
    )).test("prefix/my/package/my.notclass"));
    Assert.assertFalse(DefaultArtifactInspector.getClassNameCheckPredicate(ImmutableList.of(
      "my.package"
    )).test("prefix/my/package/my.class/some.class"));
  }

  @Test
  public void testInspectPluginWithMissingSuperclassPropagatesNoClassDefFoundError()
      throws Exception {
    File incompleteJar = createClassOnlyJar(
        new File(TMP_FOLDER.newFolder(), "BrokenPlugin-1.0.0.jar"),
        BrokenPluginSubclass.class.getPackage().getName(),
        BrokenPluginSubclass.class);

    Id.Artifact artifactId = Id.Artifact.from(Id.Namespace.DEFAULT, "BrokenPlugin", "1.0.0");
    InvalidArtifactException thrown = Assert.assertThrows(
        InvalidArtifactException.class,
        () -> artifactInspector.inspectArtifact(
            artifactId, incompleteJar, Collections.emptyList(), Collections.emptySet()));
    Assert.assertTrue(
        "Expected NoClassDefFoundError cause, but got: " + thrown.getCause(),
        thrown.getCause() instanceof NoClassDefFoundError);
  }

  @Test
  public void testInspectPluginFiltersNonExportedParentClassesAcrossParents() throws Exception {
    File incompleteJar = createClassOnlyJar(
        new File(TMP_FOLDER.newFolder(), "BrokenChildPlugin-1.0.0.jar"),
        BrokenPluginSubclass.class.getPackage().getName(),
        BrokenPluginSubclass.class);
    File parentJar = createClassOnlyJar(
        new File(TMP_FOLDER.newFolder(), "ParentWithPrivateHelper-1.0.0.jar"),
        InspectionApp.class.getPackage().getName(),
        InspectionApp.class, OmittedPluginSuperclass.class);

    Id.Artifact childId = Id.Artifact.from(Id.Namespace.DEFAULT, "BrokenChildPlugin", "1.0.0");
    Id.Artifact parentId = Id.Artifact.from(Id.Namespace.DEFAULT, "ParentWithPrivateHelper", "1.0.0");
    List<ArtifactDescriptor> parentDescriptor = Collections.singletonList(
        new ArtifactDescriptor(parentId.getNamespace().getId(), parentId.toArtifactId(),
            Locations.toLocation(parentJar)));

    InvalidArtifactException thrown = Assert.assertThrows(
        InvalidArtifactException.class,
        () -> artifactInspector.inspectArtifact(
            childId, incompleteJar, parentDescriptor, Collections.emptySet()));
    Assert.assertTrue(
        "Expected NoClassDefFoundError cause, but got: " + thrown.getCause(),
        thrown.getCause() instanceof NoClassDefFoundError);

    // Multi-parent inspection must still expose Export-Package classes from intermediate parent plugins
    File childUsingMidExportJar = createClassOnlyJar(
        new File(TMP_FOLDER.newFolder(), "ChildUsingMidExport-1.0.0.jar"),
        ChildUsingMidExportPlugin.class.getPackage().getName(),
        ChildUsingMidExportPlugin.class);
    File nestedExportParentJar = createClassOnlyJar(
        new File(TMP_FOLDER.newFolder(), "MidPlugin-1.0.0.jar"),
        NestedConfigPlugin.class.getPackage().getName(),
        NestedConfigPlugin.class, NestedConfigPlugin.NestedConfig.class);
    Id.Artifact midId = Id.Artifact.from(Id.Namespace.DEFAULT, "MidPlugin", "1.0.0");
    List<ArtifactDescriptor> multiParents = ImmutableList.of(
        new ArtifactDescriptor(midId.getNamespace().getId(), midId.toArtifactId(),
            Locations.toLocation(nestedExportParentJar)),
        new ArtifactDescriptor(parentId.getNamespace().getId(), parentId.toArtifactId(),
            Locations.toLocation(parentJar)));
    ArtifactClasses inspected = artifactInspector.inspectArtifact(
        Id.Artifact.from(Id.Namespace.DEFAULT, "ChildUsingMidExport", "1.0.0"),
        childUsingMidExportJar, multiParents, Collections.emptySet()).getArtifactClasses();
    Assert.assertTrue(
        inspected.getPlugins().stream().anyMatch(p -> "childMidExport".equals(p.getName())));
  }

  @Test
  public void testTwoHopPluginHierarchyIsolatesIntermediatePluginFromRootLibs() throws Exception {
    // 1. Transitive non-exported class in root artifact must not leak into intermediate parent plugin
    File rootJar = createClassOnlyJar(
        new File(TMP_FOLDER.newFolder(), "RootWithPrivateHelper-1.0.0.jar"),
        InspectionApp.class.getPackage().getName(),
        InspectionApp.class, OmittedPluginSuperclass.class);
    Id.Artifact rootId = Id.Artifact.from(Id.Namespace.DEFAULT, "RootWithPrivateHelper", "1.0.0");
    ArtifactDescriptor rootDesc = new ArtifactDescriptor(
        rootId.getNamespace().getId(), rootId.toArtifactId(), Locations.toLocation(rootJar));

    String midBrokenConfigInternal = "com/example/midbroken/MidBrokenConfig";
    byte[] midBrokenConfigBytes = generateEmptyClassBytes(
        Opcodes.ACC_PUBLIC | Opcodes.ACC_SUPER,
        midBrokenConfigInternal,
        OmittedPluginSuperclass.class.getName().replace('.', '/'),
        null, null);
    File midBrokenJar = createCustomBundleJar(
        new File(TMP_FOLDER.newFolder(), "MidBroken-1.0.0.jar"),
        "com.example.midbroken",
        ImmutableMap.of(midBrokenConfigInternal + ".class", midBrokenConfigBytes),
        Collections.emptyMap());
    Id.Artifact midBrokenId = Id.Artifact.from(Id.Namespace.DEFAULT, "MidBroken", "1.0.0");
    ArtifactDescriptor midBrokenDesc = new ArtifactDescriptor(
        midBrokenId.getNamespace().getId(), midBrokenId.toArtifactId(),
        Locations.toLocation(midBrokenJar));

    String brokenGrandchildInternal = "com/example/grandchildbroken/BrokenGrandchildPlugin";
    byte[] brokenGrandchildBytes = generatePluginClassBytes(
        brokenGrandchildInternal, "brokenTwoHopChild", "L" + midBrokenConfigInternal + ";");
    File brokenGrandchildJar = createCustomBundleJar(
        new File(TMP_FOLDER.newFolder(), "BrokenGrandchild-1.0.0.jar"),
        "com.example.grandchildbroken",
        ImmutableMap.of(brokenGrandchildInternal + ".class", brokenGrandchildBytes),
        Collections.emptyMap());
    Id.Artifact brokenGrandchildId =
        Id.Artifact.from(Id.Namespace.DEFAULT, "BrokenGrandchild", "1.0.0");

    InvalidArtifactException leakThrown = Assert.assertThrows(
        InvalidArtifactException.class,
        () -> artifactInspector.inspectArtifact(
            brokenGrandchildId, brokenGrandchildJar,
            ImmutableList.of(midBrokenDesc, rootDesc), Collections.emptySet()));
    Assert.assertTrue(
        "Expected NoClassDefFoundError cause, but got: " + leakThrown.getCause(),
        leakThrown.getCause() instanceof NoClassDefFoundError);

    // 2. Bundled lib/*.jar in root artifact (e.g. Guava 32) must not leak parent-first into
    //    an intermediate parent plugin that bundles its own incompatible version in lib/*.jar.
    String conflictDepInternal = "com/example/shaded/ConflictDep";
    // Root's lib/root-dep.jar defines ConflictDep as final (incompatible if loaded by MidPlugin)
    byte[] rootConflictDepBytes = generateEmptyClassBytes(
        Opcodes.ACC_PUBLIC | Opcodes.ACC_FINAL | Opcodes.ACC_SUPER,
        conflictDepInternal, "java/lang/Object", null, null);
    File rootDepJar = createCustomBundleJar(
        new File(TMP_FOLDER.newFolder(), "root-dep.jar"), null,
        ImmutableMap.of(conflictDepInternal + ".class", rootConflictDepBytes),
        Collections.emptyMap());
    File rootWithLibJar = createCustomBundleJar(
        new File(TMP_FOLDER.newFolder(), "RootWithShadedLib-1.0.0.jar"),
        InspectionApp.class.getPackage().getName(),
        Collections.emptyMap(),
        ImmutableMap.of("lib/root-dep.jar", rootDepJar));
    Id.Artifact rootLibId = Id.Artifact.from(Id.Namespace.DEFAULT, "RootWithShadedLib", "1.0.0");
    ArtifactDescriptor rootLibDesc = new ArtifactDescriptor(
        rootLibId.getNamespace().getId(), rootLibId.toArtifactId(),
        Locations.toLocation(rootWithLibJar));

    // MidPlugin's lib/mid-dep.jar defines non-final ConflictDep extending PluginConfig
    byte[] midConflictDepBytes = generateEmptyClassBytes(
        Opcodes.ACC_PUBLIC | Opcodes.ACC_SUPER,
        conflictDepInternal, PluginConfig.class.getName().replace('.', '/'),
        "midShadedProp", "Ljava/lang/String;");
    File midDepJar = createCustomBundleJar(
        new File(TMP_FOLDER.newFolder(), "mid-dep.jar"), null,
        ImmutableMap.of(conflictDepInternal + ".class", midConflictDepBytes),
        Collections.emptyMap());
    String midConfigInternal = "com/example/mid/MidExportedConfig";
    byte[] midConfigBytes = generateEmptyClassBytes(
        Opcodes.ACC_PUBLIC | Opcodes.ACC_SUPER,
        midConfigInternal, conflictDepInternal,
        "midOwnProp", "Ljava/lang/String;");
    File midWithLibJar = createCustomBundleJar(
        new File(TMP_FOLDER.newFolder(), "MidWithOwnLib-1.0.0.jar"),
        "com.example.mid",
        ImmutableMap.of(midConfigInternal + ".class", midConfigBytes),
        ImmutableMap.of("lib/mid-dep.jar", midDepJar));
    Id.Artifact midLibId = Id.Artifact.from(Id.Namespace.DEFAULT, "MidWithOwnLib", "1.0.0");
    ArtifactDescriptor midLibDesc = new ArtifactDescriptor(
        midLibId.getNamespace().getId(), midLibId.toArtifactId(),
        Locations.toLocation(midWithLibJar));

    String grandchildInternal = "com/example/grandchild/TwoHopChildPlugin";
    byte[] grandchildBytes = generatePluginClassBytes(
        grandchildInternal, "twoHopChild", "L" + midConfigInternal + ";");
    File grandchildJar = createCustomBundleJar(
        new File(TMP_FOLDER.newFolder(), "TwoHopChild-1.0.0.jar"),
        "com.example.grandchild",
        ImmutableMap.of(grandchildInternal + ".class", grandchildBytes),
        Collections.emptyMap());
    Id.Artifact grandchildId = Id.Artifact.from(Id.Namespace.DEFAULT, "TwoHopChild", "1.0.0");

    ArtifactClasses twoHopClasses = artifactInspector.inspectArtifact(
        grandchildId, grandchildJar,
        ImmutableList.of(midLibDesc, rootLibDesc), Collections.emptySet()).getArtifactClasses();
    PluginClass childPlugin = twoHopClasses.getPlugins().stream()
        .filter(p -> "twoHopChild".equals(p.getName()))
        .findFirst()
        .orElseThrow(() -> new AssertionError("Expected twoHopChild plugin to be inspected"));
    Assert.assertTrue(childPlugin.getProperties().containsKey("midShadedProp"));
    Assert.assertTrue(childPlugin.getProperties().containsKey("midOwnProp"));
  }

  private static byte[] generateEmptyClassBytes(int access, String internalName,
      String superInternalName, String optionalFieldName, String optionalFieldDesc) {
    ClassWriter cw = new ClassWriter(0);
    cw.visit(Opcodes.V1_8, access, internalName, null, superInternalName, null);
    if (optionalFieldName != null) {
      cw.visitField(Opcodes.ACC_PRIVATE, optionalFieldName, optionalFieldDesc, null, null)
          .visitEnd();
    }
    MethodVisitor mv = cw.visitMethod(Opcodes.ACC_PUBLIC, "<init>", "()V", null, null);
    mv.visitCode();
    mv.visitVarInsn(Opcodes.ALOAD, 0);
    mv.visitMethodInsn(Opcodes.INVOKESPECIAL, superInternalName, "<init>", "()V", false);
    mv.visitInsn(Opcodes.RETURN);
    mv.visitMaxs(1, 1);
    mv.visitEnd();
    cw.visitEnd();
    return cw.toByteArray();
  }

  private static byte[] generatePluginClassBytes(String internalName, String pluginName,
      String configFieldDesc) {
    ClassWriter cw = new ClassWriter(0);
    cw.visit(Opcodes.V1_8, Opcodes.ACC_PUBLIC | Opcodes.ACC_SUPER,
        internalName, null, "java/lang/Object", null);
    AnnotationVisitor pluginAv = cw.visitAnnotation(
        "L" + Plugin.class.getName().replace('.', '/') + ";", true);
    pluginAv.visit("type", "dummy");
    pluginAv.visitEnd();
    AnnotationVisitor nameAv = cw.visitAnnotation(
        "L" + Name.class.getName().replace('.', '/') + ";", true);
    nameAv.visit("value", pluginName);
    nameAv.visitEnd();
    cw.visitField(Opcodes.ACC_PRIVATE, "config", configFieldDesc, null, null).visitEnd();
    MethodVisitor mv = cw.visitMethod(Opcodes.ACC_PUBLIC, "<init>", "()V", null, null);
    mv.visitCode();
    mv.visitVarInsn(Opcodes.ALOAD, 0);
    mv.visitMethodInsn(Opcodes.INVOKESPECIAL, "java/lang/Object", "<init>", "()V", false);
    mv.visitInsn(Opcodes.RETURN);
    mv.visitMaxs(1, 1);
    mv.visitEnd();
    cw.visitEnd();
    return cw.toByteArray();
  }

  private static File createCustomBundleJar(File destFile, String exportPackage,
      Map<String, byte[]> classEntries, Map<String, File> embeddedJars) throws IOException {
    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
    if (exportPackage != null) {
      manifest.getMainAttributes().put(ManifestFields.EXPORT_PACKAGE, exportPackage);
    }
    try (JarOutputStream jos = new JarOutputStream(new FileOutputStream(destFile), manifest)) {
      for (Map.Entry<String, byte[]> entry : classEntries.entrySet()) {
        jos.putNextEntry(new JarEntry(entry.getKey()));
        jos.write(entry.getValue());
        jos.closeEntry();
      }
      for (Map.Entry<String, File> embedded : embeddedJars.entrySet()) {
        jos.putNextEntry(new JarEntry(embedded.getKey()));
        com.google.common.io.Files.asByteSource(embedded.getValue()).copyTo(jos);
        jos.closeEntry();
      }
    }
    return destFile;
  }

  private File createClassOnlyJar(File destFile, String exportPackage, Class<?>... classes)
      throws IOException {
    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
    manifest.getMainAttributes().put(ManifestFields.EXPORT_PACKAGE, exportPackage);
    try (JarOutputStream jos = new JarOutputStream(new FileOutputStream(destFile), manifest)) {
      for (Class<?> cls : classes) {
        String classResource = cls.getName().replace('.', '/') + ".class";
        try (InputStream classBytes = getClass().getClassLoader().getResourceAsStream(classResource)) {
          Assert.assertNotNull(classBytes);
          jos.putNextEntry(new JarEntry(classResource));
          ByteStreams.copy(classBytes, jos);
          jos.closeEntry();
        }
      }
    }
    return destFile;
  }

  /**
   * Unpackaged base class omitted from the incomplete plugin test jar.
   */
  public static class OmittedPluginSuperclass {
  }

  /**
   * Plugin whose config references an intermediate parent plugin's exported class.
   */
  @Plugin(type = "dummy")
  @Name("childMidExport")
  public static class ChildUsingMidExportPlugin {
    @SuppressWarnings({"unused", "java:S1068"})
    private NestedConfigPlugin.NestedConfig config;
  }

  /**
   * Annotated plugin class whose superclass is omitted from the test jar to trigger NoClassDefFoundError.
   */
  @Plugin(type = "dummy")
  @Name("brokenSubclass")
  public static class BrokenPluginSubclass extends OmittedPluginSuperclass {
  }

  private File getAppFile() throws IOException {
    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(ManifestFields.EXPORT_PACKAGE, InspectionApp.class.getPackage().getName());
    return createJar(InspectionApp.class, new File(TMP_FOLDER.newFolder(), "InspectionApp-1.0.0.jar"), manifest);
  }

  private static File createJar(Class<?> cls, File destFile, Manifest manifest) throws IOException {
    Location deploymentJar = AppJarHelper.createDeploymentJar(new LocalLocationFactory(TMP_FOLDER.newFolder()),
                                                              cls, manifest);
    DirUtils.mkdirs(destFile.getParentFile());
    Locations.linkOrCopyOverwrite(deploymentJar, destFile);
    return destFile;
  }
}

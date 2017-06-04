.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2016-2017 Cask Data, Inc.

.. _cdap-pipelines-testing-plugins:

===============
Testing Plugins
===============

Test Framework for Plugins
==========================

.. highlight:: java

.. include:: /testing/testing.rst
   :start-after: .. _test-framework-strategies-artifacts:
   :end-before:  .. _test-framework-validating-sql:

CDAP Pipelines Test Module
==========================
Additional information on unit testing with CDAP is in the Developer Manual section
on :ref:`Testing a CDAP Application <test-framework>`.

.. highlight:: xml

In addition, CDAP provides a ``hydrator-test`` module that contains several mock plugins
for you to use in tests with your custom plugins. To use the module, add a dependency to
your ``pom.xml``::

    <dependency>
      <groupId>co.cask.cdap</groupId>
      <artifactId>hydrator-test</artifactId>
      <version>${cdap.version}</version>
      <scope>test</scope>
    </dependency>

.. highlight:: java

Then extend the ``HydratorTestBase`` class, and create a method that will setup up the
application artifact and mock plugins, as well as the artifact containing your custom plugins::

  /**
   * Unit tests for our plugins.
   */
  public class PipelineTest extends HydratorTestBase {
    private static final ArtifactSummary APP_ARTIFACT = new ArtifactSummary("cdap-data-pipeline", "1.0.0");
    @ClassRule
    public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false);

    @BeforeClass
    public static void setupTestClass() throws Exception {
      ArtifactId parentArtifact = NamespaceId.DEFAULT.artifact(APP_ARTIFACT.getName(), APP_ARTIFACT.getVersion());

      // Add the data pipeline artifact and mock plugins.
      setupBatchArtifacts(parentArtifact, DataPipelineApp.class);

      // Add our plugins artifact with the data pipeline artifact as its parent.
      // This will make our plugins available to the data pipeline.
      addPluginArtifact(NamespaceId.DEFAULT.artifact("example-plugins", "1.0.0"),
                        parentArtifact,
                        TextFileSetSource.class,
                        TextFileSetSink.class,
                        WordCountAggregator.class,
                        WordCountCompute.class,
                        WordCountSink.class);
    }

You can then add test cases as you see fit. The ``cdap-data-pipeline-plugins-archetype``
includes an example of this unit test.

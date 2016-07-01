.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2016 Cask Data, Inc.
    :description: Glossary of terms related to Cask Hydrator, ETL, and Data Pipelines


.. _cask-hydrator-glossary:

======================
Cask Hydrator Glossary
======================

.. glossary::
   :sorted:

  Application
    A collection of programs and services that read and write through the data
    abstraction layer in CDAP.
      
  CDAP Application
    See :term:`Application`.

  Artifact
    A JAR file containing Java classes and resources required to create and run an
    :term:`Application`. Multiple applications can be created from the same artifact.

  Cask Hydrator
    An extendable framework for the development, running, and operating of data
    pipelines on Hadoop. Encompasses both the framework and the Cask Hydrator Studio, a visual
    editor of pipelines.

  Cask Hydrator Studio
    A visual editor for the configuring and previewing of pipelines. It
    includes: drag-and-drop plugins, such as sources, transformations, and sinks; naming and
    validating of a pipeline; previewing the running of the pipeline with a [subset?] of the
    data.

  ETL
    Abbreviation for Extract, Transform, and Load of data.

  Hydrator Pipeline
    A CDAP application; created from an application template, generally one
    of the system artifacts shipped with CDAP; defines a source to read from, zero or more
    transformations or other steps to perform on the data that was read from the source, and
    one or more sinks to write the transformed data to.

  System Artifact
    An application template, shipped with CDAP, that with the addition of
    configuration file, can be used to create manifestations of applications.

  ETL Application
    A CDAP application that performs ETL, such as a Hydrator Pipeline.

  Hydrator Plugin
    A plugin of type BatchSource, RealtimeSource, BatchSink, RealtimeSink, or
    Transformation, packaged in a JAR file format, for use as a plugin in an ETL Application a
    Hydrator Pipeline.

  Plugin
    A plugin extends an application [or extends an application template?] by
    implementing an interface expected by the application. Plugins are packaged in an
    artifact.

  Structured Record
    A data format, defined in CDAP, that can be used to exchange events
    between plugins. Used by many of the Hydrator Plugins included in CDAP.

  CDAP Extension
    *[To be completed]*

  ETL Pipeline
    See :term:`Hydrator Pipeline`.

  Application Template
    *[To be completed]*

  Pipeline
    *[To be completed]*

  Data Pipeline
    *[To be completed]*

  Batch Pipeline
    *[To be completed]*

  Real-time Pipeline
    *[To be completed]*

  Logical Pipeline
    *[To be completed]*

  Physical Pipeline
    *[To be completed]*


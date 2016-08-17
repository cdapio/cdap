.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2016 Cask Data, Inc.
    :description: Glossary of terms related to Cask Hydrator, ETL, and Data Pipelines


.. _cask-hydrator-glossary:

======================
Cask Hydrator Glossary
======================

*Additional entries are included in the* :ref:`CDAP Glossary <reference:glossary>`.

.. glossary::
   :sorted:

   Application
      A collection of programs and services that read and write through the data
      abstraction layer in CDAP.

   Application Template
      An artifact, that with the addition of a configuration file, can be used to create
      manifestations of applications.

   Artifact
      A JAR file containing Java classes and resources required to create and run an
      :term:`Application`. Multiple applications can be created from the same artifact.

   Batch Pipeline
      A type of :term:`Hydrator Pipeline` that runs on a schedule, performing actions on
      a distinct set of data.

   CDAP Application
      See :term:`Application`.

   CDAP Extension
      One of :term:`Cask Hydrator` or Cask Tracker; a framework with APIs that extends
      CDAP functionality with specialized features and user interfaces.

   Cask Hydrator
      An extendable framework for the development, running, and operating of data
      pipelines on Hadoop. Encompasses both the framework and the Cask Hydrator Studio, a visual
      editor of pipelines.

   Cask Hydrator Studio
      A visual editor for the configuring and previewing of pipelines. It includes:
      drag-and-drop plugins, such as sources, transformations, and sinks; naming and
      validating of a pipeline.

   Data Pipeline
      A type of :term:`pipeline`, often not linear in nature and require the performing of
      complex transformations including forks and joins at the record and feed level. They
      can be configured to perform various functions at different times, including
      machine-learning algorithms and custom processing.

   ETL
      Abbreviation for *extract,* *transform,* and *loading* of data.

   ETL Application
      A CDAP application that performs ETL, such as a Hydrator Pipeline.

   ETL Pipeline
      See :term:`Hydrator Pipeline`.

   Hydrator Pipeline
      A CDAP application; created from an application template, generally one
      of the system artifacts shipped with CDAP; defines a source to read from, zero or more
      transformations or other steps to perform on the data that was read from the source, and
      one or more sinks to write the transformed data to.

   Hydrator Plugin
      A plugin of type BatchSource, RealtimeSource, BatchSink, RealtimeSink, or
      Transformation, packaged in a JAR file format, for use as a plugin in a
      Hydrator Pipeline.

   Logical Pipeline
      A view of a :term:`pipeline` composed of sources, sinks, and other plugins, and does
      not show the underlying technology used to actually manifest and run the pipeline.

   Physical Pipeline
      A physical pipeline is the manifestation of a :term:`logical pipeline` as a CDAP
      application, which is a collection of programs and services that read and write
      through the data abstraction layer in CDAP.

   Pipeline
      A pipeline is a series of stages |---| linked usages of individual programs |---|
      configured together into an application.

   Plugin
      A plugin extends an application template by implementing an interface expected by
      the application template. Plugins are packaged in an artifact.

   Real-time Pipeline
      A type of :term:`Hydrator Pipeline` that runs continuously, performing actions on
      a distinct set of data.

   Structured Record
      A data format, defined in CDAP, that can be used to exchange events
      between plugins. Used by many of the Hydrator Plugins included in CDAP.

   System Artifact
      An application template, shipped with CDAP, that with the addition of a
      configuration file, can be used to create manifestations of applications.

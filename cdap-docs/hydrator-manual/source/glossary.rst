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

   Application Template
      *[To be completed]*

   Artifact
      A JAR file containing Java classes and resources required to create and run an
      :term:`Application`. Multiple applications can be created from the same artifact.

   Batch Pipeline
      *[To be completed]*

   CDAP Application
      See :term:`Application`.

   CDAP Extension
      *[To be completed]*

   Cask Hydrator
      An extendable framework for the development, running, and operating of data
      pipelines on Hadoop. Encompasses both the framework and the Cask Hydrator Studio, a visual
      editor of pipelines.

   Cask Hydrator Studio
      A visual editor for the configuring and previewing of pipelines. It
      includes: drag-and-drop plugins, such as sources, transformations, and sinks; naming and
      validating of a pipeline; previewing the running of the pipeline with a [subset?] of the
      data.

   Data Pipeline
      *[To be completed]*

   ETL
      Abbreviation for *extract,* *transform,* and *load* of data.

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
      Transformation, packaged in a JAR file format, for use as a plugin in an ETL Application a
      Hydrator Pipeline.

   Logical Pipeline
      *[To be completed]*

   Physical Pipeline
      *[To be completed]*

   Pipeline
      *[To be completed]*

   Plugin
      A plugin extends an application [or extends an application template?] by
      implementing an interface expected by the application. Plugins are packaged in an
      artifact.

   Real-time Pipeline
      *[To be completed]*

   Structured Record
      A data format, defined in CDAP, that can be used to exchange events
      between plugins. Used by many of the Hydrator Plugins included in CDAP.

   System Artifact
      An application template, shipped with CDAP, that with the addition of
      configuration file, can be used to create manifestations of applications.

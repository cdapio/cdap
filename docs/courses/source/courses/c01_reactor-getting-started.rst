.. :Author: JJ
   :Description: Getting Started With Continuuity Reactor

========================================
Getting Started With Continuuity Reactor
========================================

.. reST Editor: .. section-numbering::
.. reST Editor: .. contents::

.. rst2pdf: CutStart
.. Slide Presentation HTML Generation
.. landslide: theme ../_theme/slides-generation/
.. landslide: build ../../html/
.. landslide: return c01_return.html

.. include:: ../_slide-fragments/continuuity_logo_copyright.rst

.. |br| raw:: html

   <br />
.. rst2pdf: CutStop

.. rst2pdf: config ../../../developer-guide/source/_templates/pdf-config
.. rst2pdf: stylesheets ../../../developer-guide/source/_templates/pdf-stylesheet
.. rst2pdf: build ../../pdf/
.. rst2pdf: .. |br| unicode:: U+0020 .. space

----

Getting Started With Continuuity Reactor
========================================

**Introduction**

- Chapter 1: What Does Continuuity Reactor Do?

.. *10 Minute Break*

**Reactor Building Blocks** 

- Chapter 2: Continuuity Reactor Applications
- Chapter 3: Ingesting Data into Continuuity Reactor
- Chapter 4: Real-time Processing Using Flows
- Chapter 5: Storing Data Using DataSets
- Chapter 6: Querying Using Procedures
- Chapter 7: Batch Processing Using MapReduce and Workflows

.. *Lunch Break*

----

Getting Started With Continuuity Reactor
========================================

**Reactor In Production**

- Chapter 8: Testing and Debugging Applications With Reactor
- Chapter 9: Operational Considerations For Reactor

.. *Conclusion*

----

Overview: Course Objectives
===========================

Upon completion of this course you should be able to:

- Describe Continuuity Reactor capabilities
- Explain Reactor programming paradigms
- Understand real-time processing using Flows
- Serve data using Procedures
- Perform batch processing using MapReduce jobs and Workflows
- Build basic Reactor applications
- Understand Reactor application debugging and testing strategies
- Complete common operations tasks and procedures

----

.. rst2pdf: CutStart

.. landslide: COURSE MODULES START HERE


.. landslide: section: Introduction
.. --------------------------------

.. chapter 1
.. landslide: chapter: What Does Continuuity Reactor Do?
.. .....................................................
.. landslide: module: Solving Big Data Problems with Reactor
.. landslide: module link: ../modules/m01_solving_big_data_problems_with_reactor.rst
.. landslide: module: Continuuity Reactor Capabilities
.. landslide: module link: ../modules/m02_reactor_capabilities.rst
.. landslide: module: Continuuity Reactor Overview
.. landslide: module link: ../modules/m03_reactor_overview.rst
.. landslide: module: Continuuity Reactor SDK
.. landslide: module link: ../modules/m04_reactor_sdk.rst
.. landslide: exercise: Download Continuuity Reactor SDK, Install and Run Quick Start
.. landslide: exercise link: ../exercises/e01_reactor_sdk.rst


.. landslide: section: Reactor Building Blocks
.. -------------------------------------------

.. chapter 2
.. landslide: chapter: Continuuity Reactor Applications
.. ....................................................
.. landslide: module: Exploring an Example Reactor Application
.. landslide: module link: ../modules/m05_exploring_a_reactor_application.rst
.. landslide: exercise: Continuuity Reactor Purchase Example Using Dashboard
.. landslide: exercise link: ../exercises/e02_purchase_example_dashboard.rst
.. landslide: module: Introduction to the Continuuity Reactor REST API
.. landslide: module link: ../modules/m06_introduction_to_cr_rest.rst
.. landslide: exercise: Continuuity Reactor Purchase Example Using REST
.. landslide: exercise link: ../exercises/e03_purchase_example_rest.rst
.. landslide: module: Building A Continuuity Reactor Application
.. landslide: module link: ../modules/m07_building_a_cr_application.rst
.. landslide: exercise: Build Your First Continuuity Reactor Application
.. landslide: exercise link: ../exercises/e04_build_your_first_cr_application.rst

.. chapter 3
.. landslide: chapter: Ingesting Data into Continuuity Reactor
.. ...........................................................
.. landslide: module: Sending Data To Streams
.. landslide: module link: ../modules/m08_sending_data_to_streams.rst
.. landslide: module: Creating Streams
.. landslide: module link: ../modules/m09_creating_streams.rst
.. landslide: module: Streams REST API
.. landslide: module link: ../modules/m10_streams_rest_api.rst

.. chapter 4
.. landslide: chapter: Real-time Processing Using Flows
.. ................................................
.. landslide: module: Flows and Flowlets
.. landslide: module link: ../modules/m11_flows_and_flowlets.rst
.. landslide: module: Flow Partitioning
.. landslide: module link: ../modules/m12_flow_partitioning.rst
.. landslide: module: Types of Flowlets
.. landslide: module link: ../modules/m13_types_of_flowlets.rst
.. landslide: module: Type Projection
.. landslide: module link: ../modules/m14_type_projection.rst
.. landslide: exercise: Building An Application Using Streams and Flows
.. landslide: exercise link: ../exercises/e05_streams_flows_exercise.rst

.. chapter 5
.. landslide: chapter: Storing Data Using DataSets
.. ................................................
.. landslide: module: Introduction to DataSets
.. landslide: module link: ../modules/m15_introduction_datasets.rst
.. landslide: module: Custom DataSets
.. landslide: module link: ../modules/m16_custom_datasets.rst
.. landslide: module: DataSet Programming API
.. landslide: module link: ../modules/m17_dataset_programming_api.rst
.. landslide: module: DataSets and Transactions
.. landslide: module link: ../modules/m18_datasets_transactions.rst
.. landslide: module: DataSet REST API
.. landslide: module link: ../modules/m19_dataset_rest_api.rst
.. landslide: exercise: Building An Application Using DataSets
.. landslide: exercise link: ../exercises/e06_datasets_exercise.rst

.. chapter 6
.. landslide: chapter: Querying Using Procedures
.. ................................................
.. landslide: module: Querying Using Procedures
.. landslide: module link: ../modules/m20_querying_using_procedures.rst
.. landslide: module: Procedures REST API
.. landslide: module link: ../modules/m21_procedures_rest_api.rst
.. landslide: exercise: Building An Application Using Procedures
.. landslide: exercise link: ../exercises/e07_procedures_exercise.rst
.. landslide: exercise: Building An Application Using External Processes
.. landslide: exercise link: ../exercises/e08_external_processing_exercise.rst

.. chapter 7
.. landslide: chapter: Batch Processing Using MapReduce and Workflows
.. ................................................
.. landslide: module: Introduction to MapReduce and Workflows
.. landslide: module link: ../modules/m22_mapreduce_workflows.rst
.. landslide: module: MapReduce and DataSets
.. landslide: module link: ../modules/m23_mapreduce_datasets.rst
.. landslide: module: MapReduce and Transactions
.. landslide: module link: ../modules/m24_mapreduce_transactions.rst
.. landslide: module: Workflows
.. landslide: module link: ../modules/m25_workflows.rst
.. landslide: exercise: Building An Application Using MapReduce and Workflows
.. landslide: exercise link: ../exercises/e09_mapreduce_workflow_exercise.rst


.. landslide: section: Reactor In Production
.. -------------------------------------------

.. chapter 8
.. landslide: chapter: Testing and Debugging Applications With Reactor
.. ................................................
.. landslide: module: Testing Reactor Applications
.. landslide: module link: ../modules/m26_testing_reactor_applications.rst
.. landslide: module: Debugging Reactor Applications
.. landslide: module link: ../modules/m27_debugging_reactor_applications.rst
.. landslide: exercise: Testing and Debugging Reactor Applications
.. landslide: exercise link: ../exercises/e10_testing_debugging_exercise.rst

.. chapter 9
.. landslide: chapter: Operational Considerations For Reactor
.. ................................................
.. landslide: module: Instrumenting Application Health via Metrics
.. landslide: module link: ../modules/m28_instrumenting_via_metrics.rst
.. landslide: module: Collecting Critical Application Information via Logs
.. landslide: module link: ../modules/m29_collecting_via_logs.rst
.. landslide: module: Runtime Arguments and Scaling Instances
.. landslide: module link: ../modules/m30_runtime_arguments_scaling_instances.rst
.. landslide: exercise: Monitoring and Logging Reactor Applications
.. landslide: exercise link: ../exercises/e11_monitoring_logging_exercise.rst
.. landslide: module: Lifecycle Management
.. landslide: module link: ../modules/m31_lifecycle_management.rst
.. landslide: exercise: Controlling the Lifecyle of a Reactor Application
.. landslide: exercise link: ../exercises/e12_controlling_application_exercise.rst

.. landslide: CUT START

.. chapter XX
.. landslide: chapter: Controlling Application Lifecycle using REST APIs
.. ................................................
.. landslide: module: Application Management API
.. landslide: module: Flow Management API
.. landslide: module: Procedure Management APIs
.. landslide: exercise: Controlling Your Reactor Application
.. landslide: exercise link: ../exercises/e11_controlling_application_exercise.rst

.. landslide: section: Integrating Reactor Into Application Development Workflow
.. -------------------------------------------

.. chapter xx
.. landslide: chapter: Reactor In Your Continuous Integration Environment
.. ................................................
.. landslide: module: Introduction to Types of Reactors
.. landslide: module: Integrating Reactor in CI

.. chapter xx
.. landslide: chapter: Integration Testing Strategies
.. ................................................
.. landslide: module: Testing with Local Reactor
.. landslide: module: Testing with Distributed Reactor

.. chapter xx
.. landslide: chapter: Debugging Applications With Reactor
.. ................................................
.. landslide: module: Debugging Strategies


.. landslide: section: Reactor In Production
.. -------------------------------------------

.. chapter xx
.. landslide: chapter: Operational Considerations For Reactor
.. ................................................
.. landslide: module: Instrumenting Application Health via Metrics
.. landslide: module: Monitoring Application Health 
.. landslide: module: Collecting Critical Application Information via Logs
.. landslide: module: Aggregating Application Logs
.. landslide: module: Scaling Instances

.. chapter xx
.. landslide: chapter: Controlling Application Lifecycle using REST APIs
.. ................................................
.. landslide: module: Application Management API
.. landslide: module: Flow Management API
.. landslide: module: Procedure Management APIs

.. chapter xx
.. landslide: chapter: Collecting Data From External Sources
.. ................................................
.. landslide: module: Getting Data from Twitter API
.. landslide: module: Getting Data from Kafka 
.. landslide: module: Getting Data from Flume

.. landslide: CUT STOP

.. landslide: COURSE MODULES END HERE

----

.. rst2pdf: CutStop

Conclusion
==========

.. rst2pdf: CutStart

.fx: center_title_slide

.. rst2pdf: CutStop

----

Appendices
==========

- Developer Documentation
- Support Page
- Data Sheets

.. - White Paper

----

Course Summary
==============

You should now be able to:

- Describe Continuuity Reactor capabilities
- Explain Reactor programming paradigms
- Build basic Reactor applications
- Create an application using ingesting with Streams
- Understand real-time processing using Flows
- Serve data using Procedures
- Perform batch processing using MapReduce jobs and Workflows
- Understand Reactor application testing strategies

----

Congratulations!
================

- You have completed the **Getting Started With Continuuity Reactor** course
- Please be sure to complete the course evaluation
- Thank you for your time and attention!

---- 

Thank you!
==========

Please complete the Evaluation Forms and return them to the instructor.

.. rst2pdf: CutStart
.. include:: ../_slide-fragments/continuuity_logo_copyright.rst
.. rst2pdf: CutStop

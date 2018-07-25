.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2018 Cask Data, Inc.

.. .. :titles-only-global-toc: true

:hide-toc: true

.. _cloud-runtimes:

==============
Cloud Runtimes
==============

.. toctree::
   :maxdepth: 3

    Concepts <concepts/index>
    Provisioners <provisioners/index>
    Profiles <profiles/index>
    Example <example/index>

Cloud Runtimes allow you to configure batch pipelines to run in a cloud environment.
Before the pipeline runs, a cluster is provisioned in the cloud. The pipeline is
executed on that cluster, and the cluster is deleted after the run finishes. Cloud Runtimes allow
you to only use compute resources when you need them, allowing you to make better use of
your resources.

.. |concepts| replace:: **Concepts:**
.. _concepts: concepts/index.html

.. |provisioners| replace:: **Provisioners:**
.. _provisioners: provisioners/index.html

.. |profiles| replace:: **Profiles:**
.. _profiles: profiles/index.html

.. |example| replace:: **Example:**
.. _example: example/index.html


- |concepts|_ Concepts and Terminology for cloud runtimes

- |provisioners|_ Provisioners are responsible for creating and deleting cloud clusters for a program run

- |profiles|_ Profiles are used to configure programs to run in different cloud environments

- |example|_ A step by step walk-through of creating and using a profile


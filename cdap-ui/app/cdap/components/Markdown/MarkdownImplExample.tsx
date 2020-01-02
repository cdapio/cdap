/*
 * Copyright © 2019 Cask Data, Inc.
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
import ThemeWrapper from 'components/ThemeWrapper';
import React from 'react';
import { MarkdownWithStyles as Markdown } from 'components/Markdown';

export default function MarkdownImpl() {
  const markdown =
    "# Google BigQuery Table Source\n\nDescription\n-----------\nThis source reads the entire contents of a BigQuery table.\nBigQuery is Google's serverless, highly scalable, enterprise data warehouse.\nData from the BigQuery table is first exported to a temporary location on Google Cloud Storage,\nthen read into the pipeline from there.\n\nCredentials\n-----------\nIf the plugin is run on a Google Cloud Dataproc cluster, the service account key does not need to be\nprovided and can be set to 'auto-detect'.\nCredentials will be automatically read from the cluster environment.\n\nIf the plugin is not run on a Dataproc cluster, the path to a service account key must be provided.\nThe service account key can be found on the Dashboard in the Cloud Platform Console.\nMake sure the account key has permission to access BigQuery and Google Cloud Storage.\nThe service account key file needs to be available on every node in your cluster and\nmust be readable by all users running the job.\n\nProperties\n----------\n**Reference Name:** Name used to uniquely identify this source for lineage, annotating metadata, etc.\n\n**Project ID**: Google Cloud Project ID, which uniquely identifies a project.\nIt can be found on the Dashboard in the Google Cloud Platform Console. This is the project\nthat the BigQuery job will run in. If a temporary bucket needs to be created, the service account\nmust have permission in this project to create buckets.\n\n**Dataset Project**: Project the dataset belongs to. This is only required if the dataset is not\nin the same project that the BigQuery job will run in. If no value is given,\nit will default to the configured Project ID.\n\n**Dataset**: Dataset the table belongs to. A dataset is contained within a specific project.\nDatasets are top-level containers that are used to organize and control access to tables and views.\n\n**Table**: Table to read from. A table contains individual records organized in rows.\nEach record is composed of columns (also called fields).\nEvery table is defined by a schema that describes the column names, data types, and other information.\n\n**Temporary Bucket Name**: Google Cloud Storage bucket to store temporary data in.\nIt will be automatically created if it does not exist, but will not be automatically deleted.\nTemporary data will be deleted after it has been read. If it is not provided, a unique bucket will be\ncreated and then deleted after the run finishes.\n\n**Service Account File Path**: Path on the local file system of the service account key used for\nauthorization. Can be set to 'auto-detect' when running on a Dataproc cluster.\nWhen running on other clusters, the file must be present on every node in the cluster.\n\n**Schema**: Schema of the table to read. This can be fetched by clicking the 'Get Schema' button.\n";
  const markdown2 =
    '# Distinct Aggregator\n\nDescription\n-----------\nDe-duplicates input records so that all output records are distinct.\nCan optionally take a list of fields, which will project out all other fields and perform a distinct on just those fields.\n\n\nUse Case\n--------\nThis plugin is used when you want to ensure that all output records are unique.\n\n\nProperties\n----------\n**fields:** Optional comma-separated list of fields to perform the distinct on. If not given, all fields are used.\n\n**numPartitions:** Number of partitions to use when grouping fields. If not specified, the execution\nframework will decide on the number to use.\n\nExample\n-------\n\n```javascript \n    {\n        "name": "Distinct",\n        "type": "batchaggregator"\n        "properties": {\n            "fields": "user,item,action"\n        }\n    }\n```  \n\n\nThis example takes the ``user``, ``action``, and ``item`` fields from input records and dedupes them so that every\noutput record is a unique record with those three fields. For example, if the input to the plugin is:\n\n| user  | item   | action | timestamp |\n| ----- | ------ | ------ | --------- |\n| bob   | donut  | buy    | 1000      |\n| bob   | donut  | buy    | 1000      |\n| bob   | donut  | buy    | 1001      |\n| bob   | coffee | buy    | 1001      |\n| bob   | coffee | drink  | 1010      |\n| bob   | donut  | eat    | 1050      |\n| bob   | donut  | eat    | 1080      |\n\nthen records output will be:\n\n| user  | item   | action |\n| ----- | ------ | ------ |\n| bob   | donut  | buy    |\n| bob   | coffee | buy    |\n| bob   | coffee | drink  |\n| bob   | donut  | eat    |\n';
  const markdown5 =
    '| user  | item   | action | timestamp | \n | ----- | ------ | ------ | --------- | \n | bob | donut | buy | 1000 | \n | bob | donut | buy | 1000 | \n | bob | donut | buy | 1001 | \n | bob | coffee | buy | 1001 | \n | bob | coffee | drink | 1010 | \n | bob | donut | eat | 1050 | \n | bob | donut | eat | 1080 |';
  return (
    <ThemeWrapper>
      <div>
        <Markdown markdown={markdown2} />
      </div>
    </ThemeWrapper>
  );
}

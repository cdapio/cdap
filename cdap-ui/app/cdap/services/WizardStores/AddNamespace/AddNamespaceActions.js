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

const AddNamespaceActions = {
  setName: 'SET-NAMESPACE-NAME',
  setDescription: 'SET-NAMESPACE-DESCRIPTION',
  setSchedulerQueue: 'SET-NAMESPACE-SCHEDULER-QUEUE',
  setHDFSDirectory: 'SET-HDFS-DIRECTORY',
  setHiveDatabaseName: 'SET-HIVE-DATABASE-NAME',
  setHBaseNamespace: 'SET-HBASE-NS-NAME',
  setPrincipal: 'SET-PRINCIPAL',
  setKeytab: 'SET-KEYTAB',
  onError: 'FORM-SUBMIT-FAILURE',
  onSuccess: 'FORM-SUBMIT-SUCCESS',
  onReset: 'FORM-RESET',
  setPreferences: 'SET-PREFERENCES'
};

export default AddNamespaceActions;

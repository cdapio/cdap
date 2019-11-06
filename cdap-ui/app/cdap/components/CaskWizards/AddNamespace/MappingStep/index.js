/*
 * Copyright © 2016-2018 Cask Data, Inc.
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
import React from 'react';
import {Col, FormGroup, Label, Form} from 'reactstrap';
import AddNamespaceStore from 'services/WizardStores/AddNamespace/AddNamespaceStore';
import AddNamespaceActions  from 'services/WizardStores/AddNamespace/AddNamespaceActions';
import T from 'i18n-react';
import {Provider, connect} from 'react-redux';
import ValidatedInput from 'components/ValidatedInput';
import types from 'services/inputValidationTemplates';

var inputs = {
  hdfs: {
    error: '',
    required: false,
    template: 'NAME',
    label: 'hdfsDirectory',
  },
  hive: {
    error: '',
    required: false,
    template: 'NAME',
    label: 'hiveDatabaseName',
  },
  hbase: {
    error: '',
    required: false,
    template: 'NAME',
    label: 'hbaseNamespace',
  },
  scheduler: {
    error: '',
    required: false,
    template: 'NAME',
    label: 'schedulerQueueName',
  },
};

const getErrorMessage = (value, field) => {
  const isValid = types[inputs[field].template].validate(value);
  if (value && !isValid) {
    return types[inputs[field].template].getErrorMsg();
  } else {
    return '';
  }
};


// HDFS Root Directory
const mapStateToHDFSRootDirectoryProps = (state) => {
  return {
    value: state.mapping.hdfsDirectory,
    type: 'text',
    placeholder: T.translate('features.Wizard.Add-Namespace.Step2.hdfs-root-directory-placeholder'),
    disabled: state.editableFields.fields.indexOf('hdfsDirectory') === -1,
    label:  inputs.hdfs.label,
    validationError: inputs.hdfs.error
  };
};

const mapDispatchToHDFSRootDirectoryProps = (dispatch) => {
  return {
    onChange: (e) => {
      inputs.hdfs.error = getErrorMessage(e.target.value, 'hdfs');
      dispatch({
        type: AddNamespaceActions.setHDFSDirectory,
        payload: { hdfsDirectory : e.target.value, hdfsDirectory_valid: inputs.hdfs.error !== '' ? false : true  }
      });
    }
  };
};

// Hive Database Name
const mapStateToHiveDatabaseNameProps = (state) => {
  return {
    value: state.mapping.hiveDatabaseName,
    type: 'text',
    placeholder: T.translate('features.Wizard.Add-Namespace.Step2.hive-db-name-placeholder'),
    disabled: state.editableFields.fields.indexOf('hiveDatabaseName') === -1,
    label:  inputs.hive.label,
    validationError: inputs.hive.error
  };
};

const mapDispatchToHiveDatabaseNameProps = (dispatch) => {
  return {
    onChange: (e) => {
      inputs.hive.error = getErrorMessage(e.target.value, 'hive');
      dispatch({
        type: AddNamespaceActions.setHiveDatabaseName,
        payload: { hiveDatabaseName : e.target.value, hiveDatabaseName_valid: inputs.hive.error !== '' ? false : true  }
      });
    }
  };
};

// HBASE Namespace Name
const mapStateToHBASENamespaceNameProps = (state) => {
  return {
    value: state.mapping.hbaseNamespace,
    type: 'text',
    placeholder: T.translate('features.Wizard.Add-Namespace.Step2.hbase-nm-name-placeholder'),
    disabled: state.editableFields.fields.indexOf('hbaseNamespace') === -1,
    label:  inputs.hbase.label,
    validationError: inputs.hbase.error
  };
};

const mapDispatchToHBASENamespaceNameProps = (dispatch) => {
  return {
    onChange: (e) => {
      inputs.hbase.error = getErrorMessage(e.target.value, 'hbase');
      dispatch({
        type: AddNamespaceActions.setHBaseNamespace,
        payload: { hbaseNamespace : e.target.value, hbaseNamespace_valid: inputs.hbase.error !== '' ? false : true  }
      });
    }
  };
};

const mapStateToSchedulerQueueNameProps = (state) => {
  return {
    value: state.mapping.schedulerQueueName,
    type: 'text',
    placeholder: T.translate('features.Wizard.Add-Namespace.Step2.scheduler-queue-placeholder'),
    disabled: state.editableFields.fields.indexOf('schedulerQueueName') === -1,
    label:  inputs.scheduler.label,
    validationError: inputs.scheduler.error
  };
};
const mapDispatchToSchedulerQueueNameProps = (dispatch) => {
  return {
    onChange: (e) => {
      inputs.scheduler.error = getErrorMessage(e.target.value, 'scheduler');
      dispatch({
        type: AddNamespaceActions.setSchedulerQueueName,
        payload: { schedulerQueueName : e.target.value, schedulerQueueName_valid: inputs.scheduler.error !== '' ? false : true  }
      });
    }
  };
};

const InputRootDirectory = connect(
  mapStateToHDFSRootDirectoryProps,
  mapDispatchToHDFSRootDirectoryProps
)(ValidatedInput);

const InputHiveDbName = connect(
  mapStateToHiveDatabaseNameProps,
  mapDispatchToHiveDatabaseNameProps
)(ValidatedInput);

const InputHbaseNamespace = connect(
  mapStateToHBASENamespaceNameProps,
  mapDispatchToHBASENamespaceNameProps
)(ValidatedInput);

const InputSchedulerQueueName = connect(mapStateToSchedulerQueueNameProps, mapDispatchToSchedulerQueueNameProps)(ValidatedInput);
export default function MappingStep() {
  return (
    <Provider store={AddNamespaceStore}>
      <Form
        className="form-horizontal mapping-step"
        onSubmit={(e) => {
          e.preventDefault();
          return false;
        }}
      >
        <FormGroup row>
          <Col xs="3">
            <Label className="control-label">
              {T.translate('features.Wizard.Add-Namespace.Step2.hdfs-root-directory-label')}
            </Label>
          </Col>
          <Col xs="7">
            <InputRootDirectory />
          </Col>
        </FormGroup>
        <FormGroup row>
          <Col xs="3">
            <Label className="control-label">
              {T.translate('features.Wizard.Add-Namespace.Step2.hive-db-name-label')}
            </Label>
          </Col>
          <Col xs="7">
            <InputHiveDbName />
          </Col>
        </FormGroup>
        <FormGroup row>
          <Col xs="3">
            <Label className="control-label">
              {T.translate('features.Wizard.Add-Namespace.Step2.hbase-nm-name-label')}
            </Label>
          </Col>
          <Col xs="7">
            <InputHbaseNamespace />
          </Col>
        </FormGroup>
        <FormGroup row>
          <Col xs="3">
            <Label className="control-label">
              {T.translate('features.Wizard.Add-Namespace.Step2.scheduler-queue-name')}
            </Label>
          </Col>
          <Col xs="7">
            <InputSchedulerQueueName />
          </Col>
        </FormGroup>
      </Form>
    </Provider>
  );
}

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
import React from 'react';
import {connect, Provider} from 'react-redux';
import {Col, FormGroup, Label, Form} from 'reactstrap';
import T from 'i18n-react';

import SelectWithOptions from 'components/SelectWithOptions';
import SimpleSchema from 'components/SimpleSchema';
import CreateStreamWithUploadStore, { defaultSchemaFormats } from 'services/WizardStores/CreateStreamWithUpload/CreateStreamWithUploadStore';
import CreateStreamWithUploadActions from 'services/WizardStores/CreateStream/CreateStreamActions';
const mapStateToSchemaTypeProps = (state) => {
  return {
    value: state.schema.format,
    options: defaultSchemaFormats
  };
};

const mapDispatchToSchemaTypeProps = (dispatch) => {
  return {
    onChange: (e) => (dispatch({
      type: CreateStreamWithUploadActions.setSchemaFormat,
      payload: { format: e.target.value}
    }))
  };
};

const SchemaType = connect(
  mapStateToSchemaTypeProps,
  mapDispatchToSchemaTypeProps
)(SelectWithOptions);
const SimpleSchemaWrapper = connect(
  state => {
    return {
      schema: state.schema.value,
    };
  },
  dispatch => {
    return {
      onSchemaChange: (schema) => {
        dispatch({
          type: CreateStreamWithUploadActions.setSchema,
          payload: { schema }
        });
      }
    };
  }
)(SimpleSchema);

export default function SchemaStep() {
  return (
    <Provider store={CreateStreamWithUploadStore}>
      <Form
        className="form-horizontal"
        onSubmit={(e) => {
          e.preventDefault();
          return false;
        }}
      >
        <FormGroup>
          <Col xs="2">
            <Label className="control-label">{T.translate('commons.formatLabel')}</Label>
          </Col>
          <Col xs="4">
            <SchemaType className="input-sm"/>
          </Col>
        </FormGroup>
        <FormGroup>
          <Col xs="12">
            <Label className="control-label">{T.translate('commons.schemaLabel')}</Label>
          </Col>
          <Col xs="12">
            <SimpleSchemaWrapper />
          </Col>
        </FormGroup>
      </Form>
    </Provider>
  );
}

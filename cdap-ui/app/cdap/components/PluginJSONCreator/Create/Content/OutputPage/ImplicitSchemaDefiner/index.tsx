/*
 * Copyright © 2020 Cask Data, Inc.
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

import * as React from 'react';

import Alert from 'components/Alert';
import Button from '@material-ui/core/Button';
import WidgetWrapper from 'components/ConfigurationGroup/WidgetWrapper';
import { useOutputState } from 'components/PluginJSONCreator/Create';

export enum SchemaStatus {
  Normal = 'Normal',
  Success = 'SUCCESS',
  Failure = 'FAILURE',
}

const ImplicitSchemaDefiner = () => {
  const { schema, setSchema } = useOutputState();
  const [localSchema, setLocalSchema] = React.useState(JSON.stringify(schema, undefined, 2));

  const [schemaStatus, setSchemaStatus] = React.useState(SchemaStatus.Normal);

  const onSchemaChange = (val) => {
    setLocalSchema(val);
  };

  const saveSchema = () => {
    try {
      setSchema(JSON.parse(localSchema));
      setSchemaStatus(SchemaStatus.Success);
    } catch (e) {
      setSchemaStatus(SchemaStatus.Failure);
    }
  };

  // Success Alert component always closes after 3000ms.
  // After a timeout for 3000ms, reset schemaStatus to make a success Alert component disappear
  const onSuccessAlertClose = () => {
    setSchemaStatus(SchemaStatus.Normal);
  };

  // A failure Alert component status should close only when user manually closes it
  const onFailureAlertClose = () => {
    setSchemaStatus(SchemaStatus.Normal);
  };

  return (
    <div data-cy="implicit-schema-definer">
      <WidgetWrapper
        widgetProperty={{
          field: 'schema',
          name: 'schema',
          'widget-type': 'json-editor',
          'widget-attributes': {
            rows: '20',
          },
        }}
        pluginProperty={{
          required: false,
          name: 'implicit-schema',
        }}
        value={localSchema}
        onChange={onSchemaChange}
      />
      <Button variant="contained" color="primary" onClick={saveSchema} data-cy="save-schema-btn">
        Save
      </Button>
      <Alert
        message={'Output schema saved successfully'}
        showAlert={schemaStatus === SchemaStatus.Success}
        type="success"
        onClose={onSuccessAlertClose}
      />
      <Alert
        message={'Invalid JSON for schema'}
        showAlert={schemaStatus === SchemaStatus.Failure}
        type="error"
        onClose={onFailureAlertClose}
      />
    </div>
  );
};

export default ImplicitSchemaDefiner;

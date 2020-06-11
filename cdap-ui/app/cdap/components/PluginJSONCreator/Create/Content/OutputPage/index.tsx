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

import withStyles, { StyleRules, WithStyles } from '@material-ui/core/styles/withStyles';
import WidgetWrapper from 'components/ConfigurationGroup/WidgetWrapper';
import Heading, { HeadingTypes } from 'components/Heading';
import If from 'components/If';
import { SchemaType } from 'components/PluginJSONCreator/constants';
import { useOutputState } from 'components/PluginJSONCreator/Create';
import PluginInput from 'components/PluginJSONCreator/Create/Content/PluginInput';
import StepButtons from 'components/PluginJSONCreator/Create/Content/StepButtons';
import { SCHEMA_TYPES } from 'components/SchemaEditor/SchemaHelpers';
import * as React from 'react';

const styles = (): StyleRules => {
  return {
    selectSchemaType: {
      marginBottom: '20px',
    },
    outputInput: {
      marginTop: '20px',
      marginBottom: '20px',
    },
  };
};

const SCHEMA_TYPES_DELIMITER = ',';

const OutputPageView: React.FC<WithStyles<typeof styles>> = ({ classes }) => {
  const {
    outputName,
    setOutputName,
    outputWidgetType,
    setOutputWidgetType,
    schemaTypes,
    setSchemaTypes,
    schemaDefaultType,
    setSchemaDefaultType,
    schema,
    setSchema,
  } = useOutputState();

  const [isValidSchema, setIsValidSchema] = React.useState(true);

  const onSchemaTypesChange = (val) => {
    setSchemaTypes(val.split(SCHEMA_TYPES_DELIMITER));
  };

  const onSchemaChange = (val) => {
    try {
      setSchema(JSON.parse(val));
    } catch (e) {
      setIsValidSchema(false);
    }
  };

  return (
    <div>
      <Heading type={HeadingTypes.h3} label="Output" />
      <br />
      <div className={classes.selectSchemaType}>
        <PluginInput
          widgetType={'radio-group'}
          value={outputWidgetType}
          onChange={setOutputWidgetType}
          label={'Schema Type'}
          options={Object.values(SchemaType).map((mode) => ({ id: mode, label: mode }))}
          layout={'inline'}
        />
      </div>
      <If condition={outputWidgetType === SchemaType.Explicit}>
        <div className={classes.outputInput}>
          <PluginInput
            widgetType={'textbox'}
            value={outputName}
            onChange={setOutputName}
            label={'Output Name'}
            placeholder={'output name'}
          />
        </div>
        <div className={classes.outputInput}>
          <PluginInput
            widgetType={'multi-select'}
            onChange={onSchemaTypesChange}
            label={'Schema Types'}
            delimiter={SCHEMA_TYPES_DELIMITER}
            options={SCHEMA_TYPES.types.map((type) => ({ id: type, label: type }))}
            value={schemaTypes}
          />
        </div>
        <div className={classes.outputInput}>
          <PluginInput
            widgetType={'select'}
            value={schemaDefaultType}
            onChange={setSchemaDefaultType}
            label={'Schema Default Type'}
            options={schemaTypes}
          />
        </div>
      </If>
      <If condition={outputWidgetType === SchemaType.Implicit}>
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
            name: 'json-editor',
          }}
          value={JSON.stringify(schema, undefined, 2)}
          onChange={onSchemaChange}
        />
      </If>
      <StepButtons nextDisabled={false} />
    </div>
  );
};

const OutputPage = withStyles(styles)(OutputPageView);
export default OutputPage;

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

import withStyles, { StyleRules, WithStyles } from '@material-ui/core/styles/withStyles';

import { COMMON_DELIMITER } from 'components/PluginJSONCreator/constants';
import PluginInput from 'components/PluginJSONCreator/Create/Content/PluginInput';
import { schemaTypes as avroSchemaTypes } from 'components/AbstractWidget/SchemaEditor/SchemaConstants';
import { useOutputState } from 'components/PluginJSONCreator/Create';

const styles = (): StyleRules => {
  return {
    outputInput: {
      marginTop: '30px',
      marginBottom: '30px',
    },
  };
};

const ExplicitSchemaDefinerView: React.FC<WithStyles<typeof styles>> = ({ classes }) => {
  const {
    outputName,
    setOutputName,
    schemaTypes,
    setSchemaTypes,
    schemaDefaultType,
    setSchemaDefaultType,
  } = useOutputState();

  // schemaDefaultType should update to empty value if schemaTypes doesn't contain schemaDefaultType
  React.useEffect(() => {
    if (schemaTypes && !schemaTypes.includes(schemaDefaultType)) {
      setSchemaDefaultType('');
    }
  }, [schemaTypes]);

  const onSchemaTypesChange = (val) => {
    setSchemaTypes(val.split(COMMON_DELIMITER));
  };

  return (
    <div>
      <div className={classes.outputInput} data-cy="explicit-schema-definer">
        <PluginInput
          widgetType={'textbox'}
          value={outputName}
          onChange={setOutputName}
          label={'Output name'}
          placeholder={'output name'}
        />
      </div>
      <div className={classes.outputInput}>
        <PluginInput
          widgetType={'multi-select'}
          onChange={onSchemaTypesChange}
          label={'Schema types'}
          delimiter={COMMON_DELIMITER}
          options={avroSchemaTypes}
          value={schemaTypes}
        />
      </div>
      <div className={classes.outputInput}>
        <PluginInput
          widgetType={'select'}
          value={schemaDefaultType}
          onChange={setSchemaDefaultType}
          label={'Schema default type'}
          options={schemaTypes}
        />
      </div>
    </div>
  );
};

const ExplicitSchemaDefiner = withStyles(styles)(ExplicitSchemaDefinerView);
export default ExplicitSchemaDefiner;

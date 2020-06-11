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
import Heading, { HeadingTypes } from 'components/Heading';
import If from 'components/If';
import { SchemaType } from 'components/PluginJSONCreator/constants';
import { useOutputState } from 'components/PluginJSONCreator/Create';
import ExplicitSchemaDefiner from 'components/PluginJSONCreator/Create/Content/OutputPage/ExplicitSchemaDefiner';
import ImplicitSchemaDefiner from 'components/PluginJSONCreator/Create/Content/OutputPage/ImplicitSchemaDefiner';
import PluginInput from 'components/PluginJSONCreator/Create/Content/PluginInput';
import StepButtons from 'components/PluginJSONCreator/Create/Content/StepButtons';
import * as React from 'react';

const styles = (): StyleRules => {
  return {
    schemaTypeSelector: {
      marginBottom: '30px',
    },
  };
};

const OutputPageView: React.FC<WithStyles<typeof styles>> = ({ classes }) => {
  const { outputWidgetType, setOutputWidgetType } = useOutputState();

  return (
    <div>
      <Heading type={HeadingTypes.h3} label="Output" />
      <br />
      <div className={classes.schemaTypeSelector}>
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
        <ExplicitSchemaDefiner />
      </If>
      <If condition={outputWidgetType === SchemaType.Implicit}>
        <ImplicitSchemaDefiner />
      </If>
      <StepButtons nextDisabled={false} />
    </div>
  );
};

const OutputPage = withStyles(styles)(OutputPageView);
export default OutputPage;

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
import If from 'components/If';
import { WIDGET_TYPES } from 'components/PluginJSONCreator/constants';
import PluginInput from 'components/PluginJSONCreator/Create/Content/PluginInput';
import * as React from 'react';

const styles = (): StyleRules => {
  return {
    widgetInput: {
      '& > *': {
        width: '100%',
        marginTop: '10px',
        marginBottom: '10px',
      },
    },
  };
};

const WidgetInputView: React.FC<WithStyles<typeof styles>> = ({
  classes,
  widgetObject,
  onNameChange,
  onLabelChange,
  onWidgetTypeChange,
  onWidgetCategoryChange,
}) => {
  return (
    <If condition={widgetObject}>
      <div className={classes.widgetInput}>
        <PluginInput
          widgetType={'textbox'}
          value={widgetObject.name}
          setValue={onNameChange}
          label={'Name'}
          placeholder={'Name a Widget'}
          required={true}
        />
        <PluginInput
          widgetType={'textbox'}
          value={widgetObject.label}
          setValue={onLabelChange}
          label={'Label'}
          placeholder={'Label a Widget'}
          required={true}
        />
        <PluginInput
          widgetType={'textbox'}
          value={widgetObject.widgetCategory}
          setValue={onWidgetCategoryChange}
          label={'Category'}
          placeholder={'Categorize a Widget'}
          required={false}
        />
        <PluginInput
          widgetType={'select'}
          value={widgetObject.widgetType}
          setValue={onWidgetTypeChange}
          label={'Widget Type'}
          options={WIDGET_TYPES}
          required={true}
        />
      </div>
    </If>
  );
};

const WidgetInput = withStyles(styles)(WidgetInputView);
export default WidgetInput;

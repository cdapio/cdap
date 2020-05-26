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

import IconButton from '@material-ui/core/IconButton';
import withStyles, { StyleRules, WithStyles } from '@material-ui/core/styles/withStyles';
import AddIcon from '@material-ui/icons/Add';
import DeleteIcon from '@material-ui/icons/Delete';
import If from 'components/If';
import { WIDGET_TYPES } from 'components/PluginJSONCreator/constants';
import PluginInput from 'components/PluginJSONCreator/Create/Content/PluginInput';
import * as React from 'react';

const styles = (): StyleRules => {
  return {
    widgetInput: {
      '& > *': {
        width: '80%',
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
  onAddWidget,
  onDeleteWidget,
}) => {
  return (
    <If condition={widgetObject}>
      <div className={classes.widgetInput}>
        <PluginInput
          widgetType={'textbox'}
          value={widgetObject.name}
          setValue={onNameChange}
          label={'Widget Name'}
          placeholder={'Name a Widget'}
          required={true}
        />
        <PluginInput
          widgetType={'textbox'}
          value={widgetObject.label}
          setValue={onLabelChange}
          label={'Widget Label'}
          placeholder={'Label a Widget'}
          required={true}
        />
        <PluginInput
          widgetType={'textbox'}
          value={widgetObject.widgetCategory}
          setValue={onWidgetCategoryChange}
          label={'Widget Category'}
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

      <div>
        <IconButton onClick={onAddWidget} data-cy="add-row">
          <AddIcon fontSize="small" />
        </IconButton>
        <IconButton onClick={onDeleteWidget} color="secondary" data-cy="remove-row">
          <DeleteIcon fontSize="small" />
        </IconButton>
      </div>
    </If>
  );
};

const WidgetInput = withStyles(styles)(WidgetInputView);
export default WidgetInput;

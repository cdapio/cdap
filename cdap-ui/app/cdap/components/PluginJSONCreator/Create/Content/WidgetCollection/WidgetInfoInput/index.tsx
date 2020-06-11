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
import {
  WIDGET_CATEGORY,
  WIDGET_TYPES,
  WIDGET_TYPE_TO_ATTRIBUTES,
} from 'components/PluginJSONCreator/constants';
import { useWidgetState } from 'components/PluginJSONCreator/Create';
import PluginInput from 'components/PluginJSONCreator/Create/Content/PluginInput';
import { fromJS } from 'immutable';
import * as React from 'react';

const styles = (): StyleRules => {
  return {
    widgetInput: {
      width: '100%',
      marginTop: '20px',
      marginBottom: '20px',
    },
    widgetField: {
      width: '100%',
      marginTop: '10px',
      marginBottom: '10px',
    },
  };
};

interface IWidgetInfoInputProps extends WithStyles<typeof styles> {
  widgetID: string;
}

const WidgetInfoInputView: React.FC<IWidgetInfoInputProps> = ({ classes, widgetID }) => {
  const { widgetInfo, setWidgetInfo, widgetToAttributes, setWidgetToAttributes } = useWidgetState();

  function onNameChange(id) {
    return (name) => {
      setWidgetInfo(fromJS(widgetInfo).setIn([id, 'name'], name));
    };
  }

  function onLabelChange(id) {
    return (label) => {
      setWidgetInfo(fromJS(widgetInfo).setIn([id, 'label'], label));
    };
  }

  function onWidgetTypeChange(id) {
    return (widgetType) => {
      setWidgetInfo(fromJS(widgetInfo).setIn([id, 'widgetType'], widgetType));

      setWidgetToAttributes(
        fromJS(widgetToAttributes).set(
          id,
          fromJS(
            Object.keys(WIDGET_TYPE_TO_ATTRIBUTES[widgetType]).reduce((acc, curr) => {
              acc[curr] = '';
              return acc;
            }, {})
          )
        )
      );
    };
  }

  function onWidgetCategoryChange(id) {
    return (widgetCategory) => {
      setWidgetInfo(fromJS(widgetInfo).setIn([id, 'widgetCategory'], widgetCategory));
    };
  }

  const widget = widgetInfo.get(widgetID);
  return React.useMemo(
    () => (
      <If condition={widget !== undefined}>
        <div className={classes.widgetInput}>
          <div className={classes.widgetField}>
            <PluginInput
              widgetType={'textbox'}
              value={widget.get('name')}
              onChange={onNameChange(widgetID)}
              label={'Name'}
              placeholder={'Name a Widget'}
              required={false}
            />
          </div>
          <div className={classes.widgetField}>
            <PluginInput
              widgetType={'textbox'}
              value={widget.get('label')}
              onChange={onLabelChange(widgetID)}
              label={'Label'}
              placeholder={'Label a Widget'}
              required={false}
            />
          </div>
          <div className={classes.widgetField}>
            <PluginInput
              widgetType={'select'}
              value={widget.get('widgetCategory')}
              onChange={onWidgetCategoryChange(widgetID)}
              label={'Category'}
              options={WIDGET_CATEGORY}
              required={false}
            />
          </div>
          <div className={classes.widgetField}>
            <PluginInput
              widgetType={'select'}
              value={widget.get('widgetType')}
              onChange={onWidgetTypeChange(widgetID)}
              label={'Widget Type'}
              options={WIDGET_TYPES}
              required={true}
            />
          </div>
        </div>
      </If>
    ),
    [widget]
  );
};

const WidgetInfoInput = withStyles(styles)(WidgetInfoInputView);
export default WidgetInfoInput;

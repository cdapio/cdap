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
<<<<<<< HEAD
import {
  WIDGET_CATEGORY,
  WIDGET_TYPES,
  WIDGET_TYPE_TO_ATTRIBUTES,
} from 'components/PluginJSONCreator/constants';
=======
import { WIDGET_TYPES } from 'components/PluginJSONCreator/constants';
>>>>>>> f3cdee8cbf7... [CDAP-16862] Define properties under each configuration group (plugin JSON creator)
import PluginInput from 'components/PluginJSONCreator/Create/Content/PluginInput';
import { ICreateContext } from 'components/PluginJSONCreator/CreateContextConnect';
import * as React from 'react';

const styles = (): StyleRules => {
  return {
    widgetInput: {
<<<<<<< HEAD
      width: '100%',
      marginTop: '20px',
      marginBottom: '20px',
    },
    widgetField: {
      width: '100%',
      marginTop: '10px',
      marginBottom: '10px',
=======
      '& > *': {
        width: '100%',
        marginTop: '10px',
        marginBottom: '10px',
      },
>>>>>>> f3cdee8cbf7... [CDAP-16862] Define properties under each configuration group (plugin JSON creator)
    },
  };
};

interface IWidgetInputProps extends WithStyles<typeof styles>, ICreateContext {
  widgetID: number;
}

const WidgetInputView: React.FC<IWidgetInputProps> = ({
  classes,
  widgetID,
<<<<<<< HEAD
  widgetInfo,
  setWidgetInfo,
  widgetToAttributes,
  setWidgetToAttributes,
}) => {
  function onNameChange() {
    return (name) => {
      setWidgetInfo((prevObjs) => ({
        ...prevObjs,
        [widgetID]: { ...prevObjs[widgetID], name },
      }));
    };
  }

  function onLabelChange() {
    return (label) => {
      setWidgetInfo((prevObjs) => ({
        ...prevObjs,
        [widgetID]: { ...prevObjs[widgetID], label },
      }));
    };
  }

  function onWidgetTypeChange() {
    return (widgetType) => {
      setWidgetInfo((prevObjs) => ({
        ...prevObjs,
        [widgetID]: { ...prevObjs[widgetID], widgetType },
      }));

      setWidgetToAttributes({
        ...widgetToAttributes,
        [widgetID]: Object.keys(WIDGET_TYPE_TO_ATTRIBUTES[widgetType]).reduce((acc, curr) => {
          acc[curr] = '';
          return acc;
        }, {}),
      });
    };
  }

  function onWidgetCategoryChange() {
    return (widgetCategory) => {
      setWidgetInfo((prevObjs) => ({
        ...prevObjs,
        [widgetID]: { ...prevObjs[widgetID], widgetCategory },
      }));
    };
  }

  return (
    <If condition={widgetInfo[widgetID]}>
      <div className={classes.widgetInput}>
        <div className={classes.widgetField}>
          <PluginInput
            widgetType={'textbox'}
            value={widgetInfo[widgetID].name}
            onChange={onNameChange()}
            label={'Name'}
            placeholder={'Name a Widget'}
            required={false}
          />
        </div>
        <div className={classes.widgetField}>
          <PluginInput
            widgetType={'textbox'}
            value={widgetInfo[widgetID].label}
            onChange={onLabelChange()}
            label={'Label'}
            placeholder={'Label a Widget'}
            required={false}
          />
        </div>
        <div className={classes.widgetField}>
          <PluginInput
            widgetType={'select'}
            value={widgetInfo[widgetID].widgetCategory}
            onChange={onWidgetCategoryChange()}
            label={'Category'}
            options={WIDGET_CATEGORY}
            required={false}
          />
        </div>
        <div className={classes.widgetField}>
          <PluginInput
            widgetType={'select'}
            value={widgetInfo[widgetID].widgetType}
            onChange={onWidgetTypeChange()}
            label={'Widget Type'}
            options={WIDGET_TYPES}
            required={true}
          />
        </div>
=======
  widgetToInfo,
  setWidgetToInfo,
}) => {
  function onNameChange(obj, id) {
    return (name) => {
      setWidgetToInfo((prevObjs) => ({ ...prevObjs, [id]: { ...obj, name } }));
    };
  }

  function onLabelChange(obj, id) {
    return (label) => {
      setWidgetToInfo((prevObjs) => ({ ...prevObjs, [id]: { ...obj, label } }));
    };
  }

  function onWidgetTypeChange(obj, id) {
    return (widgetType) => {
      setWidgetToInfo((prevObjs) => ({ ...prevObjs, [id]: { ...obj, widgetType } }));
    };
  }

  function onWidgetCategoryChange(obj, id) {
    return (widgetCategory) => {
      setWidgetToInfo((prevObjs) => ({ ...prevObjs, [id]: { ...obj, widgetCategory } }));
    };
  }

  const widgetInfo = widgetToInfo[widgetID];

  return (
    <If condition={widgetInfo}>
      <div className={classes.widgetInput}>
        <PluginInput
          widgetType={'textbox'}
          value={widgetInfo.name}
          setValue={onNameChange(widgetInfo, widgetID)}
          label={'Name'}
          placeholder={'Name a Widget'}
          required={false}
        />
        <PluginInput
          widgetType={'textbox'}
          value={widgetInfo.label}
          setValue={onLabelChange(widgetInfo, widgetID)}
          label={'Label'}
          placeholder={'Label a Widget'}
          required={false}
        />
        <PluginInput
          widgetType={'textbox'}
          value={widgetInfo.widgetCategory}
          setValue={onWidgetCategoryChange(widgetInfo, widgetID)}
          label={'Category'}
          placeholder={'Categorize a Widget'}
          required={false}
        />
        <PluginInput
          widgetType={'select'}
          value={widgetInfo.widgetType}
          setValue={onWidgetTypeChange(widgetInfo, widgetID)}
          label={'Widget Type'}
          options={WIDGET_TYPES}
          required={true}
        />
>>>>>>> f3cdee8cbf7... [CDAP-16862] Define properties under each configuration group (plugin JSON creator)
      </div>
    </If>
  );
};

const WidgetInput = withStyles(styles)(WidgetInputView);
export default WidgetInput;

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

import { StyleRules, withStyles, WithStyles } from '@material-ui/core/styles';
import { CustomOperator } from 'components/ConfigurationGroup/types';
import Heading, { HeadingTypes } from 'components/Heading';
import If from 'components/If';
import { OPERATOR_VALUES } from 'components/PluginJSONCreator/constants';
import PluginInput from 'components/PluginJSONCreator/Create/Content/PluginInput';
import { ICreateContext, IWidgetInfo } from 'components/PluginJSONCreator/CreateContextConnect';
import isNil from 'lodash/isNil';
import * as React from 'react';

const styles = (): StyleRules => {
  return {
    filterConditionInput: {
      marginTop: '10px',
      marginBottom: '10px',
    },
  };
};

export enum FilterConditionMode {
  Operator = 'operator',
  Expression = 'expression',
}

enum FilterConditionProperty {
  Expression = 'expression',
  Property = 'property',
  Operator = 'operator',
  Value = 'value',
}

interface IFilterConditionInputProps extends WithStyles<typeof styles>, ICreateContext {
  filterID: string;
}

const FilterConditionInputview: React.FC<IFilterConditionInputProps> = ({
  classes,
  filterID,
  filterToCondition,
  setFilterToCondition,
  widgetInfo,
}) => {
  const existingCondition = filterToCondition[filterID];

  const [conditionMode, setConditionMode] = React.useState(
    existingCondition.expression ? FilterConditionMode.Expression : FilterConditionMode.Operator
  );

  const allWidgetNames = widgetInfo
    ? Object.values(widgetInfo)
        .map((info: IWidgetInfo) => info.name)
        .filter((widgetName) => !isNil(widgetName))
    : [];

  React.useEffect(() => {
    // reset condition data
    if (conditionMode === FilterConditionMode.Expression) {
      setFilterToCondition((prevObjs) => ({
        ...prevObjs,
        [filterID]: { expression: existingCondition.expression },
      }));
    } else {
      setFilterToCondition((prevObjs) => ({
        ...prevObjs,
        [filterID]: {
          property: existingCondition.property,
          operator: existingCondition.operator,
          value: existingCondition.value,
        },
      }));
    }
  }, [conditionMode]);

  function setFilterCondition(conditionProperty: string) {
    return (val) => {
      setFilterToCondition((prevObjs) => ({
        ...prevObjs,
        [filterID]: { ...prevObjs[filterID], [conditionProperty]: val },
      }));
    };
  }

  return (
    <If condition={existingCondition}>
      <Heading type={HeadingTypes.h6} label="Show these widgets by the following condition..." />
      <div className={classes.filterConditionInput}>
        <PluginInput
          widgetType={'radio-group'}
          value={conditionMode}
          onChange={setConditionMode}
          label={'Condition Type'}
          options={Object.values(FilterConditionMode).map((mode) => ({ id: mode, label: mode }))}
          layout={'inline'}
        />
      </div>
      <If condition={conditionMode === FilterConditionMode.Expression}>
        <div className={classes.filterConditionInput}>
          <PluginInput
            widgetType={'textbox'}
            value={existingCondition.expression ? existingCondition.expression : ''}
            onChange={setFilterCondition(FilterConditionProperty.Expression)}
            label={FilterConditionProperty.Expression}
            required={false}
          />
        </div>
      </If>
      <If condition={conditionMode === FilterConditionMode.Operator}>
        <div className={classes.filterConditionInput}>
          <PluginInput
            widgetType={'select'}
            value={existingCondition.property}
            onChange={setFilterCondition(FilterConditionProperty.Property)}
            label={FilterConditionProperty.Property}
            options={allWidgetNames}
          />
        </div>
        <div className={classes.filterConditionInput}>
          <PluginInput
            widgetType={'select'}
            value={existingCondition.operator}
            onChange={setFilterCondition(FilterConditionProperty.Operator)}
            label={FilterConditionProperty.Operator}
            options={OPERATOR_VALUES}
          />
        </div>
        <If
          condition={
            existingCondition.operator === CustomOperator.EQUALTO ||
            existingCondition.operator === CustomOperator.NOTEQUALTO
          }
        >
          <div className={classes.filterConditionInput}>
            <PluginInput
              widgetType={'textbox'}
              value={existingCondition.value}
              onChange={setFilterCondition(FilterConditionProperty.Value)}
              label={FilterConditionProperty.Value}
              required={false}
            />
          </div>
        </If>
      </If>
    </If>
  );
};

const FilterConditionInput = withStyles(styles)(FilterConditionInputview);
export default FilterConditionInput;

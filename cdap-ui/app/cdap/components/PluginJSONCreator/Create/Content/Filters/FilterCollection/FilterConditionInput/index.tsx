/*
 * Copyright © 2018 Cask Data, Inc.
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
import { ICreateContext } from 'components/PluginJSONCreator/CreateContextConnect';
import { fromJS, List } from 'immutable';
import * as React from 'react';

const styles = (): StyleRules => {
  return {
    filterConditionInput: {
      '& > *': {
        marginTop: '10px',
        marginBottom: '10px',
      },
    },
  };
};

export enum FilterConditionMode {
  Operator = 'operator',
  Expression = 'expression',
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
  const existingCondition = filterToCondition.get(filterID);

  const [conditionMode, setConditionMode] = React.useState(
    existingCondition.get('expression')
      ? FilterConditionMode.Expression
      : FilterConditionMode.Operator
  );

  const allWidgetNames = widgetInfo
    ? widgetInfo
        .valueSeq()
        .map((info) => fromJS(info).get('name'))
        .filter((widgetName) => widgetName !== undefined && widgetName !== null)
    : List([]);

  React.useEffect(() => {
    // reset condition data
    if (conditionMode === FilterConditionMode.Expression) {
      setFilterToCondition(
        filterToCondition.set(
          filterID,
          fromJS({
            expression: existingCondition.get('expression') || '',
          })
        )
      );
    } else {
      setFilterToCondition(
        filterToCondition.set(
          filterID,
          fromJS({
            property: existingCondition.get('property') || '',
            operator: existingCondition.get('operator') || '',
            value: existingCondition.get('value') || '',
          })
        )
      );
    }
  }, [conditionMode]);

  function setFilterCondition(conditionProperty: string) {
    return (val) => {
      setFilterToCondition(fromJS(filterToCondition).setIn([filterID, conditionProperty], val));
    };
  }

  return (
    <If condition={existingCondition !== undefined}>
      <div className={classes.filterConditionInput}>
        <Heading type={HeadingTypes.h6} label="Show these widgets by the following condition..." />
        <PluginInput
          widgetType={'radio-group'}
          value={conditionMode}
          onChange={setConditionMode}
          label={'Condition Type'}
          options={Object.values(FilterConditionMode).map((mode) => ({ id: mode, label: mode }))}
          layout={'inline'}
        />
        <If condition={conditionMode === FilterConditionMode.Expression}>
          <PluginInput
            widgetType={'textbox'}
            value={existingCondition.get('expression')}
            onChange={setFilterCondition('expression')}
            label={'expression'}
            required={false}
          />
        </If>
        <If condition={conditionMode === FilterConditionMode.Operator}>
          <PluginInput
            widgetType={'select'}
            value={existingCondition.get('property')}
            onChange={setFilterCondition('property')}
            label={'property'}
            options={allWidgetNames}
          />
          <PluginInput
            widgetType={'select'}
            value={existingCondition.get('operator')}
            onChange={setFilterCondition('operator')}
            label={'operator'}
            options={OPERATOR_VALUES}
          />
          <If
            condition={
              existingCondition.get('operator') === CustomOperator.EQUALTO ||
              existingCondition.get('operator') === CustomOperator.NOTEQUALTO
            }
          >
            <PluginInput
              widgetType={'textbox'}
              value={existingCondition.get('value')}
              onChange={setFilterCondition('value')}
              label={'value'}
              required={false}
            />
          </If>
        </If>
      </div>
    </If>
  );
};

const FilterConditionInput = withStyles(styles)(FilterConditionInputview);
export default FilterConditionInput;

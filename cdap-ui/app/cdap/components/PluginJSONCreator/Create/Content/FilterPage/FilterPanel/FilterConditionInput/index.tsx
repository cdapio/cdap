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
import { useFilterState, useWidgetState } from 'components/PluginJSONCreator/Create';
import PluginInput from 'components/PluginJSONCreator/Create/Content/PluginInput';
import { fromJS, List } from 'immutable';
import * as React from 'react';

const styles = (): StyleRules => {
  return {
    filterConditionInput: {
      marginTop: '10px',
      marginBottom: '10px',
    },
  };
};

enum FilterConditionMode {
  Operator = 'operator',
  Expression = 'expression',
}

interface IFilterConditionInputProps extends WithStyles<typeof styles> {
  filterID: string;
}

const FilterConditionInputview: React.FC<IFilterConditionInputProps> = ({ classes, filterID }) => {
  const { widgetInfo } = useWidgetState();
  const { filterToCondition, setFilterToCondition } = useFilterState();

  const existingCondition = filterToCondition.get(filterID);

  // Set the initial 'conditionMode' based on the existing condition.
  const [conditionMode, setConditionMode] = React.useState(
    existingCondition.get('expression') !== undefined
      ? FilterConditionMode.Expression
      : FilterConditionMode.Operator
  );

  // All widget names are needed so that user can select the condition 'property' from this list.
  const allWidgetNames = widgetInfo
    ? widgetInfo
        .valueSeq()
        .map((info) => fromJS(info).get('name'))
        .filter((widgetName) => widgetName !== undefined && widgetName !== null)
    : List([]);

  // 'value' field in condition is required if 'operator' is 'equal to' or 'not equal to'.
  const valueFieldRequired =
    existingCondition.get('operator') === CustomOperator.EQUALTO ||
    existingCondition.get('operator') === CustomOperator.NOTEQUALTO;

  // When 'conditionMode' changes, 'filterToCondition' should change its fields.
  React.useEffect(() => {
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
            ...(valueFieldRequired && { value: existingCondition.get('value') || '' }),
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

  return React.useMemo(
    () => (
      <If condition={existingCondition !== undefined}>
        <Heading type={HeadingTypes.h5} label="Show these widgets by the following condition." />
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
        <div className={classes.filterConditionInput}>
          <If condition={conditionMode === FilterConditionMode.Expression}>
            <PluginInput
              widgetType={'textbox'}
              value={existingCondition.get('expression')}
              onChange={setFilterCondition('expression')}
              label={'expression'}
            />
          </If>
        </div>
        <If condition={conditionMode === FilterConditionMode.Operator}>
          <div className={classes.filterConditionInput}>
            <PluginInput
              widgetType={'select'}
              value={existingCondition.get('property')}
              onChange={setFilterCondition('property')}
              label={'property'}
              options={allWidgetNames}
            />
          </div>
          <div className={classes.filterConditionInput}>
            <PluginInput
              widgetType={'select'}
              value={existingCondition.get('operator')}
              onChange={setFilterCondition('operator')}
              label={'operator'}
              options={OPERATOR_VALUES}
            />
          </div>
          <div className={classes.filterConditionInput}>
            <If condition={valueFieldRequired}>
              <PluginInput
                widgetType={'textbox'}
                value={existingCondition.get('value')}
                onChange={setFilterCondition('value')}
                label={'value'}
              />
            </If>
          </div>
        </If>
      </If>
    ),
    [existingCondition, widgetInfo]
  );
};

const FilterConditionInput = withStyles(styles)(FilterConditionInputview);
export default FilterConditionInput;

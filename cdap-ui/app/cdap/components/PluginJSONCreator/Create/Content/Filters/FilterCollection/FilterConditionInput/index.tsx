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
import { CustomOperator } from 'components/ConfigurationGroup/types';
import Heading, { HeadingTypes } from 'components/Heading';
import If from 'components/If';
import { OPERATOR_VALUES } from 'components/PluginJSONCreator/constants';
import { useFilterState, useWidgetState } from 'components/PluginJSONCreator/Create';
import PluginInput from 'components/PluginJSONCreator/Create/Content/PluginInput';
import { List, Map } from 'immutable';
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

interface IFilterConditionInputProps extends WithStyles<typeof styles> {
  filterID: string;
}

const FilterConditionInputview: React.FC<IFilterConditionInputProps> = ({ classes, filterID }) => {
  const { widgetInfo } = useWidgetState();
  const { filterToCondition, setFilterToCondition } = useFilterState();

  const existingCondition = filterToCondition.get(filterID);

  const [conditionMode, setConditionMode] = React.useState(
    existingCondition.has('expression')
      ? FilterConditionMode.Expression
      : FilterConditionMode.Operator
  );

  const allWidgetNames = widgetInfo
    ? widgetInfo
        .valueSeq()
        .map((info) => info.get('name'))
        .filter((widgetName) => !isNil(widgetName))
    : List([]);

  const valueRequired =
    existingCondition.get('operator') === CustomOperator.EQUALTO ||
    existingCondition.get('operator') === CustomOperator.NOTEQUALTO;

  React.useEffect(() => {
    if (conditionMode === FilterConditionMode.Expression) {
      setFilterToCondition(
        filterToCondition.set(
          filterID,
          Map({
            expression: existingCondition.get('expression') || '',
          })
        )
      );
    } else {
      setFilterToCondition(
        filterToCondition.set(
          filterID,
          Map({
            property: existingCondition.get('property') || '',
            operator: existingCondition.get('operator') || '',
            ...(valueRequired && { value: existingCondition.get('value') || '' }),
          })
        )
      );
    }
  }, [conditionMode, valueRequired]);

  function setFilterCondition(conditionProperty: string) {
    return (val) => {
      setFilterToCondition(filterToCondition.setIn([filterID, conditionProperty], val));
    };
  }

  return React.useMemo(
    () => (
      <If condition={!isNil(existingCondition)}>
        <div className={classes.filterConditionInput}>
          <Heading
            type={HeadingTypes.h6}
            label="Show these widgets by the following condition..."
          />
          <div className={classes.filterConditionInput}>
            <PluginInput
              widgetType={'radio-group'}
              value={conditionMode}
              onChange={setConditionMode}
              label={'Condition Type'}
              options={Object.values(FilterConditionMode).map((mode) => ({
                id: mode,
                label: mode,
              }))}
              layout={'inline'}
            />
          </div>
          <If condition={conditionMode === FilterConditionMode.Expression}>
            <div className={classes.filterConditionInput}>
              <PluginInput
                widgetType={'textbox'}
                value={existingCondition.get(FilterConditionProperty.Expression)}
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
                value={existingCondition.get(FilterConditionProperty.Property)}
                onChange={setFilterCondition(FilterConditionProperty.Property)}
                label={FilterConditionProperty.Property}
                options={allWidgetNames}
              />
            </div>
            <div className={classes.filterConditionInput}>
              <PluginInput
                widgetType={'select'}
                value={existingCondition.get(FilterConditionProperty.Operator)}
                onChange={setFilterCondition(FilterConditionProperty.Operator)}
                label={FilterConditionProperty.Operator}
                options={OPERATOR_VALUES}
              />
            </div>
            <If condition={valueRequired}>
              <div className={classes.filterConditionInput}>
                <PluginInput
                  widgetType={'textbox'}
                  value={existingCondition.get(FilterConditionProperty.Value)}
                  onChange={setFilterCondition(FilterConditionProperty.Value)}
                  label={FilterConditionProperty.Value}
                  required={false}
                />
              </div>
            </If>
          </If>
        </div>
      </If>
    ),
    [filterToCondition, widgetInfo]
  );
};

const FilterConditionInput = withStyles(styles)(FilterConditionInputview);
export default FilterConditionInput;

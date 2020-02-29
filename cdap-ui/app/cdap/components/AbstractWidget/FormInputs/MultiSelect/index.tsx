/*
 * Copyright © 2019 Cask Data, Inc.
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

import React, { useState, useEffect } from 'react';
import Select from '@material-ui/core/Select';
import MenuItem from '@material-ui/core/MenuItem';
import { IWidgetProps } from 'components/AbstractWidget';
import { objectQuery } from 'services/helpers';
import { WIDGET_PROPTYPES } from 'components/AbstractWidget/constants';

export interface IOption {
  id: string;
  label: string;
}

interface IMultiSelectWidgetProps {
  delimiter?: string;
  options: IOption[];
  showSelectionCount?: boolean;
}

interface IMultiSelectProps extends IWidgetProps<IMultiSelectWidgetProps> {}

export default function MultiSelect({
  value,
  widgetProps,
  disabled,
  onChange,
  dataCy,
}: IMultiSelectProps) {
  const delimiter = objectQuery(widgetProps, 'delimiter') || ',';
  const options = objectQuery(widgetProps, 'options') || [];
  const showSelectionCount = objectQuery(widgetProps, 'showSelectionCount') || false;

  const initSelection = value.toString().split(delimiter);
  const [selections, setSelections] = useState<string[]>(initSelection);

  //  onChangeHandler takes array, turns it into string w/delimiter, and calls onChange on the string
  const onChangeHandler = (event: React.ChangeEvent<HTMLSelectElement>) => {
    const values = event.target.value as any; // it's expecting a string but multiple select returns an array
    const selectionsString = values.filter((val) => val).join(delimiter);
    setSelections(values);
    onChange(selectionsString);
  };

  useEffect(
    () => {
      const selection = value.toString().split(delimiter);
      setSelections(selection);
    },
    [value]
  );

  function renderValue(values: any) {
    if (selections.length === 0 || (selections.length === 1 && selections[0] === '')) {
      return '';
    }

    if (!showSelectionCount) {
      return selections
        .map((sel) => {
          const element = options.find((op) => op.id === sel);
          return element ? element.label : '';
        })
        .join(', ');
    }
    const selectionID = selections.find((el) => el !== '');
    const firstSelection = options.find((op) => op.id === selectionID);
    const selectionLabel = firstSelection ? firstSelection.label : '';

    let additionalSelectionCount = '';
    if (selections.length > 1) {
      additionalSelectionCount = `+${selections.length - 1}`;
    }
    return `${selectionLabel} ${additionalSelectionCount}`;
  }

  return (
    <Select
      multiple
      value={selections}
      onChange={onChangeHandler}
      disabled={disabled}
      renderValue={renderValue}
      inputProps={{
        'data-cy': dataCy,
      }}
    >
      {options.map((opt) => (
        <MenuItem value={opt.id} key={opt.id}>
          {opt.label}
        </MenuItem>
      ))}
    </Select>
  );
}

(MultiSelect as any).propTypes = WIDGET_PROPTYPES;

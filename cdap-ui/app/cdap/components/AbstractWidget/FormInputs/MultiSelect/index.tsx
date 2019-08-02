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
import React, { useState } from 'react';
import PropTypes from 'prop-types';
import Select from '@material-ui/core/Select';
import MenuItem from '@material-ui/core/MenuItem';

interface IMultiSelectProps {
  value: string;
  delimiter: string;
  onChange: (value: string) => void;
  options: IOption[];
}

interface IOption {
  id: string;
  label: string;
}

export default function MultiSelect({ value, delimiter, options, onChange }: IMultiSelectProps) {
  const initSelection = typeof value === 'string' ? value.split(delimiter) : value;
  const [selections, setSelections] = useState<string[]>(initSelection);

  //  onChangeHandler takes array, turns it into string w/delimiter, and calls onChange on the string
  const onChangeHandler = (event: React.ChangeEvent<HTMLSelectElement>) => {
    const values = event.target.value as any; // it's expecting a string but multiple select returns an array
    const selectionsString = values.join(delimiter);
    setSelections(values);
    onChange(selectionsString);
  };

  const optionValues = options.map((opt) => opt.label);

  return (
    <Select multiple value={selections} onChange={onChangeHandler}>
      {optionValues.map((opt) => (
        <MenuItem value={opt} key={opt}>
          {opt}
        </MenuItem>
      ))}
    </Select>
  );
}

(MultiSelect as any).propTypes = {
  value: PropTypes.string,
  delimiter: PropTypes.string,
  onChange: PropTypes.func,
  options: PropTypes.arrayOf(PropTypes.shape({ id: PropTypes.string, label: PropTypes.string })),
};

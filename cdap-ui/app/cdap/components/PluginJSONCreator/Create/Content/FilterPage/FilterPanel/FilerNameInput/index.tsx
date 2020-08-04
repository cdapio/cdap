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

import * as React from 'react';

import PluginInput from 'components/PluginJSONCreator/Create/Content/PluginInput';
import { useFilterState } from 'components/PluginJSONCreator/Create';

interface IFilterNameInputProps {
  filterID: string;
}

const FilterNameInput: React.FC<IFilterNameInputProps> = ({ filterID }) => {
  const { filterToName, setFilterToName } = useFilterState();

  function setFilterName(filterObjID: string) {
    return (name) => {
      setFilterToName(filterToName.set(filterObjID, name));
    };
  }

  return React.useMemo(
    () => (
      <PluginInput
        widgetType={'textbox'}
        value={filterToName.get(filterID)}
        onChange={setFilterName(filterID)}
        label={'Filter name'}
      />
    ),
    [filterToName]
  );
};

export default FilterNameInput;

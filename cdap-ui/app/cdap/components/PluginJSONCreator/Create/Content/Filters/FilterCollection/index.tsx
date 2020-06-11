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

import Button from '@material-ui/core/Button';
import If from 'components/If';
import { useFilterState } from 'components/PluginJSONCreator/Create';
import FilterInput from 'components/PluginJSONCreator/Create/Content/Filters/FilterCollection/FilterInput';
import { fromJS } from 'immutable';
import * as React from 'react';
import uuidV4 from 'uuid/v4';

const FilterCollection = () => {
  const {
    filters,
    setFilters,
    filterToName,
    setFilterToName,
    filterToCondition,
    setFilterToCondition,
    filterToShowList,
    setFilterToShowList,
    showToInfo,
    setShowToInfo,
  } = useFilterState();

  function addFilter(index: number) {
    return () => {
      const newFilterID = 'Filter_' + uuidV4();

      if (filters.size === 0) {
        setFilters(filters.insert(0, newFilterID));
      } else {
        setFilters(filters.insert(index + 1, newFilterID));
      }

      setFilterToName(filterToName.set(newFilterID, ''));
      setFilterToCondition(filterToCondition.set(newFilterID, fromJS({})));

      // put one empty show as a placeholder
      const newShowID = 'Show_' + uuidV4();
      setFilterToShowList(filterToShowList.set(newFilterID, fromJS([newShowID])));
      setShowToInfo(
        showToInfo.set(
          newShowID,
          fromJS({
            name: '',
            type: '',
          })
        )
      );
    };
  }

  function deleteFilter(index: number) {
    return () => {
      const filterToDelete = filters.get(index);

      const newFilters = filters.remove(index);
      setFilters(newFilters);

      const newFilterToName = fromJS(filterToName.delete(filterToDelete));
      setFilterToName(newFilterToName);

      const newFilterToCondition = fromJS(filterToCondition.delete(filterToDelete));
      setFilterToCondition(newFilterToCondition);

      const showlistToDelete = filterToShowList.get(filterToDelete);
      const newwFilterToShowlist = fromJS(filterToShowList.delete(filterToDelete));
      setFilterToShowList(newwFilterToShowlist);

      showlistToDelete.map((show) => {
        const newShowToInfo = fromJS(showToInfo.delete(show));
        setShowToInfo(newShowToInfo);
      });
    };
  }

  return (
    <div>
      {filters.map((filterID: string, filterIndex: number) => {
        return (
          <FilterInput
            filterID={filterID}
            addFilter={addFilter(filterIndex)}
            deleteFilter={deleteFilter(filterIndex)}
          />
        );
      })}
      <If condition={filters.size === 0}>
        <Button variant="contained" color="primary" onClick={addFilter(0)}>
          Add Filters
        </Button>
      </If>
    </div>
  );
};

export default React.memo(FilterCollection);

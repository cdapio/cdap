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
import Heading, { HeadingTypes } from 'components/Heading';
import If from 'components/If';
import { useFilterState } from 'components/PluginJSONCreator/Create';
import FilterPanel from 'components/PluginJSONCreator/Create/Content/FilterPage/FilterPanel';
import StepButtons from 'components/PluginJSONCreator/Create/Content/StepButtons';
import { fromJS } from 'immutable';
import * as React from 'react';
import uuidV4 from 'uuid/v4';

const FilterPage = () => {
  const {
    filters,
    setFilters,
    filterToName,
    setFilterToName,
    filterToCondition,
    setFilterToCondition,
    filterToShowlist,
    setFilterToShowlist,
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
      setFilterToShowlist(filterToShowlist.set(newFilterID, fromJS([newShowID])));
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

      const showlistToDelete = filterToShowlist.get(filterToDelete);
      const newFilterToShowlist = fromJS(filterToShowlist.delete(filterToDelete));
      setFilterToShowlist(newFilterToShowlist);

      showlistToDelete.map((show) => {
        const newShowToInfo = fromJS(showToInfo.delete(show));
        setShowToInfo(newShowToInfo);
      });
    };
  }

  return React.useMemo(
    () => (
      <div>
        <Heading type={HeadingTypes.h3} label="Filters" />
        <If condition={filters.size === 0}>
          <Button variant="contained" color="primary" onClick={addFilter(0)}>
            Add Filters
          </Button>
        </If>

        {filters.map((filterID: string, filterIndex: number) => {
          return (
            <FilterPanel
              key={filterID}
              filterID={filterID}
              addFilter={addFilter(filterIndex)}
              deleteFilter={deleteFilter(filterIndex)}
            />
          );
        })}
        <StepButtons nextDisabled={false} />
      </div>
    ),
    [filters]
  );
};

export default React.memo(FilterPage);

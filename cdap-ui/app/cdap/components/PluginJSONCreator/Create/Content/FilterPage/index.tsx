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

import Heading, { HeadingTypes } from 'components/Heading';
import { List, Map } from 'immutable';

import Button from '@material-ui/core/Button';
import FilterPanel from 'components/PluginJSONCreator/Create/Content/FilterPage/FilterPanel';
import If from 'components/If';
import StepButtons from 'components/PluginJSONCreator/Create/Content/StepButtons';
import { useFilterState } from 'components/PluginJSONCreator/Create';
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

  const [activeFilterIndex, setActiveFilterIndex] = React.useState(null);

  function addFilter(index: number) {
    return () => {
      const newFilterID = 'Filter_' + uuidV4();

      let newFilters;
      if (filters.size === 0) {
        newFilters = filters.insert(0, newFilterID);
      } else {
        newFilters = filters.insert(index + 1, newFilterID);
      }
      setFilters(newFilters);

      // Set the activeFilterIndex to the new filter's index
      if (newFilters.size <= 1) {
        setActiveFilterIndex(0);
      } else {
        setActiveFilterIndex(index + 1);
      }

      setFilterToName(filterToName.set(newFilterID, ''));
      setFilterToCondition(filterToCondition.set(newFilterID, Map({})));

      // put one empty show as a placeholder
      const newShowID = 'Show_' + uuidV4();
      setFilterToShowlist(filterToShowlist.set(newFilterID, List([newShowID])));
      setShowToInfo(
        showToInfo.set(
          newShowID,
          Map({
            name: '',
            type: '',
          })
        )
      );
    };
  }

  function deleteFilter(index: number) {
    return () => {
      setActiveFilterIndex(null);

      const filterToDelete = filters.get(index);

      const newFilters = filters.remove(index);
      setFilters(newFilters);

      const newFilterToName = filterToName.delete(filterToDelete);
      setFilterToName(newFilterToName);

      const newFilterToCondition = filterToCondition.delete(filterToDelete);
      setFilterToCondition(newFilterToCondition);

      const showlistToDelete = filterToShowlist.get(filterToDelete);
      const newwFilterToShowlist = filterToShowlist.delete(filterToDelete);
      setFilterToShowlist(newwFilterToShowlist);

      showlistToDelete.map((show) => {
        const newShowToInfo = showToInfo.delete(show);
        setShowToInfo(newShowToInfo);
      });
    };
  }

  const switchEditFilter = (index) => (event, newExpanded) => {
    if (newExpanded) {
      setActiveFilterIndex(index);
    } else {
      setActiveFilterIndex(null);
    }
  };

  return React.useMemo(
    () => (
      <div>
        <Heading type={HeadingTypes.h3} label="Filters" />
        <br />
        <If condition={filters.size === 0}>
          <Button
            variant="contained"
            color="primary"
            onClick={addFilter(0)}
            data-cy="add-filter-btn"
          >
            Add Filters
          </Button>
        </If>

        {filters.map((filterID: string, filterIndex: number) => {
          return (
            <FilterPanel
              filterIndex={filterIndex}
              key={filterID}
              filterID={filterID}
              addFilter={addFilter(filterIndex)}
              deleteFilter={deleteFilter(filterIndex)}
              filterExpanded={activeFilterIndex === filterIndex}
              switchEditFilter={switchEditFilter(filterIndex)}
            />
          );
        })}
        <StepButtons nextDisabled={false} />
      </div>
    ),
    [filters, filterToName, filterToCondition, filterToShowlist, activeFilterIndex]
  );
};

export default React.memo(FilterPage);

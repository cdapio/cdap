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

import If from 'components/If';
import { Map } from 'immutable';
import ShowPropertyRow from 'components/PluginJSONCreator/Create/Content/FilterPage/FilterPanel/FilterShowlistInput/ShowPropertyRow';
import { useFilterState } from 'components/PluginJSONCreator/Create';
import uuidV4 from 'uuid/v4';

interface IFilterShowlistInputProps {
  filterID: string;
}

const FilterShowlistInput: React.FC<IFilterShowlistInputProps> = ({ filterID }) => {
  const { filterToShowlist, setFilterToShowlist, showToInfo, setShowToInfo } = useFilterState();

  function setShowProperty(showID: string, property: string) {
    return (val) => {
      setShowToInfo(showToInfo.setIn([showID, property], val));
    };
  }

  function addShowToFilter(filterObjID: string, index: number) {
    return () => {
      const newShowID = 'Show_' + uuidV4();

      const showlist = filterToShowlist.get(filterObjID);
      const newShowlist = showlist.insert(index + 1, newShowID);
      setFilterToShowlist(filterToShowlist.set(filterObjID, newShowlist));

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

  function deleteShowFromFilter(filterObjID: string, index: number) {
    return () => {
      const showlist = filterToShowlist.get(filterObjID);
      const showToDelete = showlist.get(index);
      if (showlist.size > 1) {
        const newShowlist = showlist.remove(index);
        setFilterToShowlist(filterToShowlist.set(filterObjID, newShowlist));

        const newShowToInfo = showToInfo.delete(showToDelete);
        setShowToInfo(newShowToInfo);
      } else {
        // If there is only one widget left in the showlist, do not delete it.
        // Instead, reset its name and type.
        const newShowToInfo = showToInfo.set(showToDelete, Map({ name: '', type: '' }));
        setShowToInfo(newShowToInfo);
      }
    };
  }

  return React.useMemo(
    () => (
      <If condition={filterToShowlist.has(filterID)}>
        <Heading type={HeadingTypes.h6} label="Add widgets to configure" />
        {filterToShowlist.get(filterID).map((showID: string, showIndex: number) => {
          if (!showToInfo.has(showID)) {
            return null;
          }
          const show = showToInfo.get(showID);
          return (
            <div key={showID} data-cy={`show-${showIndex}`}>
              <ShowPropertyRow
                showName={show.get('name')}
                showType={show.get('type')}
                setShowName={setShowProperty(showID, 'name')}
                setShowType={setShowProperty(showID, 'type')}
                addShowToFilter={addShowToFilter(filterID, showIndex)}
                deleteShowFromFilter={deleteShowFromFilter(filterID, showIndex)}
              />
            </div>
          );
        })}
      </If>
    ),
    [filterToShowlist, showToInfo]
  );
};

export default FilterShowlistInput;

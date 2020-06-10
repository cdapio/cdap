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
import withStyles, { StyleRules, WithStyles } from '@material-ui/core/styles/withStyles';
import WidgetWrapper from 'components/ConfigurationGroup/WidgetWrapper';
import If from 'components/If';
import FilterConditionInput from 'components/PluginJSONCreator/Create/Content/Filters/FilterCollection/FilterConditionInput';
import FilterShowlistInput from 'components/PluginJSONCreator/Create/Content/Filters/FilterCollection/FilterShowlistInput';
import { ICreateContext } from 'components/PluginJSONCreator/CreateContextConnect';
import { fromJS, List } from 'immutable';
import * as React from 'react';
import uuidV4 from 'uuid/v4';

const styles = (theme): StyleRules => {
  return {
    filterInput: {
      display: 'block',
      marginLeft: 'auto',
      marginRight: 'auto',
      marginTop: '10px',
      marginBottom: '10px',
      '& > *': {
        marginTop: '15px',
        marginBottom: '15px',
      },
    },
    nestedFilters: {
      border: `1px solid`,
      borderColor: theme.palette.grey[300],
      borderRadius: '6px',
      position: 'relative',
      padding: '7px 10px 5px',
      margin: '25px',
    },
    filterContainer: {
      width: 'calc(100%-1000px)',
    },
  };
};

const FilterNameInput = ({ filterID, filterToName, setFilterToName }) => {
  function setFilterName(filterObjID: string) {
    return (name) => {
      setFilterToName((prevObjs) => prevObjs.set(filterObjID, name));
    };
  }

  const label = 'Filter Name';

  const widget = {
    label,
    'widget-type': 'textbox',
  };

  const property = {
    required: false,
    name: label,
  };

  return (
    <div>
      <WidgetWrapper
        widgetProperty={widget}
        pluginProperty={property}
        value={filterToName.get(filterID)}
        onChange={setFilterName(filterID)}
      />
    </div>
  );
};

const FilterCollectionView: React.FC<ICreateContext & WithStyles<typeof styles>> = ({
  classes,
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
  widgetInfo,
}) => {
  function addFilter(index: number) {
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
    setFilterToShowList(filterToShowList.set(newFilterID, List([newShowID])));
    setShowToInfo(
      showToInfo.set(
        newShowID,
        fromJS({
          name: '',
          type: '',
        })
      )
    );
  }

  function deleteFilter(index: number) {
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
  }

  return (
    <div>
      {filters.map((filterID: string, filterIndex: number) => {
        return (
          <div className={classes.nestedFilters} data-cy="widget-wrapper-container">
            <div className={classes.filterContainer}>
              <div>
                <Button variant="contained" color="primary" onClick={() => addFilter(filterIndex)}>
                  Add Filter
                </Button>
                <Button
                  variant="contained"
                  color="inherit"
                  onClick={() => deleteFilter(filterIndex)}
                >
                  Delete Filter
                </Button>
                <div className={classes.filterInput}>
                  <FilterNameInput
                    filterID={filterID}
                    filterToName={filterToName}
                    setFilterToName={setFilterToName}
                  />
                  <FilterShowlistInput
                    filterID={filterID}
                    filterToShowList={filterToShowList}
                    setFilterToShowList={setFilterToShowList}
                    showToInfo={showToInfo}
                    setShowToInfo={setShowToInfo}
                    widgetInfo={widgetInfo}
                  />
                  <FilterConditionInput
                    filterID={filterID}
                    filterToCondition={filterToCondition}
                    setFilterToCondition={setFilterToCondition}
                    widgetInfo={widgetInfo}
                  />
                </div>
              </div>
            </div>
          </div>
        );
      })}
      <If condition={filters.size === 0}>
        <Button variant="contained" color="primary" onClick={() => addFilter(0)}>
          Add Filter
        </Button>
      </If>
    </div>
  );
};

const FilterCollection = withStyles(styles)(FilterCollectionView);
export default FilterCollection;

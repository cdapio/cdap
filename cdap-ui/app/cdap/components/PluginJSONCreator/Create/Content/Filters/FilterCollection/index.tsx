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
import { IPropertyShowConfig } from 'components/ConfigurationGroup/types';
import WidgetWrapper from 'components/ConfigurationGroup/WidgetWrapper';
import If from 'components/If';
import FilterConditionInput from 'components/PluginJSONCreator/Create/Content/Filters/FilterCollection/FilterConditionInput';
import FilterShowlistInput from 'components/PluginJSONCreator/Create/Content/Filters/FilterCollection/FilterShowlistInput';
import { ICreateContext } from 'components/PluginJSONCreator/CreateContextConnect';
import * as React from 'react';
import uuidV4 from 'uuid/v4';

const styles = (theme): StyleRules => {
  return {
    filterInput: {
      display: 'block',
      marginTop: '30px',
      marginBottom: '30px',
    },
    nestedFilters: {
      border: `1px solid ${theme.palette.grey[300]}`,
      borderRadius: '6px',
      position: 'relative',
      padding: '7px',
      margin: '25px',
    },
    filterContainer: {
      width: '90%',
    },
  };
};

const FilterNameInput = ({ filterID, filterToName, setFilterToName }) => {
  function setFilterName(filterObjID: string) {
    return (name) => {
      setFilterToName((prevObjs) => ({ ...prevObjs, [filterObjID]: name }));
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
    <WidgetWrapper
      widgetProperty={widget}
      pluginProperty={property}
      value={filterToName[filterID]}
      onChange={setFilterName(filterID)}
    />
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

    const newFilters = [...filters];

    if (newFilters.length === 0) {
      newFilters.splice(0, 0, newFilterID);
    } else {
      newFilters.splice(index + 1, 0, newFilterID);
    }

    setFilters(newFilters);

    setFilterToName({ ...filterToName, [newFilterID]: '' });
    setFilterToCondition({ ...filterToCondition, [newFilterID]: {} });

    // put one empty show as a placeholder
    const newShowID = 'Show_' + uuidV4();
    setFilterToShowList({
      ...filterToShowList,
      [newFilterID]: [newShowID],
    });
    setShowToInfo({
      ...showToInfo,
      [newShowID]: {
        name: '',
      } as IPropertyShowConfig,
    });
  }

  function deleteFilter(index: number) {
    const newFilters = [...filters];
    const filterToDelete = newFilters[index];
    newFilters.splice(index, 1);
    setFilters(newFilters);

    const { [filterToDelete]: name, ...restFilterToName } = filterToName;
    setFilterToName(restFilterToName);

    const { [filterToDelete]: condition, ...restFilterToCondition } = filterToCondition;
    setFilterToCondition(restFilterToCondition);

    const { [filterToDelete]: showList, ...restFilterToShowList } = filterToShowList;
    setFilterToShowList(restFilterToShowList);

    showList.map((show) => {
      const { [show]: info, ...rest } = showToInfo;
      setShowToInfo(rest);
    });
  }

  return (
    <div>
      {filters.map((filterID: string, filterIndex: number) => {
        return (
          <div key={filterID} className={classes.nestedFilters}>
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
                </div>
                <div className={classes.filterInput}>
                  <FilterShowlistInput
                    filterID={filterID}
                    filterToShowList={filterToShowList}
                    setFilterToShowList={setFilterToShowList}
                    showToInfo={showToInfo}
                    setShowToInfo={setShowToInfo}
                    widgetInfo={widgetInfo}
                  />
                </div>
                <div className={classes.filterInput}>
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
      <If condition={filters.length === 0}>
        <Button variant="contained" color="primary" onClick={() => addFilter(0)}>
          Add Filter
        </Button>
      </If>
    </div>
  );
};

const FilterCollection = withStyles(styles)(FilterCollectionView);
export default FilterCollection;

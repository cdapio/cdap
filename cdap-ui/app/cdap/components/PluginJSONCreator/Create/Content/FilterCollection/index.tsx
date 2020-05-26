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

import { IconButton } from '@material-ui/core';
import Button from '@material-ui/core/Button';
import Divider from '@material-ui/core/Divider';
import withStyles, { StyleRules, WithStyles } from '@material-ui/core/styles/withStyles';
import AddIcon from '@material-ui/icons/Add';
import DeleteIcon from '@material-ui/icons/Delete';
import { IPropertyShowConfig } from 'components/ConfigurationGroup/types';
import WidgetWrapper from 'components/ConfigurationGroup/WidgetWrapper';
import If from 'components/If';
import {
  FILTER_CONDITION_PROPERTIES,
  OPERATOR_VALUES,
  SHOW_TYPE_VALUES,
} from 'components/PluginJSONCreator/constants';
import PluginInput from 'components/PluginJSONCreator/Create/Content/PluginInput';
import * as React from 'react';
import uuidV4 from 'uuid/v4';

const styles = (theme: { palette: { grey: any[]; white: any[] } }): StyleRules => {
  return {
    eachFilter: {
      display: 'block',
      marginLeft: 'auto',
      marginRight: 'auto',
      marginTop: '10px',
      marginBottom: '10px',
      // padding: '7px 10px 5px',
      '& > *': {
        //  margin: '6px',
        marginTop: '15px',
        marginBottom: '15px',
      },
    },
    widgetInput: {
      '& > *': {
        width: '80%',
        marginTop: '10px',
        marginBottom: '10px',
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
    label: {
      fontSize: '12px',
      position: 'absolute',
      top: '-10px',
      left: '15px',
      padding: '0 5px',
      backgroundColor: theme.palette.white[50],
    },
    required: {
      fontSize: '14px',
      marginLeft: '5px',
      lineHeight: '12px',
      verticalAlign: 'middle',
    },
    filterContainer: {
      width: 'calc(100%-1000px)',
    },
    filterDivider: {
      width: '100%',
    },
  };
};

const FilterNameInput = ({ filterID, filterToName, setFilterToName }) => {
  function setFilterName(filterID: string) {
    return (name) => {
      setFilterToName((prevObjs) => ({ ...prevObjs, [filterID]: name }));
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

const FilterConditionInput = ({ filterID, filterToCondition, setFilterToCondition }) => {
  function setFilterCondition(filterID: string, conditionProperty: string) {
    return (val) => {
      setFilterToCondition((prevObjs) => ({
        ...prevObjs,
        [filterID]: { ...prevObjs[filterID], [conditionProperty]: val },
      }));
    };
  }

  return (
    <div>
      {FILTER_CONDITION_PROPERTIES.map((conditionProperty: string) => {
        const widget = {
          label: conditionProperty,
          name: conditionProperty,
          'widget-type': 'textbox',
        };

        const property = {
          required: false,
          name: conditionProperty,
        };

        return (
          <div>
            <If condition={conditionProperty === 'operator'}>
              <PluginInput
                widgetType={'select'}
                value={filterToCondition[filterID][conditionProperty]}
                setValue={setFilterCondition(filterID, conditionProperty)}
                label={conditionProperty}
                options={OPERATOR_VALUES}
                required={false}
              />
            </If>

            <If condition={conditionProperty !== 'operator'}>
              <WidgetWrapper
                widgetProperty={widget}
                pluginProperty={property}
                value={filterToCondition[filterID][conditionProperty]}
                onChange={setFilterCondition(filterID, conditionProperty)}
              />
            </If>
          </div>
        );
      })}
    </div>
  );
};

const FilterShowListInput = ({
  filterID,
  filterToShowList,
  setFilterToShowList,
  showToInfo,
  setShowToInfo,
}) => {
  function setShowProperty(showID: string, property: string) {
    return (val) => {
      setShowToInfo((prevObjs) => ({
        ...prevObjs,
        [showID]: { ...prevObjs[showID], [property]: val },
      }));
    };
  }

  function addShowToFilter(filterID: string, index?: number) {
    const newShowID = 'Show_' + uuidV4();

    setShowToInfo({
      ...showToInfo,
      [newShowID]: {
        name: '',
      } as IPropertyShowConfig,
    });

    if (index === undefined) {
      setFilterToShowList({
        ...filterToShowList,
        [filterID]: [...filterToShowList[filterID], newShowID],
      });
    } else {
      const showList = filterToShowList[filterID];

      if (showList.length == 0) {
        showList.splice(0, 0, newShowID);
      } else {
        showList.splice(index + 1, 0, newShowID);
      }

      setFilterToShowList({
        ...filterToShowList,
        [filterID]: showList,
      });
    }
  }

  function deleteShowFromFilter(filterID: string, index: number) {
    const showList = filterToShowList[filterID];

    const showToDelete = showList[index];

    showList.splice(index, 1);

    setFilterToShowList({
      ...filterToShowList,
      [filterID]: showList,
    });

    const { [showToDelete]: tmp, ...restShowToInfo } = showToInfo;
    setShowToInfo(restShowToInfo);
  }

  return (
    <If condition={filterToShowList[filterID]}>
      {filterToShowList[filterID].map((showID: string, showIndex: number) => {
        return (
          <div>
            <PluginInput
              widgetType={'textbox'}
              value={showToInfo[showID].name}
              setValue={setShowProperty(showID, 'name')}
              label={'name'}
              required={true}
            />
            <PluginInput
              widgetType={'select'}
              value={showToInfo[showID].type}
              setValue={setShowProperty(showID, 'type')}
              options={SHOW_TYPE_VALUES}
              label={'type'}
              required={false}
            />
            <IconButton onClick={() => addShowToFilter(filterID, showIndex)} data-cy="add-row">
              <AddIcon fontSize="small" />
            </IconButton>
            <IconButton
              onClick={() => deleteShowFromFilter(filterID, showIndex)}
              color="secondary"
              data-cy="remove-row"
            >
              <DeleteIcon fontSize="small" />
            </IconButton>
            <Divider />
          </div>
        );
      })}
    </If>
  );
};

const FilterCollectionView: React.FC<WithStyles<typeof styles>> = ({
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
}) => {
  const [activeFilterIndex, setActiveFilterIndex] = React.useState(null);

  function addFilter(index: number) {
    const newFilterID = 'Filter_' + uuidV4();

    const newFilters = [...filters];

    if (newFilters.length == 0) {
      newFilters.splice(0, 0, newFilterID);
    } else {
      newFilters.splice(index + 1, 0, newFilterID);
    }

    setFilters(newFilters);

    if (newFilters.length <= 1) {
      setActiveFilterIndex(0);
    } else {
      setActiveFilterIndex(index + 1);
    }

    setFilterToName({ ...filterToName, [newFilterID]: '' });
    setFilterToCondition({ ...filterToCondition, [newFilterID]: {} });

    // put one empty show as a placeholder
    const newShowID = 'Show_' + uuidV4();
    setFilterToShowList({
      ...filterToShowList,
      [newFilterID]: [
        ...(filterToShowList && Object.keys(filterToShowList).length > 0
          ? filterToShowList[newFilterID]
          : []),
        newShowID,
      ],
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
    <div className={classes.nestedFilters} data-cy="widget-wrapper-container">
      <If condition={true}>
        <div className={`widget-wrapper-label ${classes.label}`}>
          Add Widgets
          <span className={classes.required}>*</span>
        </div>
      </If>
      <div className={classes.filterContainer}>
        {filters.map((filterID: string, filterIndex: number) => {
          return (
            <div>
              <Button variant="contained" color="primary" onClick={() => addFilter(filterIndex)}>
                Add Filter
              </Button>
              <Button variant="contained" color="primary" onClick={() => deleteFilter(filterIndex)}>
                Delete Filter
              </Button>
              <div className={classes.eachFilter}>
                <FilterNameInput
                  filterID={filterID}
                  filterToName={filterToName}
                  setFilterToName={setFilterToName}
                />
              </div>
              <div className={classes.eachFilter}>
                <FilterConditionInput
                  filterID={filterID}
                  filterToCondition={filterToCondition}
                  setFilterToCondition={setFilterToCondition}
                />
              </div>
              <div className={classes.eachFilter}>
                <FilterShowListInput
                  filterID={filterID}
                  filterToShowList={filterToShowList}
                  setFilterToShowList={setFilterToShowList}
                  showToInfo={showToInfo}
                  setShowToInfo={setShowToInfo}
                />
              </div>
              <Divider className={classes.filterDivider} />
            </div>
          );
        })}

        <If condition={filters.length == 0}>
          <Button variant="contained" color="primary" onClick={() => addFilter(0)}>
            Add Filter
          </Button>
        </If>
      </div>
    </div>
  );
};

const FilterCollection = withStyles(styles)(FilterCollectionView);
export default FilterCollection;

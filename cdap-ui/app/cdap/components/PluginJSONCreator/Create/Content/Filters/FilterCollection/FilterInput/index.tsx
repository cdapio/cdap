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

import { Button } from '@material-ui/core';
import withStyles, { StyleRules, WithStyles } from '@material-ui/core/styles/withStyles';
import WidgetWrapper from 'components/ConfigurationGroup/WidgetWrapper';
import { useFilterState } from 'components/PluginJSONCreator/Create';
import FilterConditionInput from 'components/PluginJSONCreator/Create/Content/Filters/FilterCollection/FilterConditionInput';
import FilterShowlistInput from 'components/PluginJSONCreator/Create/Content/Filters/FilterCollection/FilterShowlistInput';
import * as React from 'react';

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

const FilterNameInput = ({ filterID }) => {
  const { filterToName, setFilterToName } = useFilterState();

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

  return React.useMemo(
    () => (
      <div>
        <WidgetWrapper
          widgetProperty={widget}
          pluginProperty={property}
          value={filterToName.get(filterID)}
          onChange={setFilterName(filterID)}
        />
      </div>
    ),
    [filterToName.get(filterID)]
  );
};

interface IFilterInputProps extends WithStyles<typeof styles> {
  filterID: string;
  addFilter: () => void;
  deleteFilter: () => void;
}

const FilterInputView: React.FC<IFilterInputProps> = ({
  classes,
  filterID,
  addFilter,
  deleteFilter,
}) => {
  return (
    <div className={classes.nestedFilters} data-cy="widget-wrapper-container">
      <div className={classes.filterContainer}>
        <div>
          <Button variant="contained" color="primary" onClick={addFilter}>
            Add Filter
          </Button>
          <Button variant="contained" color="inherit" onClick={deleteFilter}>
            Delete Filter
          </Button>
          <div className={classes.filterInput}>
            <FilterNameInput filterID={filterID} />
          </div>
          <div className={classes.filterInput}>
            <FilterShowlistInput filterID={filterID} />
          </div>
          <div className={classes.filterInput}>
            <FilterConditionInput filterID={filterID} />
          </div>
        </div>
      </div>
    </div>
  );
};

const FilterInput = withStyles(styles)(FilterInputView);
export default React.memo(FilterInput);

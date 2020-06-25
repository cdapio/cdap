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
import FilterNameInput from 'components/PluginJSONCreator/Create/Content/FilterPage/FilterPanel/FilerNameInput';
import FilterConditionInput from 'components/PluginJSONCreator/Create/Content/FilterPage/FilterPanel/FilterConditionInput';
import FilterShowlistInput from 'components/PluginJSONCreator/Create/Content/FilterPage/FilterPanel/FilterShowlistInput';
import * as React from 'react';

const styles = (theme): StyleRules => {
  return {
    filterContainer: {
      border: `1px solid`,
      borderColor: theme.palette.grey[300],
      borderRadius: '6px',
      position: 'relative',
      padding: '7px 10px 5px',
      margin: '25px',
    },
    filterInput: {
      display: 'block',
      marginLeft: 'auto',
      marginRight: 'auto',
      marginTop: '30px',
      marginBottom: '30px',
    },
  };
};

interface IFilterPanelProps extends WithStyles<typeof styles> {
  filterID: string;
  addFilter: () => void;
  deleteFilter: () => void;
}

const FilterPanelView: React.FC<IFilterPanelProps> = ({
  classes,
  filterID,
  addFilter,
  deleteFilter,
}) => {
  return (
    <div className={classes.filterContainer}>
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
  );
};

const FilterPanel = withStyles(styles)(FilterPanelView);
export default FilterPanel;

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

import withStyles, { StyleRules, WithStyles } from '@material-ui/core/styles/withStyles';

import AddIcon from '@material-ui/icons/Add';
import DeleteIcon from '@material-ui/icons/Delete';
import ExpandMoreIcon from '@material-ui/icons/ExpandMore';
import ExpansionPanel from '@material-ui/core/ExpansionPanel';
import ExpansionPanelActions from '@material-ui/core/ExpansionPanelActions';
import ExpansionPanelSummary from '@material-ui/core/ExpansionPanelSummary';
import FilterConditionInput from 'components/PluginJSONCreator/Create/Content/FilterPage/FilterPanel/FilterConditionInput';
import FilterNameInput from 'components/PluginJSONCreator/Create/Content/FilterPage/FilterPanel/FilerNameInput';
import FilterShowlistInput from 'components/PluginJSONCreator/Create/Content/FilterPage/FilterPanel/FilterShowlistInput';
import IconButton from '@material-ui/core/IconButton';
import If from 'components/If';
import Typography from '@material-ui/core/Typography';
import { useFilterState } from 'components/PluginJSONCreator/Create';

const styles = (theme): StyleRules => {
  return {
    filterContainer: {
      position: 'relative',
      display: 'grid',
      gridTemplateColumns: '5fr 1fr',
    },
    filterContent: {
      display: 'block',
      padding: `0 ${theme.spacing(4)}px`,
    },
    filterInput: {
      display: 'block',
      marginLeft: 'auto',
      marginRight: 'auto',
      marginTop: theme.spacing(3),
      marginBottom: theme.spacing(3),
    },
  };
};

interface IFilterPanelProps extends WithStyles<typeof styles> {
  filterIndex: number;
  filterID: string;
  addFilter: () => void;
  deleteFilter: () => void;
  filterExpanded: boolean;
  switchEditFilter: () => void;
}

const FilterPanelView: React.FC<IFilterPanelProps> = ({
  classes,
  filterIndex,
  filterID,
  addFilter,
  deleteFilter,
  filterExpanded,
  switchEditFilter,
}) => {
  const { filterToName } = useFilterState();

  return (
    <div className={classes.filterContainer} data-cy={`filter-panel-${filterIndex}`}>
      <ExpansionPanel
        expanded={filterExpanded}
        onChange={switchEditFilter}
        data-cy={`open-filter-panel-${filterIndex}`}
      >
        <ExpansionPanelSummary expandIcon={<ExpandMoreIcon />} id="panel1c-header">
          <Typography className={classes.heading}>{filterToName.get(filterID)}</Typography>
        </ExpansionPanelSummary>
        <ExpansionPanelActions className={classes.filterContent}>
          <If condition={filterExpanded}>
            <div className={classes.filterInput}>
              <FilterNameInput filterID={filterID} data-cy="filter-name" />
            </div>
            <div className={classes.filterInput} data-cy="filter-showlist-input">
              <FilterShowlistInput filterID={filterID} />
            </div>
            <div className={classes.filterInput} data-cy="filter-condition-input">
              <FilterConditionInput filterID={filterID} />
            </div>
          </If>
        </ExpansionPanelActions>
      </ExpansionPanel>
      <div>
        <IconButton onClick={addFilter} data-cy="add-filter-btn">
          <AddIcon fontSize="small" />
        </IconButton>
        <IconButton onClick={deleteFilter} color="secondary" data-cy="delete-filter-btn">
          <DeleteIcon fontSize="small" />
        </IconButton>
      </div>
    </div>
  );
};

const FilterPanel = withStyles(styles)(FilterPanelView);
export default FilterPanel;

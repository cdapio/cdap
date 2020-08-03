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

import withStyles, { StyleRules, WithStyles } from '@material-ui/core/styles/withStyles';

import IconButton from '@material-ui/core/IconButton';
import InputBase from '@material-ui/core/InputBase';
import Paper from '@material-ui/core/Paper';
import React from 'react';
import SearchIcon from '@material-ui/icons/Search';

const styles = (theme): StyleRules => {
  return {
    root: {
      padding: '2px 4px',
      display: 'flex',
      alignItems: 'center',
      width: '100%',
      borderTop: `1px solid ${theme.palette.grey[300]}`,
      borderBottom: `1px solid ${theme.palette.grey[300]}`,
    },
    searchInput: {
      marginLeft: theme.spacing(1),
      flex: 1,
    },
    searchIcon: {
      padding: 10,
    },
  };
};

interface IRequestSearchProps extends WithStyles<typeof styles> {
  searchText: string;
  setSearchText: (searchText: string) => void;
}

const RequestSearchView: React.FC<IRequestSearchProps> = ({
  classes,
  searchText,
  setSearchText,
}) => {
  return (
    <Paper className={classes.root} elevation={0}>
      <IconButton type="submit" className={classes.searchIcon}>
        <SearchIcon />
      </IconButton>
      <InputBase
        className={classes.searchInput}
        placeholder="Search"
        value={searchText}
        onChange={(e) => setSearchText(e.target.value)}
        data-cy="request-search-input"
      />
    </Paper>
  );
};

const RequestSearch = withStyles(styles)(RequestSearchView);
export default RequestSearch;

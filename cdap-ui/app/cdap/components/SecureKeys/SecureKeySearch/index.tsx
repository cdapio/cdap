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

import CloseIcon from '@material-ui/icons/Close';
import FormControl from '@material-ui/core/FormControl';
import IconButton from '@material-ui/core/IconButton';
import InputAdornment from '@material-ui/core/InputAdornment';
import React from 'react';
import Search from '@material-ui/icons/Search';
import TextField from '@material-ui/core/TextField';
import classnames from 'classnames';

const styles = (theme): StyleRules => {
  return {
    margin: {
      margin: theme.spacing(1),
    },
    textField: {
      width: '30ch',
    },
  };
};

interface ISecureKeySearchProps extends WithStyles<typeof styles> {
  searchText: string;
  setSearchText: (searchText: string) => void;
}

const SecureKeySearchView: React.FC<ISecureKeySearchProps> = ({
  classes,
  searchText,
  setSearchText,
}) => {
  return (
    <FormControl className={classnames(classes.margin, classes.textField)} variant="outlined">
      <TextField
        className={classes.margin}
        value={searchText}
        onChange={(e) => setSearchText(e.target.value)}
        placeholder={'Search secure keys'}
        InputProps={{
          startAdornment: (
            <InputAdornment position="start">
              <IconButton>
                <Search />
              </IconButton>
            </InputAdornment>
          ),
          endAdornment: (
            <InputAdornment position="end">
              <IconButton onClick={() => setSearchText('')}>
                <CloseIcon />
              </IconButton>
            </InputAdornment>
          ),
        }}
        data-cy="search-secure-key"
      />
    </FormControl>
  );
};

const SecureKeySearch = withStyles(styles)(SecureKeySearchView);
export default SecureKeySearch;

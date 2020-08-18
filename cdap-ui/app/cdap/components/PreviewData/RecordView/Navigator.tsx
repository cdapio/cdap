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

import React, { useEffect, useState } from 'react';
import withStyles, { WithStyles, StyleRules } from '@material-ui/core/styles/withStyles';
import ArrowRightIcon from '@material-ui/icons/ArrowRight';
import ArrowLeftIcon from '@material-ui/icons/ArrowLeft';
import IconButton from '@material-ui/core/IconButton';
import Select from 'components/AbstractWidget/FormInputs/Select';

const styles = (theme): StyleRules => ({
  root: {
    display: 'flex',
    justifyContent: 'space-between',
    padding: '0px 10px',
    borderBottom: `1px solid ${theme.palette.grey[400]}`,
  },
  select: {
    display: 'flex',
    alignItems: 'center',
    width: '120px',
  },
});

interface IRecordNavigatorProps extends WithStyles<typeof styles> {
  selectedRecord: number;
  numRecords: number;
  updateRecord: (newVal: string) => void;
  prevOperation: () => void;
  nextOperation: () => void;
}

const RecordNavigatorBase: React.FC<IRecordNavigatorProps> = ({
  classes,
  selectedRecord,
  numRecords,
  updateRecord,
  prevOperation,
  nextOperation,
}) => {
  const prevDisabled = selectedRecord - 1 < 1;
  const nextDisabled = selectedRecord + 1 > numRecords;

  const createSelectOptions = () => {
    const options = [];
    for (let i = 1; i <= numRecords; i++) {
      const value = `Record ${i}`;
      options.push({ value, label: value });
    }
    return options;
  };

  const [selectOptions, setOptions] = useState([]);

  useEffect(() => {
    setOptions(createSelectOptions());
  }, []);

  return (
    <div className={classes.root} data-cy="record-navigator">
      <IconButton
        onClick={!prevDisabled ? prevOperation : undefined}
        disabled={prevDisabled}
        data-cy="previous-record-btn"
      >
        <ArrowLeftIcon fontSize="large" />
      </IconButton>
      <span className={classes.select} data-cy="record-dropdown">
        <Select
          value={`Record ${selectedRecord}`}
          onChange={(e) => updateRecord(e)}
          widgetProps={{ options: selectOptions }}
        />
      </span>
      <IconButton
        onClick={!nextDisabled ? nextOperation : undefined}
        disabled={nextDisabled}
        data-cy="next-record-btn"
      >
        <ArrowRightIcon fontSize="large" />
      </IconButton>
    </div>
  );
};

const RecordNavigator = withStyles(styles)(RecordNavigatorBase);

export default RecordNavigator;

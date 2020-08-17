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

import Button from '@material-ui/core/Button';
import Paper from '@material-ui/core/Paper';
import SecureKeyActionButtons from 'components/SecureKeys/SecureKeyList/SecureKeyActionButtons';
import SecureKeyCreate from 'components/SecureKeys/SecureKeyCreate';
import SecureKeySearch from 'components/SecureKeys/SecureKeySearch';
import Table from '@material-ui/core/Table';
import TableBody from '@material-ui/core/TableBody';
import TableCell from '@material-ui/core/TableCell';
import TableHead from '@material-ui/core/TableHead';
import TableRow from '@material-ui/core/TableRow';

export const CustomTableCell = withStyles((theme) => ({
  head: {
    backgroundColor: theme.palette.grey['300'],
    color: theme.palette.common.white,
    padding: '5px 10px',
    fontSize: 13,
    '&:first-of-type': {
      borderRight: `1px solid ${theme.palette.grey['500']}`,
    },
  },
  body: {
    padding: '5px 10px',
    fontSize: 13,
    '&:first-of-type': {
      borderRight: `1px solid ${theme.palette.grey['500']}`,
    },
  },
}))(TableCell);

const styles = (theme): StyleRules => {
  return {
    secureKeysTitle: {
      fontSize: '20px',
      paddingTop: theme.spacing(1),
    },
    secureKeyManager: {
      display: 'grid',
      alignItems: 'center',
      gridTemplateColumns: 'repeat(7, 1fr)',
    },
    addSecureKeyButton: {
      gridRow: '1',
      gridColumnStart: '1',
      textTransform: 'none',
      padding: '7px',
      fontSize: '13px',
    },
    secureKeySearch: {
      gridRow: '1',
      gridColumnStart: '7',
    },
    root: {
      width: '100%',
      display: 'inline-block',
      height: 'auto',
      marginTop: theme.spacing(1),
    },
    row: {
      height: 40,
      '&:nth-of-type(odd)': {
        backgroundColor: theme.palette.grey['600'],
      },
      cursor: 'pointer',
      hover: {
        cursor: 'pointer',
      },
    },
    nameCell: {
      width: '30%',
    },
    descriptionCell: {
      width: '60%',
    },
    actionButtonsCell: {
      width: '10%',
    },
  };
};

interface ISecureKeyListProps extends WithStyles<typeof styles> {
  state: any;
  alertSuccess: () => void;
  alertFailure: () => void;
  openEditDialog: (index: number) => void;
  openDeleteDialog: (index: number) => void;
}

const SecureKeyListView: React.FC<ISecureKeyListProps> = ({
  classes,
  state,
  alertSuccess,
  alertFailure,
  openEditDialog,
  openDeleteDialog,
}) => {
  const { secureKeys } = state;

  // used for filtering down secure keys
  const [searchText, setSearchText] = React.useState('');

  const [createDialogOpen, setCreateDialogOpen] = React.useState(false);

  const filteredSecureKeys = secureKeys.filter(
    (key) =>
      key
        .get('name')
        .toLowerCase()
        .includes(searchText.toLowerCase()) ||
      key
        .get('description')
        .toLowerCase()
        .includes(searchText.toLowerCase())
  );

  return (
    <div>
      <div className={classes.secureKeysTitle}>Secure keys</div>
      <div className={classes.secureKeyManager}>
        <Button
          className={classes.addSecureKeyButton}
          color="primary"
          variant="contained"
          onClick={() => setCreateDialogOpen(true)}
          data-cy="create-secure-key"
        >
          Add secure key
        </Button>
        <div className={classes.secureKeySearch}>
          <SecureKeySearch searchText={searchText} setSearchText={setSearchText} />
        </div>
      </div>

      <Paper className={classes.root}>
        <Table>
          <TableHead>
            <TableRow className={classes.row}>
              <CustomTableCell>Key</CustomTableCell>
              <CustomTableCell>Description</CustomTableCell>
              <CustomTableCell></CustomTableCell>
            </TableRow>
          </TableHead>
          <TableBody data-cy="secure-key-list">
            {filteredSecureKeys.map((keyMetadata, keyIndex) => {
              const keyID = keyMetadata.get('name');
              return (
                <TableRow
                  key={keyMetadata.get('name')}
                  hover
                  selected
                  className={classes.row}
                  onClick={() => openEditDialog(keyIndex)}
                  data-cy={`secure-key-row-${keyMetadata.get('name')}`}
                >
                  <CustomTableCell className={classes.nameCell}>{keyID}</CustomTableCell>
                  <CustomTableCell className={classes.descriptionCell}>
                    {keyMetadata.get('description')}
                  </CustomTableCell>
                  <CustomTableCell className={classes.actionButtonsCell}>
                    <SecureKeyActionButtons
                      openDeleteDialog={openDeleteDialog}
                      keyIndex={keyIndex}
                    />
                  </CustomTableCell>
                </TableRow>
              );
            })}
          </TableBody>
        </Table>
      </Paper>

      <SecureKeyCreate
        state={state}
        open={createDialogOpen}
        handleClose={() => setCreateDialogOpen(false)}
        alertSuccess={alertSuccess}
        alertFailure={alertFailure}
      />
    </div>
  );
};

const SecureKeyList = withStyles(styles)(SecureKeyListView);
export default SecureKeyList;

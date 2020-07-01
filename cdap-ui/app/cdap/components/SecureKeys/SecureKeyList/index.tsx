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
import Divider from '@material-ui/core/Divider';
import Paper from '@material-ui/core/Paper';
import withStyles, { StyleRules, WithStyles } from '@material-ui/core/styles/withStyles';
import Table from '@material-ui/core/Table';
import TableBody from '@material-ui/core/TableBody';
import TableCell from '@material-ui/core/TableCell';
import TableHead from '@material-ui/core/TableHead';
import TableRow from '@material-ui/core/TableRow';
import If from 'components/If';
import LoadingSVGCentered from 'components/LoadingSVGCentered';
import { SecureKeysPageMode, SecureKeyStatus } from 'components/SecureKeys';
import SecureKeyCreate from 'components/SecureKeys/SecureKeyCreate';
import SecureKeyActionButtons from 'components/SecureKeys/SecureKeyList/SecureKeyActionButtons';
import SecureKeySearch from 'components/SecureKeys/SecureKeySearch';
import { List, Map } from 'immutable';
import * as React from 'react';

const styles = (theme): StyleRules => {
  return {
    divider: {
      width: '100vw',
    },
    secureKeysTitle: {
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
    },
    secureKeySearch: {
      gridRow: '1',
      gridColumnStart: '7',
    },
    securityKeyRow: {
      cursor: 'pointer',
      hover: {
        cursor: 'pointer',
      },
    },
    nameCell: {
      width: '30%',
    },
    descriptionCell: {
      width: '40%',
    },
    dataCell: {
      width: '20%',
    },
    actionButtonsCell: {
      width: '10%',
    },
    loadingBox: {
      width: '100%',
      height: '100%',
      display: 'flex',
      alignItems: 'center',
      justifyContent: 'center',
    },
  };
};

interface ISecureKeyListProps extends WithStyles<typeof styles> {
  secureKeys: List<any>;
  setSecureKeyStatus: (status: SecureKeyStatus) => void;
  setActiveKeyIndex: (index: number) => void;
  visibility: Map<string, boolean>;
  setVisibility: (visibility: Map<string, boolean>) => void;
  setPageMode: (pageMode: SecureKeysPageMode) => void;
  searchText: string;
  handleSearchTextChange: (searchText: string) => void;
  setEditMode: (mode: boolean) => void;
  setDeleteMode: (mode: boolean) => void;
  loading: boolean;
}

const SecureKeyListView: React.FC<ISecureKeyListProps> = ({
  classes,
  secureKeys,
  setSecureKeyStatus,
  setActiveKeyIndex,
  visibility,
  setVisibility,
  setPageMode,
  searchText,
  handleSearchTextChange,
  setEditMode,
  setDeleteMode,
  loading,
}) => {
  const [createDialogOpen, setCreateDialogOpen] = React.useState(false);

  const onSecureKeyClick = (keyIndex) => {
    return () => {
      setPageMode(SecureKeysPageMode.Details);
      setActiveKeyIndex(keyIndex);
    };
  };

  return (
    <div>
      <h1 className={classes.secureKeysTitle}>Secure keys</h1>
      <Divider className={classes.divider} />
      <div className={classes.secureKeyManager}>
        <div className={classes.addSecureKeyButton}>
          <Button
            variant="outlined"
            color="primary"
            size="small"
            onClick={() => setCreateDialogOpen(true)}
          >
            Add Secure Key
          </Button>
        </div>
        <div className={classes.secureKeySearch}>
          <SecureKeySearch
            searchText={searchText}
            handleSearchTextChange={handleSearchTextChange}
          />
        </div>
      </div>

      <If condition={loading}>
        <div className={classes.loadingBox}>
          <LoadingSVGCentered />
        </div>
      </If>

      <If condition={!loading}>
        <Paper>
          <Table>
            <TableHead>
              <TableRow>
                <TableCell>Key</TableCell>
                <TableCell>Description</TableCell>
                <TableCell>Data</TableCell>
                <TableCell />
              </TableRow>
            </TableHead>
            <TableBody>
              {secureKeys.map((keyMetadata, keyIndex) => {
                const keyID = keyMetadata.get('name');
                return (
                  <TableRow
                    key={keyMetadata.get('name')}
                    hover
                    selected
                    className={classes.securityKeyRow}
                    onClick={onSecureKeyClick(keyIndex)}
                  >
                    <TableCell className={classes.nameCell}>{keyID}</TableCell>
                    <TableCell className={classes.descriptionCell}>
                      {keyMetadata.get('description')}
                    </TableCell>
                    <TableCell className={classes.dataCell}>
                      <If condition={!visibility.get(keyID)}>
                        <input id="password" value="password" disabled type="password"></input>
                      </If>
                      <If condition={visibility.get(keyID)}>{keyMetadata.get('data')}</If>
                    </TableCell>
                    <TableCell className={classes.actionButtonsCell}>
                      <SecureKeyActionButtons
                        keyIndex={keyIndex}
                        keyID={keyID}
                        visibility={visibility}
                        setActiveKeyIndex={setActiveKeyIndex}
                        setVisibility={setVisibility}
                        setEditMode={setEditMode}
                        setDeleteMode={setDeleteMode}
                      />
                    </TableCell>
                  </TableRow>
                );
              })}
            </TableBody>
          </Table>
        </Paper>
      </If>

      <SecureKeyCreate
        setSecureKeyStatus={setSecureKeyStatus}
        secureKeys={secureKeys}
        open={createDialogOpen}
        handleClose={() => setCreateDialogOpen(false)}
      />
    </div>
  );
};

const SecureKeyList = withStyles(styles)(SecureKeyListView);
export default SecureKeyList;

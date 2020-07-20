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

import { COMMON_DELIMITER, COMMON_KV_DELIMITER } from 'components/SecureKeys/constants';
import withStyles, { StyleRules, WithStyles } from '@material-ui/core/styles/withStyles';

import Button from '@material-ui/core/Button';
import Dialog from '@material-ui/core/Dialog';
import DialogActions from '@material-ui/core/DialogActions';
import DialogContent from '@material-ui/core/DialogContent';
import DialogTitle from '@material-ui/core/DialogTitle';
import { Map } from 'immutable';
import { MySecureKeyApi } from 'api/securekey';
import React from 'react';
import TextField from '@material-ui/core/TextField';
import WidgetWrapper from 'components/ConfigurationGroup/WidgetWrapper';
import { getCurrentNamespace } from 'services/NamespaceStore';
import isNil from 'lodash/isNil';

const styles = (theme): StyleRules => {
  return {
    secureKeyInput: {
      margin: `${theme.Spacing(3)}px ${theme.spacing(1)}px`,
    },
  };
};

interface ISecureKeyEditProps extends WithStyles<typeof styles> {
  state: any;
  open: boolean;
  handleClose: () => void;
  alertSuccess: () => void;
}

const SecureKeyEditView: React.FC<ISecureKeyEditProps> = ({
  classes,
  state,
  open,
  alertSuccess,
  handleClose,
}) => {
  const { secureKeys, activeKeyIndex } = state;

  const keyMetadata = secureKeys.get(activeKeyIndex);
  const keyID = keyMetadata ? keyMetadata.get('name') : '';
  const [localDescription, setLocalDescription] = React.useState(
    keyMetadata ? keyMetadata.get('description') : ''
  );
  const [localData, setLocalData] = React.useState('');
  // since 'properties' are in key-value form, keep a separate state in string form
  const properties = keyMetadata ? keyMetadata.get('properties') : '';
  const [localPropertiesInString, setLocalPropertiesInString] = React.useState('');

  const [valueIsChanged, setValueIsChanged] = React.useState(false);

  React.useEffect(() => {
    if (isNil(properties)) {
      return;
    }
    setLocalPropertiesInString(
      properties
        .entrySeq()
        .map((e) => e.join(COMMON_KV_DELIMITER))
        .join(COMMON_DELIMITER)
    );
  }, [properties]);

  const onLocalDescriptionChange = (e) => {
    setLocalDescription(e.target.value);
    setValueIsChanged(true);
  };

  const onLocalDataChange = (e) => {
    setLocalData(e.target.value);
    setValueIsChanged(true);
  };

  const onLocalPropertiesChange = (keyvalue) => {
    setLocalPropertiesInString(keyvalue);
    setValueIsChanged(true);
  };

  const convertLocalPropertiesInString = (keyvalue: string) => {
    let keyvaluePairs = Map({});
    keyvalue.split(COMMON_DELIMITER).forEach((pair) => {
      const [key, value] = pair.split(COMMON_KV_DELIMITER);
      keyvaluePairs = keyvaluePairs.set(key, value);
    });
    return keyvaluePairs;
  };

  const editSecureKey = () => {
    const namespace = getCurrentNamespace();

    const params = {
      namespace,
      key: keyID,
    };

    const requestBody = {
      description: localDescription,
      properties: convertLocalPropertiesInString(localPropertiesInString),
      data: localData,
    };

    MySecureKeyApi.put(params, requestBody).subscribe(() => {
      setLocalDescription('');
      setLocalData('');
      setLocalPropertiesInString('');
      alertSuccess();
      handleClose();
    });
  };

  return (
    <Dialog open={open} onClose={handleClose}>
      <DialogTitle>Edit {keyID}</DialogTitle>
      <DialogContent>
        <div className={classes.secureKeyInput}>
          <TextField
            required
            variant="outlined"
            label="Description"
            defaultValue={localDescription}
            onChange={onLocalDescriptionChange}
            fullWidth
            InputProps={{
              className: classes.textField,
            }}
          />
        </div>
        <div className={classes.secureKeyInput}>
          <TextField
            required
            variant="outlined"
            label="Data"
            type={'password'}
            value={localData}
            onChange={onLocalDataChange}
            fullWidth
            InputProps={{
              className: classes.textField,
            }}
          />
        </div>
        <div className={classes.secureKeyInput}>
          <WidgetWrapper
            widgetProperty={{
              label: 'Properties',
              name: 'Properties',
              'widget-type': 'keyvalue',
              'widget-attributes': {
                delimiter: COMMON_DELIMITER,
                'kv-delimiter': COMMON_KV_DELIMITER,
                'key-placeholder': 'key',
                'value-placeholder': 'value',
              },
            }}
            pluginProperty={{
              required: false,
              name: 'Properties',
            }}
            value={localPropertiesInString}
            onChange={(keyvalue) => onLocalPropertiesChange(keyvalue)}
          />
        </div>
      </DialogContent>
      <DialogActions>
        <Button onClick={handleClose} color="primary">
          Cancel
        </Button>
        <Button
          onClick={editSecureKey}
          color="primary"
          disabled={!valueIsChanged || !localDescription || !localData}
        >
          Save
        </Button>
      </DialogActions>
    </Dialog>
  );
};

const SecureKeyEdit = withStyles(styles)(SecureKeyEditView);
export default SecureKeyEdit;

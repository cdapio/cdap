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
import Dialog from '@material-ui/core/Dialog';
import DialogActions from '@material-ui/core/DialogActions';
import DialogContent from '@material-ui/core/DialogContent';
import DialogTitle from '@material-ui/core/DialogTitle';
import withStyles, { StyleRules, WithStyles } from '@material-ui/core/styles/withStyles';
import TextField from '@material-ui/core/TextField';
import { MySecureKeyApi } from 'api/securekey';
import classnames from 'classnames';
import WidgetWrapper from 'components/ConfigurationGroup/WidgetWrapper';
import { COMMON_DELIMITER, COMMON_KV_DELIMITER } from 'components/PluginJSONCreator/constants';
import { SecureKeyStatus } from 'components/SecureKeys';
import { List, Map } from 'immutable';
import React from 'react';
import { getCurrentNamespace } from 'services/NamespaceStore';

const styles = (theme): StyleRules => {
  return {
    margin: {
      margin: `${theme.Spacing(3)}px ${theme.spacing(1)}px`,
    },
    textField: {
      width: '45ch',
    },
    keyvalueField: {
      width: '60ch',
    },
  };
};

interface ISecureKeyCreateProps extends WithStyles<typeof styles> {
  secureKeys: List<any>;
  setSecureKeyStatus: (status: SecureKeyStatus) => void;
  open: boolean;
  handleClose: () => void;
}

const SecureKeyCreateView: React.FC<ISecureKeyCreateProps> = ({
  classes,
  setSecureKeyStatus,
  secureKeys,
  open,
  handleClose,
}) => {
  const [localName, setLocalName] = React.useState('');
  const [localDescription, setLocalDescription] = React.useState('');
  const [localData, setLocalData] = React.useState('');
  // 'properties' are in key-value form, keep a state in string form
  const [localPropertiesInString, setLocalPropertiesInString] = React.useState('');

  const [showData, setShowData] = React.useState(false);

  const onLocalNameChange = (e) => {
    setLocalName(e.target.value);
  };

  const onLocalDescriptionChange = (e) => {
    setLocalDescription(e.target.value);
  };

  const onLocalDataChange = (e) => {
    setLocalData(e.target.value);
  };

  const onLocalPropertiesChange = (keyvalue) => {
    setLocalPropertiesInString(keyvalue);
  };

  const convertLocalPropertiesInString = (keyvalue: string) => {
    let keyvaluePairs = Map({});
    keyvalue.split(COMMON_DELIMITER).forEach((pair) => {
      const [key, value] = pair.split(COMMON_KV_DELIMITER);
      keyvaluePairs = keyvaluePairs.set(key, value);
    });
    return keyvaluePairs;
  };

  const saveSecureKey = () => {
    // Duplicate key name should raise an error
    const keyIDs = secureKeys.map((key) => key.get('name'));
    if (keyIDs.includes(localName)) {
      setSecureKeyStatus(SecureKeyStatus.Failure);
      return;
    }

    addSecureKey();
  };

  const addSecureKey = () => {
    const namespace = getCurrentNamespace();

    const params = {
      namespace,
      key: localName,
    };

    const requestBody = {
      description: localDescription,
      data: localData,
      properties: convertLocalPropertiesInString(localPropertiesInString),
    };

    MySecureKeyApi.put(params, requestBody).subscribe(() => {
      setLocalName('');
      setLocalDescription('');
      setLocalData('');
      setLocalPropertiesInString('');
      setSecureKeyStatus(SecureKeyStatus.Success);
      handleClose();
    });
  };

  return (
    <Dialog open={open} onClose={handleClose}>
      <DialogTitle>Create secure key</DialogTitle>
      <DialogContent>
        <div className={classnames(classes.margin, classes.textField)}>
          <TextField
            variant="outlined"
            label="Name"
            defaultValue={localName}
            onChange={onLocalNameChange}
            InputProps={{
              className: classes.textField,
            }}
          />
        </div>
        <div className={classnames(classes.margin, classes.textField)}>
          <TextField
            variant="outlined"
            label="Description"
            defaultValue={localDescription}
            onChange={onLocalDescriptionChange}
            InputProps={{
              className: classes.textField,
            }}
          />
        </div>
        <div className={classnames(classes.margin, classes.textField)}>
          <TextField
            variant="outlined"
            label="Data"
            type={showData ? 'text' : 'password'}
            value={localData}
            onChange={onLocalDataChange}
            InputProps={{
              className: classes.textField,
            }}
          />
        </div>
        <div className={classnames(classes.margin, classes.keyvalueField)}>
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
          onClick={saveSecureKey}
          color="primary"
          disabled={!localName || !localDescription || !localData}
        >
          Save
        </Button>
      </DialogActions>
    </Dialog>
  );
};

const SecureKeyCreate = withStyles(styles)(SecureKeyCreateView);
export default SecureKeyCreate;

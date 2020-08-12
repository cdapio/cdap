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
import IconSVG from 'components/IconSVG';
import React from 'react';
import Tooltip from '@material-ui/core/Tooltip';

const styles = (theme): StyleRules => {
  return {
    fileInput: {
      display: 'none',
    },
    buttonTooltip: {
      fontSize: '13px',
      backgroundColor: theme.palette.grey[50],
    },
    importButton: {
      marginTop: theme.spacing(0.5),
    },
    importIcon: {
      fontSize: '14px',
    },
  };
};

interface IJSONImportButtonProps extends WithStyles<typeof styles> {
  populateImportResults: (filename: string, fileContent: string) => void;
}

const JSONImportButtonView: React.FC<IJSONImportButtonProps> = ({
  classes,
  populateImportResults,
}) => {
  function processFileUpload() {
    return (e) => {
      const files = e.target.files;
      if (files.length > 0) {
        const filename = files[0].name;
        const reader = new FileReader();
        reader.readAsText(files[0]);
        let fileContent;
        reader.onload = (r) => {
          fileContent = r.target.result;
          populateImportResults(filename, fileContent);
        };
      }
    };
  }

  return (
    <div>
      <input
        accept="json/*"
        id="raised-button-file"
        type="file"
        className={classes.fileInput}
        onChange={processFileUpload()}
        data-cy="plugin-json-uploader"
      />
      <label htmlFor="raised-button-file">
        <Tooltip
          title="Import JSON"
          classes={{
            tooltip: classes.buttonTooltip,
          }}
        >
          <div>
            <IconButton
              className={classes.importButton}
              aria-label="save"
              component="span"
              color="primary"
              data-cy="plugin-json-import-btn"
            >
              <IconSVG name="icon-import" className={classes.importIcon} />
            </IconButton>
          </div>
        </Tooltip>
      </label>
    </div>
  );
};

const JSONImportButton = withStyles(styles)(JSONImportButtonView);
export default JSONImportButton;

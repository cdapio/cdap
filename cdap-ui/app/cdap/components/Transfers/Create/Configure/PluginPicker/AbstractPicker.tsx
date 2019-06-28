/*
 * Copyright © 2019 Cask Data, Inc.
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
import withStyles, { WithStyles, StyleRules } from '@material-ui/core/styles/withStyles';
import classnames from 'classnames';
import IconSVG from 'components/IconSVG';
import StepButtons from '../../StepButtons';

const styles = (theme): StyleRules => {
  return {
    plugin: {
      display: 'inline-flex',
      justifyContent: 'center',
      alignItems: 'center',
      width: '200px',
      height: '100px',
      margin: '15px 25px',
      border: `3px solid ${theme.palette.grey[400]}`,
      borderRadius: '6px',
      cursor: 'pointer',
      fontSize: '18px',
      '&:hover': {
        backgroundColor: theme.palette.grey[700],
      },
    },
    disabled: {
      cursor: 'not-allowed !important',
      backgroundColor: theme.palette.grey[400],
      color: theme.palette.grey[200],
      '&:hover': {
        backgroundColor: `${theme.palette.grey[400]} !important`,
      },
    },
    icon: {
      fontSize: '28px',
    },
  };
};

interface IProps extends WithStyles<typeof styles> {
  plugins: any;
  onClick: (plugin: any) => void;
}

const AbstractPickerView: React.SFC<IProps> = ({ plugins, onClick, classes }) => {
  return (
    <div>
      <div>
        {plugins.map((plugin) => {
          return (
            <div
              key={plugin.label}
              className={classnames(classes.plugin, { [classes.disabled]: plugin.disabled })}
              onClick={onClick.bind(null, plugin)}
            >
              <div className="text-center">
                <div>
                  <IconSVG name="icon-database" className={classes.icon} />
                </div>
                {plugin.label}
              </div>
            </div>
          );
        })}
      </div>

      <br />
      <StepButtons hideNext={true} />
    </div>
  );
};

const AbstractPicker = withStyles(styles)(AbstractPickerView);
export default AbstractPicker;

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
import withStyles, { WithStyles, StyleRules } from '@material-ui/core/styles/withStyles';
import IconSVG from 'components/IconSVG';

export const PluginCardWidth = 175;

const styles = (theme): StyleRules => {
  return {
    root: {
      border: `2px solid ${theme.palette.grey[300]}`,
      borderRadius: '4px',
      padding: '10px',
      textAlign: 'center',
      color: theme.palette.grey[100],
      width: `${PluginCardWidth}px`,
      height: '100px',

      '&:hover': {
        backgroundColor: theme.palette.grey[700],
        borderColor: theme.palette.blue[100],

        '& *': {
          textDecoration: 'none',
          color: 'inherit',
        },
      },
    },
    img: {
      height: '40px',
      width: '40px',
      color: theme.palette.grey[100],
    },
    name: {
      marginTop: '15px',
      fontWeight: 600,
      color: theme.palette.grey[100],
    },
  };
};

interface IPluginCardProps extends WithStyles<typeof styles> {
  name: string;
}

const PluginCardView: React.FC<IPluginCardProps> = ({ classes, name }) => {
  return (
    <div className={classes.root}>
      <div className={classes.imgContainer}>
        <IconSVG name="icon-plug" className={classes.img} />
      </div>
      <div className={classes.name}>{name}</div>
    </div>
  );
};

const PluginCard = withStyles(styles)(PluginCardView);
export default PluginCard;

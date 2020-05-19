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

export const PluginCardWidth = 153;
export const PluginCardHeight = 135;

const styles = (theme): StyleRules => {
  return {
    root: {
      border: `1px solid ${theme.palette.grey[200]}`,
      borderRadius: '4px',
      padding: '20px 15px',
      textAlign: 'center',
      color: theme.palette.grey[50],
      width: `${PluginCardWidth}px`,
      height: `${PluginCardHeight}px`,

      '&:hover': {
        backgroundColor: theme.palette.grey[700],

        '& *': {
          textDecoration: 'none',
          color: 'inherit',
        },
      },
    },
    img: {
      height: '40px',
      width: '40px',
    },
    name: {
      marginTop: '22px',
      fontWeight: 600,
      color: theme.palette.grey[50],
      whiteSpace: 'pre-line',
      lineHeight: 1.2,
    },
  };
};

interface IPluginCardProps extends WithStyles<typeof styles> {
  name: string;
  icon?: string;
}

const PluginCardView: React.FC<IPluginCardProps> = ({ classes, name, icon }) => {
  const defaultIcon = <IconSVG name="icon-plug" className={classes.img} />;

  return (
    <div className={classes.root}>
      <div className={classes.imgContainer}>
        {icon ? <img src={icon} className={classes.img} /> : defaultIcon}
      </div>
      <div className={classes.name}>{name}</div>
    </div>
  );
};

const PluginCard = withStyles(styles)(PluginCardView);
export default PluginCard;

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
import capitalize from 'lodash/capitalize';
import IconSVG from 'components/IconSVG';
import classnames from 'classnames';
import { objectQuery } from 'services/helpers';
import Popover from 'components/Popover';

const styles = (theme): StyleRules => {
  return {
    green: {
      color: theme.palette.green[50],
    },
    red: {
      color: theme.palette.red[100],
    },
    supportIcon: {
      marginTop: '-2px',
    },
    clickable: {
      cursor: 'pointer',
      color: theme.palette.blue[100],

      '&:hover $supportText': {
        textDecoration: 'underline',
      },
    },
    supportText: {
      marginLeft: '5px',
    },
    popoverContentContainer: {
      display: 'flex',
      textAlign: 'left',
      padding: '10px 5px',
    },
    popover: {
      '& .popper': {
        width: '250px',
      },
    },
    iconContainer: {
      paddingRight: '5px',
    },
  };
};

interface ISupportedProps extends WithStyles<typeof styles> {
  columnRow: {
    support: string;
    suggestion?: {
      message: string;
    };
  };
}

export enum SUPPORT {
  yes = 'YES',
  no = 'NO',
  partial = 'PARTIAL',
}

const popoverTextMap = {
  [SUPPORT.no]: 'The column is not supported',
  [SUPPORT.partial]: 'The column is partially supported',
};

const SupportedView: React.FC<ISupportedProps> = ({ classes, columnRow }) => {
  const icon = (
    <IconSVG
      name={columnRow.support === SUPPORT.partial ? 'icon-circle-o' : 'icon-circle'}
      className={classnames(classes.supportIcon, {
        [classes.green]: columnRow.support === SUPPORT.yes,
        [classes.red]: columnRow.support !== SUPPORT.yes,
      })}
    />
  );

  const supportTarget = (
    <span
      className={classnames({
        [classes.clickable]: columnRow.support !== SUPPORT.yes,
      })}
    >
      {icon}
      <span className={classes.supportText}>{capitalize(columnRow.support)}</span>
    </span>
  );

  if (columnRow.support === SUPPORT.yes) {
    return supportTarget;
  }

  const description = objectQuery(columnRow, 'suggestion', 'message');

  return (
    <Popover
      target={() => supportTarget}
      className={classes.popover}
      placement="left"
      bubbleEvent={false}
      enableInteractionInPopover={true}
      showOn="Click"
    >
      <div className={classes.popoverContentContainer}>
        <div className={classes.iconContainer}>{icon}</div>
        <div className={classes.popoverContent}>
          <div>{popoverTextMap[columnRow.support]}</div>
          <br />
          <div>
            <strong>Reason:</strong>
          </div>
          <div>{description}</div>
        </div>
      </div>
    </Popover>
  );
};

const Supported = withStyles(styles)(SupportedView);
export default Supported;

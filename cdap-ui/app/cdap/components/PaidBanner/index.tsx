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

import React from 'react';
import classnames from 'classnames';
import If from 'components/If';
import withStyles, { WithStyles, StyleRules } from '@material-ui/core/styles/withStyles';
const colors = require('styles/colors.scss');

const styles = (theme): StyleRules => {
  return {
    paidBanner: {
      width: '23px',
      display: 'inline-block',
      position: 'relative',
      color: theme.palette.white[50],
      backgroundColor: theme.palette.bluegrey[200],
      fontSize: '13px',
      textAlign: 'left',
      paddingLeft: '5px',
    },

    chevron: {
      position: 'absolute',
      right: '-8px',
      top: '0',
      display: 'block',
      height: '16px',
      '&:before': {
        position: 'absolute',
        display: 'block',
        content: '""',
        border: '9px solid ' + theme.palette.bluegrey[200],
        right: '0',
        borderRightColor: theme.palette.white[50],
      },
    },

    large: {
      width: '155px',
      textAlign: 'left',
      paddingLeft: '5px',

      '& > #chevron:before': {
        borderRightColor: theme.palette.grey[800],
      },
    },
  };
};
interface IPaidBannerProps extends WithStyles<typeof styles> {
  expandedView: boolean;
}

function InnerPaidBanner(props: IPaidBannerProps) {
  const classes = classnames(props.classes.paidBanner, {
    [props.classes.large]: props.expandedView,
  });
  return (
    <div className={classes}>
      $
      <If condition={props.expandedView}>
        <React.Fragment> - Additional Charges</React.Fragment>
      </If>
      <div id="chevron" className={props.classes.chevron} />
    </div>
  );
}

const PaidBanner = withStyles(styles)(InnerPaidBanner);
export default PaidBanner;

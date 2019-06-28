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
import IconSVG from 'components/IconSVG';
import ArrowRight from '@material-ui/icons/ArrowRightAlt';
import T from 'i18n-react';
import { Link } from 'react-router-dom';
import { getCurrentNamespace } from 'services/NamespaceStore';

const PREFIX = 'features.Transfers.List';

const styles = (theme): StyleRules => {
  return {
    container: {
      margin: '20px 0',
    },
    addLink: {
      border: `1px solid ${theme.palette.grey[200]}`,
      borderRadius: '4px',
      display: 'inline-block',
      padding: '5px 15px 15px',
      '&:hover': {
        textDecoration: 'none',
        backgroundColor: theme.palette.grey[700],
      },
    },
    iconsContainer: {
      textAlign: 'center',
      fontSize: '30px',
      marginBottom: '10px',
      '& > *:not(:last-child)': {
        color: theme.palette.grey[100],
      },
    },
    arrow: {
      fontSize: '30px',
      width: '5rem',
      transform: 'scaleX(2)',
    },
  };
};

const AddNewTransferView: React.SFC<WithStyles<typeof styles>> = ({ classes }) => {
  return (
    <div className={classes.container}>
      <Link to={`/ns/${getCurrentNamespace()}/transfers/create`} className={classes.addLink}>
        <div className={classes.iconsContainer}>
          <IconSVG name="icon-database" />
          <ArrowRight className={classes.arrow} />
          <IconSVG name="icon-bigquery" />
        </div>
        <div>{T.translate(`${PREFIX}.addNewTransfer`)}</div>
      </Link>
    </div>
  );
};

const AddNewTransfer = withStyles(styles)(AddNewTransferView);
export default AddNewTransfer;

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
import { detailContextConnect, IDetailContext } from 'components/Replicator/Detail';
import ActionsPopover, { IAction } from 'components/ActionsPopover';
import DeleteConfirmation, { InstanceType } from 'components/Replicator/DeleteConfirmation';
import { getCurrentNamespace } from 'services/NamespaceStore';
import { Redirect } from 'react-router-dom';
import IconSVG from 'components/IconSVG';

const styles = (theme): StyleRules => {
  return {
    actionsBtn: {
      height: '50px',
      width: '60px',
      textAlign: 'center',
      backgroundColor: theme.palette.white[50],
      borderRadius: '4px',
      border: `1px solid ${theme.palette.grey[400]}`,
      cursor: 'pointer',
      marginLeft: '15px',
      userSelect: 'none',
      '&:hover': {
        backgroundColor: theme.palette.grey[800],
      },
      '&.active': {
        color: theme.palette.blue[300],
        '& $icon': {
          stroke: theme.palette.blue[300],
        },
      },
    },
    icon: {
      fontSize: '20px',
      fill: 'none',
      stroke: theme.palette.grey[200],
    },
    iconContainer: {
      paddingTop: '5px',
      marginBottom: '2px',
    },
    delete: {
      color: theme.palette.red[100],
    },
  };
};

const ActionsView: React.FC<IDetailContext & WithStyles<typeof styles>> = ({ classes, name }) => {
  const [showDeleteConfirmation, setShowDeleteConfirmation] = React.useState(false);
  const [redirect, setRedirect] = React.useState(false);

  if (redirect) {
    return <Redirect to={`/ns/${getCurrentNamespace()}/replication`} />;
  }

  const actions: IAction[] = [
    {
      label: 'Delete',
      actionFn: () => setShowDeleteConfirmation(true),
      className: classes.delete,
    },
  ];

  const targetElem = (props) => {
    return (
      <div {...props} className={`${classes.actionsBtn} ${props.className}`}>
        <div className={classes.iconContainer}>
          <IconSVG name="icon-cog-empty" className={classes.icon} />
        </div>
        <div>Actions</div>
      </div>
    );
  };

  return (
    <div className={classes.root}>
      <ActionsPopover actions={actions} targetElem={targetElem} />
      <DeleteConfirmation
        replicatorId={name}
        show={showDeleteConfirmation}
        onDelete={() => setRedirect(true)}
        closeModal={() => setShowDeleteConfirmation(false)}
        type={InstanceType.app}
      />
    </div>
  );
};

const StyledActionsView = withStyles(styles)(ActionsView);
const Actions = detailContextConnect(StyledActionsView);
export default Actions;

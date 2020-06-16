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
import { createContextConnect, ICreateContext } from 'components/Replicator/Create';
import Button from '@material-ui/core/Button';
import LoadingSVG from 'components/LoadingSVG';
import { getCurrentNamespace } from 'services/NamespaceStore';
import { MyAppApi } from 'api/app';
import { MyReplicatorApi } from 'api/replicator';
import { Redirect } from 'react-router-dom';
import If from 'components/If';
import { extractErrorMessage } from 'services/helpers';

const styles = (theme): StyleRules => {
  return {
    root: {
      marginTop: '50px',
      borderTop: `1px solid ${theme.palette.grey[300]}`,
      paddingTop: '25px',
      '& button': {
        marginRight: '50px',
      },
    },
    error: {
      marginTop: '50px',
      color: theme.palette.red[100],
    },
  };
};

enum REDIRECT_TARGET {
  detail = 'detail',
  list = 'list',
}

const ActionButtonsView: React.FC<ICreateContext & WithStyles<typeof styles>> = ({
  classes,
  getReplicatorConfig,
  parentArtifact,
  draftId,
  saveDraft,
  name,
}) => {
  const [deployLoading, setDeployLoading] = React.useState(false);
  const [error, setError] = React.useState(null);
  const [redirect, setRedirect] = React.useState(null);

  function constructJson() {
    const config = getReplicatorConfig();

    return {
      name,
      artifact: {
        ...parentArtifact,
      },
      config,
    };
  }

  function deployReplicator() {
    setDeployLoading(true);
    const spec = constructJson();

    const params = {
      namespace: getCurrentNamespace(),
      appName: spec.name,
    };

    MyAppApi.list({ namespace: getCurrentNamespace() }).subscribe(
      (apps) => {
        const existingAppName = apps.filter((app) => app.name === name);

        if (existingAppName.length > 0) {
          setError(`There is already an existing application "${name}"`);
          setDeployLoading(false);
          return;
        }

        MyReplicatorApi.publish(params, spec).subscribe(
          () => {
            MyReplicatorApi.deleteDraft({
              namespace: getCurrentNamespace(),
              draftId,
            }).subscribe(null, null, () => {
              setRedirect(REDIRECT_TARGET.detail);
            });
          },
          (err) => {
            setError(err);
            setDeployLoading(false);
          }
        );
      },
      (err) => {
        setError(err);
        setDeployLoading(false);
      }
    );
  }

  function saveAndClose() {
    saveDraft().subscribe(
      () => {
        setRedirect(REDIRECT_TARGET.list);
      },
      (err) => {
        setError(err);
      }
    );
  }

  if (redirect) {
    let redirectLink = `/ns/${getCurrentNamespace()}/replication`;
    if (redirect === REDIRECT_TARGET.detail) {
      redirectLink = `${redirectLink}/detail/${name}`;
    }

    return <Redirect to={redirectLink} />;
  }

  return (
    <React.Fragment>
      <If condition={error}>
        <div className={classes.error}>{extractErrorMessage(error)}</div>
      </If>

      <div className={classes.root}>
        <Button
          variant="contained"
          color="primary"
          onClick={deployReplicator}
          disabled={deployLoading}
        >
          <If condition={deployLoading}>
            <LoadingSVG />
          </If>
          Deploy Replication Pipeline
        </Button>

        <Button color="primary" onClick={saveAndClose}>
          Save And Close
        </Button>
      </div>
    </React.Fragment>
  );
};

const StyledActionButtons = withStyles(styles)(ActionButtonsView);
const ActionButtons = createContextConnect(StyledActionButtons);
export default ActionButtons;

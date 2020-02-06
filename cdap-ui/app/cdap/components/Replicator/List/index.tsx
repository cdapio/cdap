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
import SourceList from 'components/Replicator/List/SourceList';
import { Route, Switch, NavLink } from 'react-router-dom';
import { basepath } from 'components/Replicator';
import Deployed from 'components/Replicator/List/Deployed';
import Drafts from 'components/Replicator/List/Drafts';
import { getCurrentNamespace } from 'services/NamespaceStore';

const styles = (theme): StyleRules => {
  return {
    root: {
      padding: '25px 40px',
      height: '100%',
    },
    linkContainer: {
      marginTop: '50px',
      borderBottom: `2px solid ${theme.palette.grey[400]}`,
      display: 'flex',
    },
    link: {
      color: theme.palette.grey[50],
      fontSize: '20px',
      marginRight: '100px',
      '&:hover': {
        textDecoration: 'none',
        color: 'inherit',
      },
    },
    activeLink: {
      fontWeight: 600,
      borderBottom: `3px solid ${theme.palette.grey[200]}`,
    },
    contentContainer: {
      marginTop: '15px',

      // 100% - header in source list - source list - (margin top + NavLink) - content marginTop
      height: 'calc(100% - 50px - 115px - 85px - 15px)',
    },
  };
};

const ListView: React.FC<WithStyles<typeof styles>> = ({ classes }) => {
  return (
    <div className={classes.root}>
      <SourceList />

      <div className={classes.linkContainer}>
        <NavLink
          exact
          to={`/ns/${getCurrentNamespace()}/replicator`}
          activeClassName={classes.activeLink}
          className={classes.link}
        >
          Replicators
        </NavLink>
        <NavLink
          exact
          to={`/ns/${getCurrentNamespace()}/replicator/drafts`}
          activeClassName={classes.activeLink}
          className={classes.link}
        >
          Drafts
        </NavLink>
      </div>
      <div className={classes.contentContainer}>
        <Switch>
          <Route exact path={`${basepath}/drafts`} component={Drafts} />
          <Route exact path={`${basepath}`} component={Deployed} />
        </Switch>
      </div>
    </div>
  );
};

const List = withStyles(styles)(ListView);
export default List;

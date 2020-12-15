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

import React, { useContext } from 'react';
import withStyles, { WithStyles, StyleRules } from '@material-ui/core/styles/withStyles';
import { Route, Switch, NavLink } from 'react-router-dom';
import { getCurrentNamespace } from 'services/NamespaceStore';
import { basepath } from 'components/Replicator';
import { DetailContext } from 'components/Replicator/Detail';
import Overview from 'components/Replicator/Detail/Overview';
import Monitoring from 'components/Replicator/Detail/Monitoring';
import { humanReadableDate } from 'services/helpers';
import Heading, { HeadingTypes } from 'components/Heading';

const styles = (theme): StyleRules => {
  return {
    root: {
      display: 'grid',
      gridTemplateColumns: '350px 1fr',
      alignItems: 'end',
    },
    link: {
      color: theme.palette.grey[50],
      '&:hover $heading': {
        textDecoration: 'none',
        color: theme.palette.grey[50],
      },
    },
    separator: {
      fontSize: '25px',
      marginLeft: '10px',
      marginRight: '10px',
    },
    activeLink: {
      '& $heading': {
        fontWeight: 600,
        borderBottom: `3px solid ${theme.palette.grey[200]}`,
      },
    },
    updatedTime: {
      textAlign: 'right',
    },
    time: {
      marginLeft: '5px',
    },
    heading: {
      display: 'inline-block',
      marginBottom: 0,
    },
  };
};

const DetailContentView: React.FC<WithStyles<typeof styles>> = ({ classes }) => {
  const { name, lastUpdated } = useContext(DetailContext);

  const linkBase = `/ns/${getCurrentNamespace()}/replication/detail/${name}`;
  const routeBase = `${basepath}/detail/:replicatorId`;

  return (
    <React.Fragment>
      <div className={classes.root}>
        <div>
          <NavLink
            exact
            to={linkBase}
            activeClassName={classes.activeLink}
            className={classes.link}
          >
            <Heading type={HeadingTypes.h2} label="Overview" className={classes.heading} />
          </NavLink>
          <span className={classes.separator}>|</span>
          <NavLink
            exact
            to={`${linkBase}/monitor`}
            activeClassName={classes.activeLink}
            className={classes.link}
          >
            <Heading type={HeadingTypes.h2} label="Monitoring" className={classes.heading} />
          </NavLink>
        </div>
        <div className={classes.updatedTime}>
          <span>Last updated on</span>
          <span className={classes.time}>{humanReadableDate(lastUpdated, true)}</span>
        </div>
      </div>

      <Switch>
        <Route exact path={routeBase} component={Overview} />
        <Route exact path={`${routeBase}/monitor`} component={Monitoring} />
      </Switch>
    </React.Fragment>
  );
};

const DetailContent = withStyles(styles)(DetailContentView);
export default DetailContent;

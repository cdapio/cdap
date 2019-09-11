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
import { objectQuery } from 'services/helpers';
import { MyDeltaApi } from 'api/delta';
import { getCurrentNamespace } from 'services/NamespaceStore';
import moment from 'moment';
import { TransfersDetailContext } from 'components/Transfers/Detail/context';
import LoadingSVGCentered from 'components/LoadingSVGCentered';
import TopPanel from 'components/Transfers/Detail/TopPanel';
import Charts from './Charts';
import Statistics from './Statistics';
import PendingEvents from './PendingEvents';
import { MyProgramApi } from 'api/program';
import classnames from 'classnames';
import Configuration from './Configuration';

const styles = (theme): StyleRules => {
  return {
    separator: {
      margin: '25px 25px 10px',
      borderTopWidth: '3px',
    },
    tab: {
      padding: '10px 25px',
      fontSize: '22px',
      '& > span:not($tabSeparator)': {
        cursor: 'pointer',
      },
    },
    tabSeparator: {
      marginRight: '10px',
      marginLeft: '10px',
    },
    active: {
      fontWeight: 'bold',
      textDecoration: 'underline',
    },
    disabled: {
      cursor: 'not-allowed !important',
      color: theme.palette.grey[400],
    },
  };
};

interface IDetailProps extends WithStyles<typeof styles> {
  match: {
    params: {
      id: string;
    };
  };
}

interface IDetailState {
  name: string;
  id: string;
  description: string;
  updated: any;
  source: any;
  sourceConfig: any;
  target: any;
  targetConfig: any;
  status: string;
  fetchStatus: () => void;
  tab: number;
}

class DetailView extends React.PureComponent<IDetailProps, IDetailState> {
  public componentDidMount() {
    const id = objectQuery(this.props.match, 'params', 'id');
    const params = {
      context: getCurrentNamespace(),
      id,
    };

    MyDeltaApi.get(params).subscribe((res) => {
      this.setState({
        ...res.properties,
        name: res.name,
        id: res.id,
        updated: res.updated,
        description: res.description,
        loading: false,
      });

      this.pollStatus();
    });
  }

  public componentWillUnmount() {
    if (this.status$) {
      this.status$.unsubscribe();
    }
  }

  private getProgramParams = () => {
    return {
      namespace: getCurrentNamespace(),
      appId: `CDC-${this.state.id}`,
      programType: 'workers',
      programId: 'DeltaWorker',
    };
  };

  private status$ = null;

  private pollStatus = () => {
    this.status$ = MyProgramApi.pollStatus(this.getProgramParams()).subscribe((res) => {
      this.setState({
        status: res.status,
        tab: res.status !== 'RUNNING' ? 0 : this.state.tab,
      });
    });
  };

  public fetchStatus = () => {
    MyProgramApi.status(this.getProgramParams()).subscribe((res) => {
      this.setState({
        status: res.status,
      });
    });
  };

  public state = {
    name: '',
    id: '',
    description: '',
    updated: null,
    source: {},
    sourceConfig: {},
    target: {},
    targetConfig: {},
    loading: true,
    status: 'STOPPED',
    fetchStatus: this.fetchStatus,
    tab: 0,
  };

  private renderMonitor = () => {
    if (this.state.tab !== 1) {
      return null;
    }

    return (
      <React.Fragment>
        <Charts />
        <hr className={this.props.classes.separator} />
        <Statistics />
        <hr className={this.props.classes.separator} />
        <PendingEvents />
      </React.Fragment>
    );
  };

  private renderConfigure = () => {
    if (this.state.tab !== 0) {
      return null;
    }

    return <Configuration />;
  };

  private handleTabClick = (tab) => {
    if (this.state.status !== 'RUNNING') {
      this.setState({ tab: 0 });
      return;
    }

    this.setState({ tab });
  };

  public render() {
    if (this.state.loading) {
      return <LoadingSVGCentered />;
    }

    const classes = this.props.classes;

    return (
      <TransfersDetailContext.Provider value={this.state}>
        <div>
          <TopPanel />
          <div className="text-right pr-4">
            <small>
              Last updated {moment(this.state.updated * 1000).format('MMM D, YYYY [at] hh:mm A')}
            </small>
          </div>

          <div className={classes.tab}>
            <span
              onClick={this.handleTabClick.bind(this, 0)}
              className={classnames({ [classes.active]: this.state.tab === 0 })}
            >
              Configuration
            </span>
            <span className={classes.tabSeparator}>|</span>
            <span
              onClick={this.handleTabClick.bind(this, 1)}
              className={classnames({
                [classes.active]: this.state.tab === 1,
                [classes.disabled]: this.state.status !== 'RUNNING',
              })}
            >
              Monitor
            </span>
          </div>

          {this.renderConfigure()}
          {this.renderMonitor()}
        </div>
      </TransfersDetailContext.Provider>
    );
  }
}

const Detail = withStyles(styles)(DetailView);
export default Detail;

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
import { connect, Provider } from 'react-redux';
import SystemDelayStore from 'services/SystemDelayStore';
import SystemDelayActions from 'services/SystemDelayStore/SystemDelayActions';
import Snackbar from '@material-ui/core/Snackbar';
import Button from '@material-ui/core/Button';
import ee from 'event-emitter';
import { WINDOW_ON_FOCUS, WINDOW_ON_BLUR } from 'services/WindowManager';
import {
  getExperimentValue,
  isExperimentEnabled,
  isNilOrEmpty,
  ONE_HOUR_SECONDS,
} from 'services/helpers';
import { GenerateStatsFromRequests } from 'components/SystemServicesDelay/LatencyStats';
import withStyles, { StyleRules, WithStyles } from '@material-ui/core/styles/withStyles';
import {
  IBindingRequestInfo,
  IChartStatType,
  ISystemDelayProps,
} from 'components/SystemServicesDelay/LatencyTypes';
import ConfigurableTab from 'components/ConfigurableTab';
import { LatencyChart } from 'components/SystemServicesDelay/LatencyChart';
import { LatencyStatsContainer } from 'components/SystemServicesDelay/LatencyStatsContainer';
import InfoIcon from '@material-ui/icons/InfoRounded';
import CloseIcon from '@material-ui/icons/CloseRounded';

const EXPERIMENT_ID = 'system-delay-notification';
const snoozeTimelabel = `${EXPERIMENT_ID}-snoozetime`;
const HEALTH_CHECK_INTERVAL = 1000;
const DEFAULT_DELAY_TIME = 10000;
const CLEAN_CHECK_COUNT = 3;

const Tab = {
  tabs: [
    {
      name: 'Chart',
      id: 'Chart',
      content: null,
    },
    {
      name: 'Stats',
      id: 'Stats',
      content: null,
    },
  ],
  layout: 'horizontal',
  defaultTab: 'Chart',
};

const styles = (theme): StyleRules => {
  return {
    snackbar: {
      backgroundColor: theme.palette.grey[600],
      color: 'black',
    },
    container: {
      position: 'fixed',
      right: 0,
      bottom: 0,
      background: '#ccc',
      border: '2px solid black',
      width: '500px',
      height: '350px',
    },
    iconBtn: {
      outline: 'none',
      width: '25px',
      minWidth: 'unset',
    },
    moreInfoBtn: {
      color: theme.palette.blue[100],
    },
    InfoPanelCloseBtn: {
      position: 'absolute',
      right: '10px',
      top: '10px',
      color: 'black',
    },
  };
};
export interface ISystemDelayState {
  cleanChecksNeeded: number;
  requestDelayMap: Record<number, IBindingRequestInfo>;
  showMoreInfo: boolean;
}

class SystemServicesDelayView extends React.Component<ISystemDelayProps, ISystemDelayState> {
  public state = {
    cleanChecksNeeded: 0,
    requestDelayMap: {},
    showMoreInfo: false,
  };
  private healthCheckInterval: NodeJS.Timeout;
  private eventEmitter = ee(ee);

  public componentDidMount() {
    if (isExperimentEnabled(EXPERIMENT_ID)) {
      this.checkForDelayedBindings();
    }
    this.startHealthCheck();
    this.eventEmitter.on(WINDOW_ON_FOCUS, () => {
      this.startHealthCheck();
    });
    this.eventEmitter.on(WINDOW_ON_BLUR, () => {
      this.stopHealthCheck();
    });
  }

  public componentWillUnmount() {
    this.stopHealthCheck();
  }

  private startHealthCheck = () => {
    if (isExperimentEnabled(EXPERIMENT_ID)) {
      this.healthCheckInterval = setInterval(this.checkForDelayedBindings, HEALTH_CHECK_INTERVAL);
    }
  };

  /**
   * When the user clicks on 'Close' button we snooze the notification for 1 hour
   * We wait for 1 hour to show the delay notification if there are any delays.
   */
  private isNotificationSnoozed = () => {
    const snoozeTime = window.localStorage.getItem(snoozeTimelabel);
    if (!snoozeTime) {
      return false;
    }
    const snoozeEpoch = parseInt(snoozeTime, 10);
    return Date.now() - snoozeEpoch < ONE_HOUR_SECONDS * 1000;
  };

  private checkForDelayedBindings = () => {
    if (this.isNotificationSnoozed()) {
      return;
    }
    this.constructData();
  };

  private stopHealthCheck = () => {
    this.setState({ cleanChecksNeeded: 0 }, () => {
      SystemDelayStore.dispatch({
        type: SystemDelayActions.hideDelay,
      });
    });
    clearInterval(this.healthCheckInterval);
  };

  private closeNotification = () => {
    this.setState({ cleanChecksNeeded: 0 }, () => {
      SystemDelayStore.dispatch({
        type: SystemDelayActions.hideDelay,
      });
    });
    window.localStorage.setItem(snoozeTimelabel, Date.now().toString());
  };

  private constructData = () => {
    const delayedTimeFromExperiment = parseInt(getExperimentValue(EXPERIMENT_ID), 10);
    const requestsTakingTime: IChartStatType[] = GenerateStatsFromRequests(
      this.props.activeDataSources
    );
    const requestDelayMap = {};
    const slicedRequests =
      requestsTakingTime.length > 30 ? requestsTakingTime.slice(-30) : requestsTakingTime;
    let totalBackendRequestDelay = 0;
    let totalNetworkDelay = 0;
    for (const request of slicedRequests) {
      totalBackendRequestDelay += request.y;
      totalNetworkDelay += request.networkDelay;
      requestDelayMap[request.id] = {
        requestTimeFromClient: request.networkDelay,
        backendRequestTimeDuration: request.y,
      };
    }
    if (totalBackendRequestDelay / 30 > delayedTimeFromExperiment || totalNetworkDelay / 30 > 10) {
      SystemDelayStore.dispatch({
        type: SystemDelayActions.showDelay,
      });
    }
    this.setState({ requestDelayMap });
  };

  public toggleMoreInfoPane = () => {
    this.setState({ showMoreInfo: !this.state.showMoreInfo });
  };

  public renderMoreInfo = () => {
    if (!this.state.showMoreInfo) {
      return null;
    }
    const { classes } = this.props;
    Tab.tabs[0].content = <LatencyChart requestDelayMap={this.state.requestDelayMap} />;
    Tab.tabs[1].content = <LatencyStatsContainer requestDelayMap={this.state.requestDelayMap} />;
    return (
      <div className={this.props.classes.container}>
        <div>
          <Button
            className={`${classes.iconBtn} ${classes.InfoPanelCloseBtn}`}
            onClick={this.toggleMoreInfoPane}
          >
            <CloseIcon />
          </Button>
        </div>
        <ConfigurableTab tabConfig={Tab} />
      </div>
    );
  };

  public render() {
    const { classes } = this.props;
    return (
      <React.Fragment>
        <Snackbar
          data-cy="system-delay-snackbar"
          anchorOrigin={{ vertical: 'top', horizontal: 'center' }}
          open={this.props.showDelay}
          message="UI is experiencing slowness or is unable to communicate with server"
          ContentProps={{
            classes: {
              root: this.props.classes.snackbar,
            },
          }}
          action={
            <div>
              <Button
                size="small"
                color="primary"
                onClick={this.closeNotification}
                data-cy="snooze-system-delay-notification"
              >
                Snooze for 1 hour
              </Button>
              <Button
                className={`${classes.moreInfoBtn} ${classes.iconBtn}`}
                onClick={this.toggleMoreInfoPane}
              >
                <InfoIcon />
              </Button>
            </div>
          }
        />
        {this.renderMoreInfo()}
      </React.Fragment>
    );
  }
}

const mapStateToProps = (state) => {
  return {
    activeDataSources: state.activeDataSources,
    showDelay: state.showDelay,
  };
};

const ConnectedSystemServicesDelay = connect(mapStateToProps)(SystemServicesDelayView);
const StyledConnectedSystemServices = withStyles(styles)(ConnectedSystemServicesDelay);
export default function SystemServicesDelay({ ...props }) {
  return (
    <Provider store={SystemDelayStore}>
      <StyledConnectedSystemServices {...props} />
    </Provider>
  );
}

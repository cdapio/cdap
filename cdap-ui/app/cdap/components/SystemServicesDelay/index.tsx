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
import DataSource from 'services/datasource';
import withStyles, { StyleRules, WithStyles } from '@material-ui/core/styles/withStyles';
import {
  XYPlot,
  makeVisFlexible,
  XAxis,
  YAxis,
  HorizontalGridLines,
  VerticalBarSeries,
  MarkSeries,
  DiscreteColorLegend,
  Hint,
} from 'react-vis';
import flatten from 'lodash/flatten';

interface IBindingRequestInfo {
  requestTimeFromClient: number;
  backendRequestTimeDuration: number;
}
interface IHealthCheckBindings {
  [key: string]: IBindingRequestInfo;
}

interface ISystemDelayProps extends WithStyles<StyleRules> {
  showDelay: boolean;
  activeDataSources: DataSource[];
}

interface ISystemDelayState {
  cleanChecksNeeded: number;
  requestDelayMap: Record<number, number>;
}

const EXPERIMENT_ID = 'system-delay-notification';
const snoozeTimelabel = `${EXPERIMENT_ID}-snoozetime`;
const HEALTH_CHECK_INTERVAL = 1000;
const DEFAULT_DELAY_TIME = 10000;
const CLEAN_CHECK_COUNT = 3;

const styles = (theme): StyleRules => {
  return {
    snackbar: {
      backgroundColor: theme.palette.grey[600],
      color: 'black',
    },
    graph: {
      position: 'fixed',
      right: 0,
      bottom: 0,
      background: 'white',
      border: '2px solid black',
      padding: '10px',
    },
  };
};

class SystemServicesDelayView extends React.Component<ISystemDelayProps> {
  public state: ISystemDelayState = {
    cleanChecksNeeded: 0,
    requestDelayMap: {},
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
    const currentTime = Date.now();
    const delayedTimeFromExperiment = getExperimentValue(EXPERIMENT_ID);
    const requestsTakingTime = flatten(
      this.props.activeDataSources
        .map((dataSource: DataSource) => {
          return dataSource.getBindingsForHealthCheck() as IHealthCheckBindings;
        })
        .filter((binding: IHealthCheckBindings) => {
          return !isNilOrEmpty(binding);
        })
        .map((binding: IHealthCheckBindings) => {
          return Object.keys(binding)
            .filter((k) => binding[k])
            .map((id, index) => {
              const {
                requestTimeFromClient: requestTime,
                backendRequestTimeDuration: networkDelay,
              } = binding[id];
              return { id, y: currentTime - requestTime, networkDelay };
            });
        })
    ).map((d, i) => ({ x: i, ...d }));
    const requestDelayMap = this.state.requestDelayMap;
    for (const request of requestsTakingTime) {
      requestDelayMap[request.id] = request.y;
    }
    this.setState({ requestDelayMap });
  };

  public render() {
    const data = [];
    let counter = 1;
    for (const [id, value] of Object.entries(this.state.requestDelayMap)) {
      data.push({
        id,
        x: counter++,
        y: value,
      });
    }
    // tslint:disable-next-line: no-console
    console.log(data);
    if (!Array.isArray(data) || !data.length) {
      return null;
    }
    return (
      <div className={this.props.classes.graph}>
        <XYPlot width={300} height={300}>
          <VerticalBarSeries data={data} />
          <XAxis />
          <YAxis
            tickFormat={function tickFormat(d) {
              return d >= 1000 ? `${(d / 1000).toFixed(1)}k` : d;
            }}
          />
        </XYPlot>
      </div>
    );
    //   return (
    //     <Snackbar
    //       data-cy="system-delay-snackbar"
    //       anchorOrigin={{ vertical: 'top', horizontal: 'center' }}
    //       open={this.props.showDelay}
    //       message="UI is experiencing slowness or is unable to communicate with server"
    //       ContentProps={{
    //         classes: {
    //           root: this.props.classes.snackbar,
    //         },
    //       }}
    //       action={
    //         <Button
    //           size="small"
    //           color="primary"
    //           onClick={this.closeNotification}
    //           data-cy="snooze-system-delay-notification"
    //         >
    //           Snooze for 1 hour
    //         </Button>
    //       }
    //     />
    //   );
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

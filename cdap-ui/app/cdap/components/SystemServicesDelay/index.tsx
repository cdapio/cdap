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
import { getExperimentValue, isExperimentEnabled, ONE_HOUR_SECONDS } from 'services/helpers';
import DataSource from 'services/datasource';
import withStyles, { StyleRules, WithStyles } from '@material-ui/core/styles/withStyles';

interface IHealthCheckBindings {
  [key: string]: number | null;
}

interface ISystemDelayProps extends WithStyles<StyleRules> {
  showDelay: boolean;
  activeDataSources: DataSource[];
}

interface ISystemDelayState {
  cleanChecksNeeded: number;
}

const EXPERIMENT_ID = 'system-delay-notification';
const snoozeTimelabel = `${EXPERIMENT_ID}-snoozetime`;
const HEALTH_CHECK_INTERVAL = 12000;
const DEFAULT_DELAY_TIME = 5000;
const CLEAN_CHECK_COUNT = 3;

const styles = (theme): StyleRules => {
  return {
    snackbar: {
      backgroundColor: theme.palette.grey[300],
      color: 'black',
    },
  };
};

class SystemServicesDelayView extends React.Component<ISystemDelayProps> {
  public state: ISystemDelayState = {
    cleanChecksNeeded: 0,
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
    delete window.localStorage[snoozeTimelabel];
    const delayedTimeFromExperiment = getExperimentValue(EXPERIMENT_ID);
    const SERVICES_DELAYED_TIME = delayedTimeFromExperiment
      ? parseInt(delayedTimeFromExperiment, 10) * 1000
      : DEFAULT_DELAY_TIME;
    const currentTime = Date.now();
    const hasDelayedBinding = this.props.activeDataSources.some((dataSource: DataSource) => {
      const bindings = dataSource.getBindingsForHealthCheck() as IHealthCheckBindings;
      return Object.keys(bindings).some((id) => {
        const requestTime = bindings[id];
        return requestTime && currentTime - requestTime > SERVICES_DELAYED_TIME;
      });
    });
    if (hasDelayedBinding) {
      // If there is atleast one delayed binding, show the delay and we need to wait for CLEAN_CHECK_COUNT
      // more cycles with no delayed bindings before we can remove the notification
      this.setState({ cleanChecksNeeded: CLEAN_CHECK_COUNT }, () => {
        if (!this.props.showDelay) {
          SystemDelayStore.dispatch({
            type: SystemDelayActions.showDelay,
          });
        }
      });
    } else {
      if (this.state.cleanChecksNeeded > 0) {
        // No delayed bindings and we need more checks before we can say there is no delay
        this.setState({ cleanChecksNeeded: this.state.cleanChecksNeeded - 1 });
      } else {
        // If we complete CLEAN_CHECK_COUNT intervals with no delays, we hide the notification
        if (this.props.showDelay) {
          SystemDelayStore.dispatch({
            type: SystemDelayActions.hideDelay,
          });
        }
      }
    }
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

  public render() {
    return (
      <Snackbar
        data-cy="system-delay-snackbar"
        anchorOrigin={{ vertical: 'top', horizontal: 'center' }}
        open={this.props.showDelay}
        message="Some system services are experiencing delays."
        ContentProps={{
          classes: {
            root: this.props.classes.snackbar,
          },
        }}
        action={
          <Button
            size="small"
            color="primary"
            onClick={this.closeNotification}
            data-cy="snooze-system-delay-notification"
          >
            Close
          </Button>
        }
      />
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

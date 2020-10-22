/*
 * Copyright © 2016 Cask Data, Inc.
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
import { Theme } from 'services/ThemeHelper';
import LoadingSVGCentered from 'components/LoadingSVGCentered';
import Loadable from 'react-loadable';
import classnames from 'classnames';
import ee from 'event-emitter';
import globalEvents from 'services/global-events';
import Button from '@material-ui/core/Button';
import withStyles, { WithStyles } from '@material-ui/core/styles/withStyles';

const styles = (theme) => {
  return {
    buttonLink: {
      ...theme.buttonLink,
      fontWeight: 300,
      color: theme.palette.grey[700],
    },
  };
};

const PlusButtonModal = Loadable({
  loader: () => import(/* webpackChunkName: "PlusButtonModal" */ 'components/PlusButtonModal'),
  loading: LoadingSVGCentered,
});

interface IHubButtonProps extends WithStyles<typeof styles> {
  className?: string;
  children: React.ReactNode;
}
interface IHubButtonState {
  showMarketPlace: boolean;
}

class HubButton extends React.PureComponent<IHubButtonProps, IHubButtonState> {
  private eventEmitter = ee(ee);
  public state = {
    showMarketPlace: false,
  };
  constructor(props) {
    super(props);
    this.eventEmitter.on(globalEvents.OPENMARKET, this.openHubModal);
    this.eventEmitter.on(globalEvents.CLOSEMARKET, this.closeHubModal);
  }

  public componentWillUnmount() {
    this.eventEmitter.off(globalEvents.OPENMARKET, this.openHubModal);
    this.eventEmitter.off(globalEvents.CLOSEMARKET, this.closeHubModal);
  }

  private onClickHandler = () => {
    const newState = !this.state.showMarketPlace;

    /**
     * Emitting these events here and listening to these events with methods on the same class can be weird
     * but we have usecases where we want to listen to open market event, say in app drawer which has to close
     * when market is opened. So instead of setting the state here directly we emit the event
     * and let listeners take action based on it instead of having separate events.
     */
    if (newState === false) {
      this.eventEmitter.emit(globalEvents.CLOSEMARKET);
    } else {
      this.eventEmitter.emit(globalEvents.OPENMARKET);
    }
  };

  private openHubModal = () => {
    this.setState({
      showMarketPlace: true,
    });
  };

  private closeHubModal = () => {
    this.setState({
      showMarketPlace: false,
    });
  };

  public render() {
    const { classes } = this.props;
    return (
      <React.Fragment>
        <Button
          className={classnames(classes.buttonLink)}
          onClick={this.onClickHandler}
          id="navbar-hub"
        >
          <div>
            <span
              className={classnames('cask-market-button', this.props.className, {
                active: this.state.showMarketPlace,
              })}
            >
              {this.props.children}
            </span>
          </div>
        </Button>
        <PlusButtonModal isOpen={this.state.showMarketPlace} onCloseHandler={this.onClickHandler} />
      </React.Fragment>
    );
  }
}

const HubButtinWithStyles = withStyles(styles)(HubButton);

export default function HubButtonWrapper() {
  if (Theme.showHub === false) {
    return null;
  }

  const featureName = Theme.featureNames.hub;

  return (
    <HubButtinWithStyles>
      <span className="hub-text-wrapper">{featureName}</span>
    </HubButtinWithStyles>
  );
}

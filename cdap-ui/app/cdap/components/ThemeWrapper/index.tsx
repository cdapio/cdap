/*
 * Copyright © 2018 Cask Data, Inc.
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
import { ThemeProvider as MuiThemeProvider } from '@material-ui/core/styles';
import createMuiTheme, { ThemeOptions, Theme } from '@material-ui/core/styles/createMuiTheme';
import {
  blue,
  grey,
  green,
  red,
  bluegrey,
  orange,
  yellow,
  white,
} from 'components/ThemeWrapper/colors';

interface IThemeWraperProps {
  render?: () => React.ReactNode;
  component?: React.ReactNode;
  children?: any;
}

export default class ThemeWrapper extends React.PureComponent<IThemeWraperProps> {
  private baseTheme: Theme = createMuiTheme({
    palette: {
      primary: {
        main: '#1a73e8',
      },
      blue,
      grey,
      green,
      red,
      bluegrey,
      orange,
      yellow,
      white,
    },
    navbarBgColor: 'var(--navbar-color)',
    buttonLink: {
      '&:hover': {
        color: 'inherit',
        backgroundColor: 'rgba(255, 255, 255, 0.10)',
      },
      fontSize: '1rem',
      color: 'white',
    },
    iconButtonFocus: {
      '&:focus': {
        outline: 'none',
        backgroundColor: 'rgba(255, 255, 255, 0.10)',
      },
    },
    grow: {
      flexGrow: 1,
    },
    typography: {
      fontSize: 13,
      fontFamily: 'var(--font-family)',
      useNextVariants: true,
    },
    zIndex: {
      drawer: 1300, // Must be < z-index of NUX in services/GuidedTour/GuidedTour.scss
    },
    overrides: {
      MuiTypography: {
        caption: {
          fontSize: '0.92rem',
        },
      },
    },
    Spacing: (factor) => [0, 4, 8, 16, 24, 32, 40, 48, 56, 64][factor],
  } as ThemeOptions);
  public render() {
    let Component;
    if (this.props.component) {
      Component = this.props.component;
    }
    if (!this.props.render && !this.props.component && !this.props.children) {
      return null;
    }
    if (this.props.children) {
      return <MuiThemeProvider theme={this.baseTheme}>{this.props.children}</MuiThemeProvider>;
    }
    if (this.props.render) {
      return <MuiThemeProvider theme={this.baseTheme}>{this.props.render()}</MuiThemeProvider>;
    }
    return (
      <MuiThemeProvider theme={this.baseTheme}>
        <Component />
      </MuiThemeProvider>
    );
  }
}

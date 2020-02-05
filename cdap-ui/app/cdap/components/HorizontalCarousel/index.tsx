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
import IconButton from '@material-ui/core/IconButton';
import KeyboardArrowRight from '@material-ui/icons/KeyboardArrowRight';
import KeyboardArrowLeft from '@material-ui/icons/KeyboardArrowLeft';
import throttle from 'lodash/throttle';

const styles = (): StyleRules => {
  return {
    contentContainer: {
      paddingLeft: '60px',
      paddingRight: '60px',
      whiteSpace: 'nowrap',
      overflowX: 'scroll',
      scrollSnapType: 'x proximity',
      scrollPadding: '60px',
      display: 'flex',
      scrollBehavior: 'smooth',
      '-ms-overflow-style': 'none',
      '&::-webkit-scrollbar': {
        display: 'none',
      },
      '&:after': {
        content: '""',
        borderLeft: `30px solid transparent`, // display flex does not allow honor end padding
      },

      '& > *': {
        scrollSnapAlign: 'end',
      },
    },
    arrowsContainer: {
      position: 'relative',
      height: 0,
      width: '100%',
    },
    arrow: {
      position: 'absolute',
      top: '50px',
      transform: 'translateY(-50%)',
      '&:focus': {
        outline: 0,
      },
    },
    back: {
      left: 0,
    },
    forward: {
      right: 0,
    },
    icon: {
      fontSize: '45px',
    },
  };
};

interface IHorizontalCarouselProps extends WithStyles<typeof styles> {
  scrollAmount: number;
}

interface IHorizontalCarouselState {
  leftDisabled: boolean;
  rightDisabled: boolean;
}

class HorizontalCarouselView extends React.PureComponent<
  IHorizontalCarouselProps,
  IHorizontalCarouselState
> {
  private carouselRef: React.RefObject<HTMLDivElement>;

  constructor(props) {
    super(props);

    this.carouselRef = React.createRef();
  }

  private isScrollable = () => {
    if (!this.carouselRef || !this.carouselRef.current) {
      return false;
    }

    return this.carouselRef.current.scrollWidth > this.carouselRef.current.clientWidth;
  };

  private leftDisabled = () => {
    if (!this.isScrollable()) {
      return true;
    }

    const carousel = this.carouselRef.current;

    return carousel.scrollLeft === 0;
  };

  private rightDisabled = () => {
    if (!this.isScrollable()) {
      return true;
    }

    const carousel = this.carouselRef.current;

    return carousel.scrollLeft + carousel.clientWidth >= carousel.scrollWidth;
  };

  private setArrowDisabled = () => {
    const leftDisabled = this.leftDisabled();
    const rightDisabled = this.rightDisabled();

    if (this.state.leftDisabled !== leftDisabled || this.state.rightDisabled !== rightDisabled) {
      this.setState({
        leftDisabled,
        rightDisabled,
      });
    }
  };

  public state = {
    leftDisabled: this.leftDisabled(),
    rightDisabled: this.rightDisabled(),
  };

  private throttledSetArrowDisabled = throttle(this.setArrowDisabled, 300, {
    trailing: true,
    leading: true,
  });

  public componentDidMount() {
    this.carouselRef.current.addEventListener('scroll', this.throttledSetArrowDisabled);
    window.addEventListener('resize', this.throttledSetArrowDisabled);
  }

  public componentDidUpdate() {
    this.setArrowDisabled();
  }

  public componentWillUnmount() {
    this.throttledSetArrowDisabled.cancel();
    window.removeEventListener('resize', this.throttledSetArrowDisabled);
  }

  private scrollRight = () => {
    this.carouselRef.current.scrollLeft += this.props.scrollAmount;
  };

  private scrollLeft = () => {
    this.carouselRef.current.scrollLeft -= this.props.scrollAmount;
  };

  public render() {
    const { classes } = this.props;

    return (
      <div className={classes.root}>
        <div className={classes.arrowsContainer}>
          <IconButton
            size="small"
            className={`${classes.arrow} ${classes.back}`}
            onClick={this.scrollLeft}
            disabled={this.state.leftDisabled}
          >
            <KeyboardArrowLeft className={classes.icon} />
          </IconButton>
          <IconButton
            size="small"
            className={`${classes.arrow} ${classes.forward}`}
            onClick={this.scrollRight}
            disabled={this.state.rightDisabled}
          >
            <KeyboardArrowRight className={classes.icon} />
          </IconButton>
        </div>
        <div className={classes.contentContainer} ref={this.carouselRef}>
          {this.props.children}
        </div>
      </div>
    );
  }
}

const HorizontalCarousel = withStyles(styles)(HorizontalCarouselView);
export default HorizontalCarousel;

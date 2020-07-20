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

import React from 'react';
import ReactDOM from 'react-dom';
import FllHeader from 'components/FieldLevelLineage/v2/FllHeader';
import FllTable from 'components/FieldLevelLineage/v2/FllTable';
import OperationsModal from 'components/FieldLevelLineage/v2/OperationsModal';
import {
  ITablesList,
  IField,
  ILinkSet,
} from 'components/FieldLevelLineage/v2/Context/FllContextHelper';
import withStyles, { StyleRules } from '@material-ui/core/styles/withStyles';
import { Consumer, FllContext } from 'components/FieldLevelLineage/v2/Context/FllContext';
import * as d3 from 'd3';
import debounce from 'lodash/debounce';
import { grey, orange } from 'components/ThemeWrapper/colors';
import If from 'components/If';
import TopPanel from 'components/FieldLevelLineage/v2/TopPanel';
import LoadingSVGCentered from 'components/LoadingSVGCentered';
import Heading, { HeadingTypes } from 'components/Heading';
import T from 'i18n-react';

const PREFIX = 'features.FieldLevelLineage';

const styles = (theme): StyleRules => {
  return {
    wrapper: {
      overflowY: 'scroll',
      background: theme.palette.grey[700],
      height: '100%',
      minHeight: '480px', // to avoid cutting off date picker calendar
      width: '100%',
      overflowX: 'hidden',
    },
    root: {
      paddingLeft: '100px',
      paddingRight: '100px',
      paddingTop: '5px',
      display: 'flex',
      justifyContent: 'space-between',
      position: 'relative',
      overflow: 'hidden',
    },
    container: {
      position: 'absolute',
      height: '100%',
      width: '100%',
      pointerEvents: 'none',
      overflow: 'visible',
    },
    summaryCol: {
      maxWidth: '30%',
    },
    errorContainer: {
      margin: '25vh auto 0',
      maxWidth: '50vw',
    },
    errorMessage: {
      fontWeight: 500,
    },
    errorSeparator: {
      backgroundColor: theme.palette.grey[200],
    },
    errorMessageSuggestion: {
      fontSize: '14px',
    },
  };
};

interface ILineageState {
  activeField: IField;
  activeCauseSets: ITablesList;
  activeImpactSets: ITablesList;
  activeLinks: ILinkSet;
}

class LineageSummary extends React.Component<{ classes }, ILineageState> {
  private myRef;

  private drawLineFromLink({ source, destination }, isSelected = false) {
    // get source and destination elements and their coordinates
    const sourceId = source.id.replace(/\./g, '\\.');
    const destId = destination.id.replace(/\./g, '\\.');

    const sourceEl = d3.select(`#${sourceId}`);
    const destEl = d3.select(`#${destId}`);

    const offsetX = -100; // From the padding on the LineageSummary
    const offsetY =
      -(this.myRef as HTMLElement).getBoundingClientRect().top - 65 + this.myRef.scrollTop; // From TopPanel, FllHeader, and scroll

    const sourceXY = sourceEl.node().getBoundingClientRect();
    const destXY = destEl.node().getBoundingClientRect();

    const sourceX1 = sourceXY.right + offsetX;
    const sourceY1 = sourceXY.top + 0.5 * sourceXY.height + offsetY;
    const sourceX2 = destXY.left + offsetX;
    const sourceY2 = destXY.top + 0.5 * sourceXY.height + offsetY;
    // draw an edge from line start to line end
    const linkContainer = isSelected
      ? d3.select('#selected-links')
      : d3.select(`#${sourceId}_${destId}`);

    const third = (sourceX2 - sourceX1) / 3;

    // Draw a line with a bit of curve between the straight parts
    const lineGenerator = d3.line().curve(d3.curveMonotoneX);

    const points = [
      [sourceX1, sourceY1],
      [sourceX1 + third, sourceY1],
      [sourceX2 - third, sourceY2],
      [sourceX2, sourceY2],
    ];

    const edgeColor = isSelected ? orange[50] : grey[300];

    linkContainer
      .append('path')
      .style('stroke', edgeColor)
      .style('stroke-width', '1')
      .style('fill', 'none')
      .attr('d', lineGenerator(points));

    // draw left anchor
    const anchorHeight = 8;
    const anchorRx = 1.8;
    linkContainer
      .append('rect')
      .attr('x', sourceX1 - anchorHeight * 0.5)
      .attr('y', sourceY1 - anchorHeight * 0.5)
      .attr('width', anchorHeight)
      .attr('height', anchorHeight)
      .attr('rx', anchorRx)
      .attr('pointer-events', 'fill') // To make rect clickable
      .style('fill', edgeColor);

    // draw right anchor
    linkContainer
      .append('rect')
      .attr('x', sourceX2 - anchorHeight * 0.5)
      .attr('y', sourceY2 - anchorHeight * 0.5)
      .attr('width', anchorHeight)
      .attr('height', anchorHeight)
      .attr('rx', anchorRx)
      .attr('pointer-events', 'fill') // To make rect clickable
      .style('fill', edgeColor);
  }

  private clearCanvas() {
    // clear any existing links and anchors
    d3.select('#links-container')
      .selectAll('path,rect')
      .remove();
  }

  // Draws only active links
  private drawActiveLinks(activeLinks: ILinkSet) {
    this.clearCanvas();

    const allLinks = activeLinks.incoming.concat(activeLinks.outgoing);

    allLinks.forEach((link) => {
      this.drawLineFromLink(link, true);
    });
  }

  private drawLinks = () => {
    if (this.context.loadingLineage) {
      return;
    }
    const allLinks = this.context.links;
    const activeField = this.context.activeField;

    const activeFieldId = activeField ? activeField.id : undefined;
    const comboLinks = allLinks.incoming.concat(allLinks.outgoing);
    if (comboLinks.length === 0) {
      return;
    }
    this.clearCanvas();

    comboLinks.forEach((link) => {
      const isSelected = link.source.id === activeFieldId || link.destination.id === activeFieldId;
      this.drawLineFromLink(link, isSelected);
    });
  };

  private debounceRedrawLinks = debounce(this.drawLinks, 200);

  public componentDidUpdate() {
    const { showingOneField, activeLinks, activeField } = this.context;

    // if user has just clicked "View Cause and Impact"
    if (showingOneField) {
      this.clearCanvas();
      this.drawActiveLinks(activeLinks);
      return;
    }

    if (activeField) {
      d3.select(`#${activeField.id}`).classed('selected', true);
    }
    this.clearCanvas();
    this.drawLinks();
  }

  public componentWillUnmount() {
    window.removeEventListener('resize', this.debounceRedrawLinks);
  }

  public componentDidMount() {
    this.drawLinks();
    this.myRef = ReactDOM.findDOMNode(this);
    window.addEventListener('resize', this.debounceRedrawLinks);
  }

  public render() {
    return (
      <Consumer>
        {({
          causeSets,
          target,
          targetFields,
          impactSets,
          links,
          activeLinks,
          activeCauseSets,
          activeImpactSets,
          showingOneField,
          loadingLineage,
          error,
        }) => {
          let visibleLinks = links;
          let visibleCauseSets = causeSets;
          let visibleImpactSets = impactSets;

          if (showingOneField) {
            visibleLinks = activeLinks;
            visibleCauseSets = activeCauseSets;
            visibleImpactSets = activeImpactSets;
          }
          const allLinks = visibleLinks.incoming.concat(visibleLinks.outgoing);
          const loadingIndicator = (
            <div className="loading-container text-center">
              <LoadingSVGCentered />
            </div>
          );

          if (loadingLineage) {
            return (
              <div className={this.props.classes.wrapper}>
                <TopPanel datasetId={target} />
                {loadingIndicator}
              </div>
            );
          }

          if (error) {
            return (
              <div className={this.props.classes.wrapper}>
                <TopPanel datasetId={target} />

                <div className={this.props.classes.root}>
                  <div className={this.props.classes.errorContainer}>
                    <Heading
                      type={HeadingTypes.h4}
                      label={error}
                      className={this.props.classes.errorMessage}
                    />
                    <hr className={this.props.classes.errorSeparator} />
                    <div className={this.props.classes.errorMessageSuggestion}>
                      <span>{T.translate(`${PREFIX}.Error.suggestionHeading`)}</span>
                      <br />
                      <span>{T.translate(`${PREFIX}.Error.suggestionMessage`)}</span>
                    </div>
                  </div>
                </div>
              </div>
            );
          }

          return (
            <div className={this.props.classes.wrapper}>
              <TopPanel datasetId={target} />
              <If condition={!loadingLineage && !error}>
                <div className={this.props.classes.root} id="fll-container">
                  <svg id="links-container" className={this.props.classes.container}>
                    <g>
                      {allLinks.map((link) => {
                        const id = `${link.source.id}_${link.destination.id}`;
                        return <svg id={id} key={id} className="fll-link" />;
                      })}
                    </g>
                    <g id="selected-links" />
                  </svg>
                  <div data-cy="cause-fields" className={this.props.classes.summaryCol}>
                    <FllHeader type="cause" total={Object.keys(visibleCauseSets).length} />
                    <If condition={Object.keys(visibleCauseSets).length === 0}>
                      <FllTable type="cause" />
                    </If>
                    {Object.entries(visibleCauseSets).map(([tableId, tableInfo]) => {
                      const isActive = tableId in activeCauseSets;
                      return (
                        <FllTable
                          key={tableId}
                          tableId={tableId}
                          tableInfo={tableInfo}
                          type="cause"
                          isActive={isActive}
                        />
                      );
                    })}
                  </div>
                  <div data-cy="target-fields" className={this.props.classes.summaryCol}>
                    <FllHeader type="target" total={targetFields.fields.length} />
                    <FllTable tableId={target} tableInfo={targetFields} type="target" />
                  </div>
                  <div data-cy="impact-fields" className={this.props.classes.summaryCol}>
                    <FllHeader type="impact" total={Object.keys(visibleImpactSets).length} />
                    <If condition={Object.keys(visibleImpactSets).length === 0}>
                      <FllTable type="impact" />
                    </If>
                    {Object.entries(visibleImpactSets).map(([tableId, tableInfo]) => {
                      const isActive = tableId in activeImpactSets;
                      return (
                        <FllTable
                          key={tableId}
                          tableId={tableId}
                          tableInfo={tableInfo}
                          type="impact"
                          isActive={isActive}
                        />
                      );
                    })}
                  </div>
                  <OperationsModal />
                </div>
              </If>
            </div>
          );
        }}
      </Consumer>
    );
  }
}

LineageSummary.contextType = FllContext;

const StyledLineageSummary = withStyles(styles)(LineageSummary);

export default StyledLineageSummary;

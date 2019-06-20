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
import FllHeader from 'components/FieldLevelLineage/v2/FllHeader';
import FllTable from 'components/FieldLevelLineage/v2/FllTable';
import { ITableFields, ILink } from 'components/FieldLevelLineage/v2/Context/FllContextHelper';
import withStyles from '@material-ui/core/styles/withStyles';
import { Consumer } from 'components/FieldLevelLineage/v2/Context/FllContext';
import * as d3 from 'd3';
import debounce from 'lodash/debounce';
import { grey, orange } from 'components/ThemeWrapper/colors';

const styles = (theme) => {
  return {
    root: {
      paddingLeft: '100px',
      paddingRight: '100px',
      display: 'flex',
      justifyContent: 'space-between',
      position: 'relative' as 'relative',
    },
    container: {
      position: 'absolute' as 'absolute',
      height: '110%', // this seems like cheating
      width: '100%',
      pointerEvents: 'none' as 'none',
      overflow: 'visible',
    },
  };
};

interface ILineageState {
  activeField: string;
  activeCauseSets: ITableFields;
  activeImpactSets: ITableFields;
  activeLinks: ILink[];
  showingOneField: boolean;
}

class LineageSummary extends React.Component<{ classes }, ILineageState> {
  constructor(props) {
    super(props);
    this.state = {
      activeField: null,
      activeCauseSets: null,
      activeImpactSets: null,
      activeLinks: null,
      showingOneField: false,
    };
  }
  private allLinks = [];

  // TO DO: This currently breaks when the window is scrolled before drawing
  private drawLineFromLink({ source, destination }, isSelected = false) {
    // get source and destination elements and their coordinates
    const sourceEl = d3.select(`#${source.id}`);
    const destEl = d3.select(`#${destination.id}`);

    const offsetX = -100; // From the padding on the LineageSummary
    const offsetY = -48 + window.pageYOffset; // From the FllHeader

    const sourceXY = sourceEl.node().getBoundingClientRect();
    const destXY = destEl.node().getBoundingClientRect();

    const sourceX1 = sourceXY.right + offsetX;
    const sourceY1 = sourceXY.top + offsetY + 0.5 * sourceXY.height;
    const sourceX2 = destXY.left + offsetX;
    const sourceY2 = destXY.top + offsetY + 0.5 * sourceXY.height;

    // draw an edge from line start to line end
    const linkContainer = isSelected
      ? d3.select('#selected-links')
      : d3.select(`#${source.id}_${destination.id}`);

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

  private drawActiveLinks() {
    this.clearCanvas();

    this.state.activeLinks.forEach((link) => {
      this.drawLineFromLink(link, true);
    });
  }

  private drawLinks(activeFieldId = null) {
    this.clearCanvas();

    const activeLinks = [];
    this.allLinks.forEach((link) => {
      const isSelected = link.source.id === activeFieldId || link.destination.id === activeFieldId;
      if (isSelected) {
        activeLinks.push(link);
      }
      this.drawLineFromLink(link, isSelected);
    });
    this.setState(() => ({
      ...this.state,
      activeLinks,
    }));
  }

  private handleFieldClick(e) {
    const activeField = (e.target as HTMLAreaElement).id;
    if (!activeField) {
      return;
    }
    d3.select(`#${this.state.activeField}`).classed('selected', false);
    this.setState(
      {
        ...this.state,
        activeField,
      },
      () => {
        d3.select(`#${activeField}`).classed('selected', true);
        this.drawLinks(activeField);
      }
    );
  }

  // example: "target_ns-default_ds-Employee_Data_fd-id"
  private parseFieldId(fieldId) {
    let truncId;
    const pref = fieldId.slice(0, 5);
    const type = pref === 'cause' ? pref : pref + 't';
    // chop off the "target_" or "cause_" delimiter
    truncId = type === 'cause' ? fieldId.slice(6) : fieldId.slice(7);
    // Look for "_fd-" delimiter - everything before that is the tableId, and everything after is the fieldname
    const fdIndex = truncId.indexOf('_fd-');
    const tableId = truncId.slice(0, fdIndex);
    // also need to get the dataset name out of datasetId
    const dsName = tableId.slice(tableId.indexOf('_ds-') + 4);
    const fieldName = truncId.slice(fdIndex + 4);
    const field = {
      id: fieldId,
      name: fieldName,
      group: dsName,
    };

    return { field, tableId, type };
  }

  private getActiveSets() {
    const activeCauseSets = {};
    const activeImpactSets = {};

    this.state.activeLinks.forEach((link) => {
      // for each link, look at id prefix to find the field that is not the target and add to the activeCauseSets or activeImpactSets
      const nonTargetFd = link.source.type !== 'target' ? link.source : link.destination;
      const tableToUpdate = nonTargetFd.type === 'cause' ? activeCauseSets : activeImpactSets;
      const tableId = `ns-${nonTargetFd.namespace}_ds-${nonTargetFd.dataset}`; // used as unique key to make sure we don't duplicate fields
      if (!(tableId in tableToUpdate)) {
        tableToUpdate[tableId] = [];
      }
      tableToUpdate[tableId].push(nonTargetFd);
    });
    this.setState(
      {
        ...this.state,
        activeCauseSets,
        activeImpactSets,
      },
      () => {
        this.setState({
          ...this.state,
          showingOneField: true,
        });
      }
    );
  }

  public componentDidUpdate() {
    if (this.state.showingOneField) {
      this.drawActiveLinks();
    }
  }

  private handleViewCauseImpact() {
    this.getActiveSets();
  }

  private handleReset() {
    this.setState(() => ({
      ...this.state,
      showingOneField: false,
    }));
  }

  public componentWillUnmount() {
    window.removeEventListener('resize', debounce(this.drawLinks.bind(this), 1));
  }

  public componentDidMount() {
    this.drawLinks();
    window.addEventListener('resize', debounce(this.drawLinks.bind(this), 1));
  }
  public render() {
    return (
      <Consumer>
        {({
          causeSets,
          target,
          targetFields,
          impactSets,
          firstCause,
          firstImpact,
          firstField,
          links,
        }) => {
          this.allLinks = links;
          let visibleLinks = this.allLinks;
          let visibleCauseSets = causeSets;
          let visibleImpactSets = impactSets;

          if (this.state.showingOneField) {
            visibleLinks = this.state.activeLinks;
            visibleCauseSets = this.state.activeCauseSets;
            visibleImpactSets = this.state.activeImpactSets;
          }

          return (
            <div className={this.props.classes.root} id="fll-container">
              <svg id="links-container" className={this.props.classes.container}>
                <g>
                  {visibleLinks.map((link) => {
                    const id = `${link.source.id}_${link.destination.id}`;
                    return <svg id={id} key={id} className="fll-link" />;
                  })}
                </g>
                <g id="selected-links" />
              </svg>
              <div>
                <FllHeader
                  type="cause"
                  first={firstCause}
                  total={Object.keys(visibleCauseSets).length}
                />
                {Object.keys(visibleCauseSets).map((key) => {
                  return (
                    <FllTable
                      clickFieldHandler={this.handleFieldClick.bind(this)}
                      key={key}
                      tableId={key}
                      fields={visibleCauseSets[key]}
                      activeField={this.state.activeField}
                      showingOneField={this.state.showingOneField}
                    />
                  );
                })}
              </div>
              <div>
                <FllHeader
                  type="target"
                  first={firstField}
                  total={Object.keys(targetFields).length}
                />
                <FllTable
                  clickFieldHandler={this.handleFieldClick.bind(this)}
                  isTarget={true}
                  tableId={target}
                  fields={targetFields}
                  activeField={this.state.activeField}
                  viewCauseImpactHandler={this.handleViewCauseImpact.bind(this)}
                  showingOneField={this.state.showingOneField}
                  resetHandler={this.handleReset.bind(this)}
                />
              </div>
              <div>
                <FllHeader
                  type="impact"
                  first={firstImpact}
                  total={Object.keys(visibleImpactSets).length}
                />
                {Object.keys(visibleImpactSets).map((key) => {
                  return (
                    <FllTable
                      clickFieldHandler={this.handleFieldClick.bind(this)}
                      key={key}
                      tableId={key}
                      fields={visibleImpactSets[key]}
                      activeField={this.state.activeField}
                      showingOneField={this.state.showingOneField}
                    />
                  );
                })}
              </div>
            </div>
          );
        }}
      </Consumer>
    );
  }
}

const StyledLineageSummary = withStyles(styles)(LineageSummary);

export default StyledLineageSummary;

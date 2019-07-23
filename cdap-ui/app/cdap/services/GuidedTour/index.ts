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

import Shepherd from 'shepherd.js';
import 'shepherd.js/dist/css/shepherd-theme-arrows-plain-buttons.css';
import 'services/GuidedTour/GuidedTour.scss';

interface IStepButton {
  text: string;
  classes?: string;
  action: () => void;
}

export interface ITourStep {
  id: string;
  title?: string;
  text: string | string[];
  buttons?: IStepButton[];
  shouldFocus?: boolean;
  classes?: string;
  attachTo:
    | string
    | {
        element: string | HTMLElement;
        on: string;
      };
  popperOptions?: object;
}

const GuidedTour = function() {
  this.addSteps = (stepsArray: ITourStep[]) => {
    stepsArray.forEach((step, index) => {
      const stepId: string = step.id;

      const stepObj: ITourStep = {
        ...step,
      };

      delete stepObj.id;

      const countText = `${index + 1} of ${stepsArray.length}`;

      if (typeof stepObj.text === 'string') {
        stepObj.text = [stepObj.text, countText];
      } else if (Array.isArray(stepObj.text)) {
        stepObj.text.push(countText);
      }

      // next-btn class used for selecting button in cypress tests
      const nextButton = {
        text: 'Next',
        classes: 'btn btn-primary next-btn',
        action: () => {
          this.next();
        },
      };
      // complete-btn class used for selecting button in cypress tests
      const completeButton = {
        text: 'Finish',
        classes: 'btn btn-primary complete-btn',
        action: () => {
          this.complete();
        },
      };
      // prev-btn class used for selecting button in cypress tests
      const previousButton = {
        text: 'Previous',
        classes: 'btn btn-secondary float-left prev-btn',
        action: () => {
          this.back();
        },
      };

      if (!stepObj.buttons) {
        stepObj.buttons = [];

        if (index === stepsArray.length - 1) {
          stepObj.buttons.push(completeButton);
        } else {
          stepObj.buttons.push(nextButton);
        }

        if (index !== 0) {
          stepObj.buttons.unshift(previousButton);
        }
      }

      this.addStep(stepId, stepObj);
    });
  };
};

GuidedTour.prototype = new Shepherd.Tour({
  defaults: {
    classes: 'guided-tour-tooltip',
    showCancelLink: true,
  },
});
GuidedTour.prototype.constructor = GuidedTour;

export default GuidedTour;

/*
 * Copyright © 2020 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the 'License'); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an 'AS IS' BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
*/
import { dataCy } from '../helpers';
import {
    loginIfRequired,
    getArtifactsPoll,
} from '../helpers';
let headers = {};
const PIPELINE_NAME = `schedule_test_pipeline_${Date.now()}`;
const SCHEDULE_REPEAT_HOURLY = 'Hourly';
const SCHEDULE_REPEAT_MONTHLY = 'Monthly';
const SCHEULE_REPEAT_DAY_OF_MONTH = '5';
const SCHEDULE_REPEAT_HOURS = "2";
const SCHEDULE_MAX_CONCURRENT_RUNS = "3";
describe('Pipeline Studio', () => {
    // Uses API call to login instead of logging in manually through UI
    before(() => {
        loginIfRequired().then(() => {
            cy.getCookie('CDAP_Auth_Token').then((cookie) => {
                if (!cookie) {
                    return;
                }
                headers = {
                    Authorization: 'Bearer ' + cookie.value,
                };
            });
        });
    });

    beforeEach(() => {
        getArtifactsPoll(headers);
    });

    after(() => {
        // Delete the pipeline to clean up
        cy.cleanup_pipelines(headers, PIPELINE_NAME);
    });

    it('Should be rendered correctly', () => {
        cy.visit('/pipelines/ns/default/studio');
    });

    it('Should create a pipeline and schedule it', () => {
        cy.visit('/pipelines/ns/default/studio');
        cy.url().should('include', '/studio');
        cy.upload_pipeline(
            'pipeline_with_macros.json',
            '#pipeline-import-config-link > input[type="file"]'
        ).then((subject) => {
            expect(subject.length).to.be.eq(1);
        });
        cy.get('[title="Airport_source"').should('exist');
        cy.get('.pipeline-name').click();
        cy.get('#pipeline-name-input')
            .clear()
            .type(PIPELINE_NAME)
            .type('{enter}');
        cy.get(dataCy('pipeline-schedule-modeless-btn')).click();
        cy.get(dataCy('schedule-run-repeats')).click();
        cy.get(`${dataCy('schedule-run-repeats')} select`).select(SCHEDULE_REPEAT_HOURLY);
        cy.get(`${dataCy('schedule-repeats-every-hourly')} select`).select(SCHEDULE_REPEAT_HOURS);
        cy.get(`${dataCy('schedule-concurrent-runs')} select`).select(SCHEDULE_MAX_CONCURRENT_RUNS);
        cy.get(dataCy('save-schedule-btn-studio')).click();
    });

    it('Should persist saved basic schedule in studio', () => {
        cy.get(dataCy('pipeline-schedule-modeless-btn')).click();
        cy.get(`${dataCy('schedule-run-repeats')} select`).should('have.value', SCHEDULE_REPEAT_HOURLY);
        cy.get(`${dataCy('schedule-repeats-every-hourly')} select`).should('have.value', SCHEDULE_REPEAT_HOURS);
        cy.get(`${dataCy('schedule-concurrent-runs')} select`).should('have.value', SCHEDULE_MAX_CONCURRENT_RUNS);
        cy.get(dataCy('save-schedule-btn-studio')).click();
        cy.get(dataCy('deploy-pipeline-btn')).click();
        cy.get(dataCy('Deployed')).should('exist');
    });
    it('Should persist saved basic schedule in pipeline details',()=>{
        cy.get(dataCy('pipeline-scheduler-btn')).contains('Schedule');
        cy.get(dataCy('pipeline-scheduler-btn')).click();
        cy.get(`${dataCy('schedule-run-repeats')} select`).should('have.value', SCHEDULE_REPEAT_HOURLY);
        cy.get(`${dataCy('schedule-repeats-every-hourly')} select`).should('have.value', SCHEDULE_REPEAT_HOURS);
        cy.get(`${dataCy('schedule-concurrent-runs')} select`).should('have.value', SCHEDULE_MAX_CONCURRENT_RUNS);
        cy.get(`${dataCy('schedule-run-repeats')} select`).select(SCHEDULE_REPEAT_MONTHLY);
        cy.get(`${dataCy('schedule-repeats-every-monthly')} select`).select(SCHEULE_REPEAT_DAY_OF_MONTH);
        cy.get(`${dataCy('schedule-concurrent-runs')} select`).select(SCHEDULE_MAX_CONCURRENT_RUNS);
        cy.get(dataCy('save-schedule-btn')).click();
        cy.get(dataCy('pipeline-scheduler-btn')).click();
        cy.get(`${dataCy('schedule-run-repeats')} select`).should('have.value', SCHEDULE_REPEAT_MONTHLY);
        cy.get(`${dataCy('schedule-repeats-every-monthly')} select`).should('have.value', SCHEULE_REPEAT_DAY_OF_MONTH);
        cy.get(`${dataCy('schedule-concurrent-runs')} select`).should('have.value', SCHEDULE_MAX_CONCURRENT_RUNS);
        cy.get(dataCy('save-start-schedule-btn')).click();
        cy.get(dataCy('pipeline-scheduler-btn')).should('have.text', 'Unschedule');
    });

    it('should save the default schedule', () => {
        cy.visit('/pipelines/ns/default/studio');
        cy.create_simple_pipeline();
    
        cy.get(dataCy('pipeline-schedule-modeless-btn')).click();
    
        cy.get(dataCy('save-schedule-btn-studio')).click();
    
        cy.get_pipeline_json().then((pipelineConfig) => {
          expect(pipelineConfig.config.schedule).eq('0 * * * *');
        });
      });
    
      it('should save an updated basic schedule', () => {
        cy.visit('/pipelines/ns/default/studio');
        cy.create_simple_pipeline();
    
        cy.get(dataCy('pipeline-schedule-modeless-btn')).click();
    
        cy.get(dataCy('schedule-repeats-every-daily')).within(() => {
          cy.get('select').select('4');
        });
    
        cy.get(dataCy('save-schedule-btn-studio')).click();
    
        cy.get_pipeline_json().then((pipelineConfig) => {
          expect(pipelineConfig.config.schedule).eq('0 1 */4 * *');
        });
      });
    
      it('should save an updated advanced schedule', () => {
        cy.visit('/pipelines/ns/default/studio');
        cy.create_simple_pipeline();
    
        cy.get(dataCy('pipeline-schedule-modeless-btn')).click();
    
        cy.get(dataCy('switch-view-advanced')).click();
    
        // Set Cron for 4:30 AM and PM
        cy.get(dataCy('advanced-input-min')).clear().type('30')
        cy.get(dataCy('advanced-input-hour')).clear().type('4,16')
    
        cy.get(dataCy('save-schedule-btn-studio')).click();
    
        cy.get_pipeline_json().then((pipelineConfig) => {
          expect(pipelineConfig.config.schedule).eq('030 04,16 * * *');
        });
      });
    
      it('should save let the user set max concurrent runs', () => {
        cy.visit('/pipelines/ns/default/studio');
        cy.create_simple_pipeline();
    
        cy.get(dataCy('pipeline-schedule-modeless-btn')).click();
    
        cy.get(dataCy('schedule-concurrent-runs')).within(() => {
          cy.get('select').select('6');
        })
    
        cy.get(dataCy('save-schedule-btn-studio')).click();
    
        cy.get_pipeline_json().then((pipelineConfig) => {
          expect(pipelineConfig.config.maxConcurrentRuns).eq(6);
        });
      });
});
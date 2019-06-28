var request = require('request'),
  fs = require('fs'),
  log4js = require('log4js');

const programsResolver = {
  ApplicationDetail: {
    async programs(parent, args, context, info) {
      return await (new Promise((resolve, reject) => {
        const programs = parent.programs
        const type = args.type

        if (type == null) {
          resolve(programs)
        }
        else {
          typePrograms = programs.filter(
            function (program) {
              return program.type == type
            }
          );
          resolve(typePrograms);
        }
      }));
    }
  }
}

const programRecordResolvers = programsResolver

module.exports = {
  programRecordResolvers
}

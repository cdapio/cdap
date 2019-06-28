const programsTypeResolver = {
  ProgramRecord: {
    async __resolveType(parent, args, context, info) {
      return await(new Promise((resolve, reject) => {
        switch(parent.type) {
          case 'Mapreduce': resolve('MapReduce')
          case 'Workflow': resolve('Workflow')
          default: resolve(null)
        }
      }));
    }
  }
}

const programRecordTypeResolvers = programsTypeResolver

module.exports = {
	programRecordTypeResolvers
}

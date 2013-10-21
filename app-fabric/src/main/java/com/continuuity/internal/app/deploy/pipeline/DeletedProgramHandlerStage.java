package com.continuuity.internal.app.deploy.pipeline;

import com.continuuity.api.ProgramSpecification;
import com.continuuity.app.Id;
import com.continuuity.app.program.Type;
import com.continuuity.app.store.Store;
import com.continuuity.data2.transaction.queue.QueueAdmin;
import com.continuuity.internal.app.deploy.ProgramTerminator;
import com.continuuity.pipeline.AbstractStage;
import com.google.common.reflect.TypeToken;

import java.util.List;

/**
 * Deleted program handler stage. Figures out which programs are deleted and handles callback.
 */
public class DeletedProgramHandlerStage extends AbstractStage<ApplicationSpecLocation> {

  private final Store store;
  private final ProgramTerminator programTerminator;
  private final QueueAdmin queueAdmin;

  public DeletedProgramHandlerStage(Store store, ProgramTerminator programTerminator, QueueAdmin queueAdmin) {
    super(TypeToken.of(ApplicationSpecLocation.class));
    this.store = store;
    this.programTerminator = programTerminator;
    this.queueAdmin = queueAdmin;
  }

  @Override
  public void process(ApplicationSpecLocation appSpec) throws Exception {
    List<ProgramSpecification> deletedSpecs = store.getDeletedProgramSpecifications(appSpec.getApplicationId(),
                                                                                    appSpec.getSpecification());
    for (ProgramSpecification spec : deletedSpecs){
      //call the deleted spec
      Type type = Type.typeOfSpecification(spec);
      Id.Program programId = Id.Program.from(appSpec.getApplicationId(), spec.getName());
      programTerminator.stop(Id.Account.from(appSpec.getApplicationId().getAccountId()),
                                   programId, type);
      // drop all queues of a deleted flow
      if (Type.FLOW.equals(type)) {
        queueAdmin.dropAllForFlow(programId.getApplicationId(), programId.getId());
      }
    }
    emit(appSpec);
  }
}

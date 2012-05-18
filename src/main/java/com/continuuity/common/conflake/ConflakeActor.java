package com.continuuity.common.conflake;

import akka.actor.UntypedActor;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 *
 */
class ConflakeActor extends UntypedActor {
  private static final Logger Log = LoggerFactory.getLogger(ConflakeActor.class);
  private Conflake conflake;

  public ConflakeActor(final Conflake conflake) {
    this.conflake = conflake;
  }

  @Override
  public void onReceive(Object message) throws Exception {
    if(message instanceof ConflakeRequest) {
      ConflakeRequest request = (ConflakeRequest) message;
      List<Long> ids = new ArrayList<Long>();
      for(int i = 0; i < request.getBatchSize(); ++i) {
        ids.add(conflake.next());
      }
      ConflakeResponse response = new ConflakeResponse(ImmutableList.copyOf(ids));
      if(request.getActor() == null) {
        getSender().tell(response);
      } else {
        request.getActor().tell(response);
      }
    }
  }


}

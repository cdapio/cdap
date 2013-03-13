package com.payvment.continuuity.tools;

/**
 * Created with IntelliJ IDEA.
 * User: alex
 * Date: 3/13/13
 * Time: 2:30 PM
 * To change this template use File | Settings | File Templates.
 */
import com.continuuity.api.*;

public class ClusterFeeds implements Application {


    public static final String APP_NAME = "ClusterFeeds";
    public static final String APP_DESC = "Continuuity+Payvment:ClusterWriter";

    public ClusterFeeds() {
        // TODO Auto-generated constructor stub
    }

    @Override
    public ApplicationSpecification configure() {
        return ApplicationSpecification.Builder.with()
                .setName(Constants.APP_NAME)
                .setDescription(Constants.APP_DESCRIPTION)
				.withStreams()
					.add(new Stream(Constants.STREAM_DRIVER1))
                .noStream()
                .withDataSets()
                .add(new KeyValueTable(Constants.PERF_MAP))
                .withFlows()
                .add(new PerfFlow1())
                .withProcedures()
                .add(new PerfMetricsQuery())
                .build();
    }

}

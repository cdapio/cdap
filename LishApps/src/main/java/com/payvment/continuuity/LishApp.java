package com.payvment.continuuity;

/**
 * Created with IntelliJ IDEA.
 * User: alex
 * Date: 3/13/13
 * Time: 2:30 PM
 * To change this template use File | Settings | File Templates.
 */
import com.continuuity.api.*;
import com.continuuity.api.data.stream.Stream;
import com.payvment.continuuity.data.ClusterTable;
import com.payvment.continuuity.data.*;

public class LishApp implements Application {


    public static final String APP_NAME = "LishApp";
    public static final String APP_DESC = "Continuuity+Payvment:ClusterWriter for Lish";

    // DataSets
    public static final String ACTIVITY_FEED_TABLE = "activity_feed_table";
    public static final String CLUSTER_TABLE = "cluster_table";
    public static final String COUNTER_TABLE = "counter_table";
    public static final String PRODUCT_TABLE = "product_table";
    public static final String SORTED_COUNTER_TABLE = "sorted_counter_table";

    public static final String PRODUCT_ACTION_TABLE = "productActions";
    public static final String ALL_TIME_SCORE_TABLE = "allTimeScores";
    public static final String TOP_SCORE_TABLE = "topScores";


    /**
     * Name of the input stream carrying CSV Payvment generated clusters.
     */
    public static final String inputStream = "clusters";



    public LishApp() {
        // TODO Auto-generated constructor stub
    }

    @Override
    public ApplicationSpecification configure() {
        return ApplicationSpecification.Builder.with()
                .setName(APP_NAME)
                .setDescription(APP_DESC)
                .withStreams()
                .add(new Stream(inputStream))
                .withDataSets()
                .add(new ActivityFeedTable(ACTIVITY_FEED_TABLE))
                .add(new ClusterTable(CLUSTER_TABLE))
                .add(new CounterTable(COUNTER_TABLE))
                .add(new ProductTable(PRODUCT_TABLE))
                .add(new SortedCounterTable(SORTED_COUNTER_TABLE))
                .add(new CounterTable(PRODUCT_ACTION_TABLE))
                .add(new CounterTable(ALL_TIME_SCORE_TABLE))
                .add(new SortedCounterTable(TOP_SCORE_TABLE))
                .withFlows()
                .add(new ClusterWriterFlow())
                .withProcedures()
                .add(new ClusterFeedQueryProvider())
                .build();
    }
}

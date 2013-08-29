package com.continuuity.metrics.data;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.metrics.MetricsScope;
import com.continuuity.data.engine.leveldb.KeyValue;
import com.continuuity.data.engine.leveldb.LevelDBOVCTable;
import com.continuuity.data.engine.leveldb.LevelDBOVCTableHandle;
import com.continuuity.data.operation.executor.ReadPointer;
import com.continuuity.data.operation.executor.Transaction;
import com.continuuity.data.operation.executor.omid.OmidTransactionException;
import com.continuuity.data.operation.executor.omid.TransactionOracle;
import com.continuuity.data.operation.executor.omid.TransactionResult;
import com.continuuity.data.operation.executor.omid.Undo;
import com.continuuity.data.table.OVCTableHandle;
import com.continuuity.data.table.OrderedVersionedColumnarTable;
import com.continuuity.data.table.Scanner;
import com.continuuity.metrics.MetricsConstants;
import com.continuuity.metrics.guice.MetricsAnnotation;
import com.google.common.base.Throwables;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.PrivateModule;
import com.google.inject.Scopes;
import com.google.inject.name.Names;
import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.CompressionType;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;

import java.io.File;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.fusesource.leveldbjni.JniDBFactory.factory;

/**
 *
 */
public class LevelDBScanner {
  static MetricsTableFactory tableFactory;
  static int blocksize = 1024;
  static long cachesize = 1024 * 1024 * 100;

  public static void main(String[] args) throws Exception {
    init();
    TimeSeriesTable tsTable = tableFactory.createTimeSeries(MetricsScope.REACTOR.name(), 1);
    //long end = TimeUnit.SECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    long end = 1377733530;
    long begin = end - 60;
    MetricsScanQuery scanQuery = new MetricsScanQueryBuilder()
      .setContext(null)
      .setMetric("process.events")
      .build(begin, end);

    JniDBFactory.pushMemoryPool(1024 * 1024 * 100);
    try {

    int i = 0;
    int rows;
    long start, dur;
    //while (true) {
      i++;
      MetricsScanner scanner = tsTable.scan(scanQuery);
      start = System.currentTimeMillis();
      rows = 0;
      while (scanner.hasNext()) {
        MetricsScanResult res = scanner.next();
        for (TimeValue tv : res) {
          System.out.println("context = " + res.getContext() + " metric = " + res.getMetric() +
                               " tag = " + res.getTag());
          System.out.println("time = " + tv.getTime() + " val = " + tv.getValue());
          rows++;
        }
      }
      dur = System.currentTimeMillis() - start;
      System.out.println("scan #" + i + " took " + dur + " ms, scanned " + rows + " rows");
    //}
    } finally {
      JniDBFactory.popMemoryPool();
    }
  }


  public static void init() {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(MetricsConstants.ConfigKeys.TIME_SERIES_TABLE_ROLL_TIME, "3600");

    Injector injector = Guice.createInjector(
      new ConfigModule(cConf),
      new PrivateModule() {

        @Override
        protected void configure() {
          try {
            bind(TransactionOracle.class).to(NoopTransactionOracle.class);

            bindConstant()
              .annotatedWith(Names.named("LevelDBOVCTableHandleBasePath"))
              .to("singlenode/data");
            bindConstant()
              .annotatedWith(Names.named("LevelDBOVCTableHandleBlockSize"))
              .to(blocksize);
            bindConstant()
              .annotatedWith(Names.named("LevelDBOVCTableHandleCacheSize"))
              .to(cachesize);

            bind(OVCTableHandle.class)
              .annotatedWith(MetricsAnnotation.class)
              .toInstance(LevelDBFilterableOVCTableHandle.getInstance());
              //.toInstance(LevelDBOVCTableHandle.getInstance());

            bind(MetricsTableFactory.class).to(DefaultMetricsTableFactory.class).in(Scopes.SINGLETON);
            expose(MetricsTableFactory.class);

          } catch (Exception e) {
            throw Throwables.propagate(e);
          }
        }
      });
    tableFactory = injector.getInstance(MetricsTableFactory.class);
  }

  private static class NoopTransactionOracle implements TransactionOracle {
    @Override
    public Transaction startTransaction(boolean trackChanges) {
      throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public void validateTransaction(Transaction tx) throws OmidTransactionException {
      throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public void addToTransaction(Transaction tx, List<Undo> undos) throws OmidTransactionException {
      throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public TransactionResult commitTransaction(Transaction tx) throws OmidTransactionException {
      throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public TransactionResult abortTransaction(Transaction tx) throws OmidTransactionException {
      throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public void removeTransaction(Transaction tx) throws OmidTransactionException {
      throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public ReadPointer getReadPointer() {
      throw new UnsupportedOperationException("Not supported");
    }
  }
}

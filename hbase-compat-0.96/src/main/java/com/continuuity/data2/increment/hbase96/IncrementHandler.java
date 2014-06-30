package com.continuuity.data2.increment.hbase96;

import com.continuuity.data2.dataset.lib.table.hbase.HBaseOcTableClient;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 *
 */
public class IncrementHandler extends BaseRegionObserver {
  private static final byte[] DELTA_MAGIC_PREFIX = new byte[] { 'X', 'D' };
  @Override
  public void preGetOp(ObserverContext<RegionCoprocessorEnvironment> e, Get get, List<Cell> results)
    throws IOException {
    super.preGetOp(e, get, results);
  }

  @Override
  public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability)
    throws IOException {
    if (put.getAttribute(HBaseOcTableClient.DELTA_WRITE) != null) {
      // incremental write
      NavigableMap<byte[], List<Cell>> newFamilyMap = new TreeMap<byte[], List<Cell>>(Bytes.BYTES_COMPARATOR);
      for (Map.Entry<byte[], List<Cell>> entry : put.getFamilyCellMap().entrySet()) {
        List<Cell> newCells = new ArrayList<Cell>(entry.getValue().size());
        for (Cell cell : entry.getValue()) {
          byte[] newValue = Bytes.add(DELTA_MAGIC_PREFIX, CellUtil.cloneValue(cell));
          newCells.add(CellUtil.createCell(CellUtil.cloneRow(cell), CellUtil.cloneFamily(cell),
                                           CellUtil.cloneQualifier(cell), cell.getTimestamp(), cell.getTypeByte(),
                                           newValue));
        }
        newFamilyMap.put(entry.getKey(), newCells);
      }
      put.setFamilyCellMap(newFamilyMap);
    }
    // put completes normally with overridden column qualifiers
  }

  @Override
  public RegionScanner postScannerOpen(ObserverContext<RegionCoprocessorEnvironment> e, Scan scan, RegionScanner s)
    throws IOException {
    return super.postScannerOpen(e, scan, s);
  }
}

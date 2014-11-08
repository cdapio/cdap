package co.cask.cdap.data2.dataset2.lib.table;

import com.google.common.base.Function;
import com.google.common.collect.Maps;

import java.util.Comparator;
import java.util.NavigableMap;

/**
 * Map Transform Utility
 */
public class MapTransformUtil {

  public static <K, VA, VB> NavigableMap<K, NavigableMap<K, VB>> transformMapValues(NavigableMap<K,
    NavigableMap<K, VA>> map, Function<VA, VB> transformer, Comparator comparator) {
    NavigableMap<K, NavigableMap<K, VB>> transformedTable = Maps.newTreeMap(comparator);
    for (NavigableMap.Entry<K, NavigableMap<K, VA>> entry : map.entrySet()) {
      transformedTable.put(entry.getKey(), Maps.transformValues(entry.getValue(), transformer));
    }
    return transformedTable;
  }
}

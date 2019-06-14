package org.apache.iotdb.db.monitor.collector;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MemTableWriteTimeCost {

  public Map<String, Map<MemTableWriteTimeCostType, long[]>> getTimeCostMaps() {
    return timeCostMaps;
  }

  private Map<String, Map<MemTableWriteTimeCostType, long[]>> timeCostMaps = new ConcurrentHashMap<>();

  public static MemTableWriteTimeCost getInstance() {
    return MemTableWriteTimeCostHolder.INSTANCE;
  }

  private static class MemTableWriteTimeCostHolder {

    private static final MemTableWriteTimeCost INSTANCE = new MemTableWriteTimeCost();
  }

  private MemTableWriteTimeCost() {

  }

  public void init() {
    if (timeCostMaps.get(Thread.currentThread().getName()) == null) {
      Map<MemTableWriteTimeCostType, long[]> map = new ConcurrentHashMap<>();
      for (MemTableWriteTimeCostType type : MemTableWriteTimeCostType.values()) {
        map.put(type, new long[2]);
      }
      timeCostMaps.put(Thread.currentThread().getName(), map);
    } else {
      timeCostMaps.get(Thread.currentThread().getName()).clear();
      for (MemTableWriteTimeCostType type : MemTableWriteTimeCostType.values()) {
        timeCostMaps.get(Thread.currentThread().getName()).put(type, new long[2]);
      }
    }
  }

  public void measure(MemTableWriteTimeCostType type, long start) {
    long elapse = System.currentTimeMillis() - start;
    long[] a = new long[2];
    // long[0] is the count, long[1] is the latency in ms
    a[0] = timeCostMaps.get(Thread.currentThread().getName()).get(type)[0] + 1;
    a[1] = timeCostMaps.get(Thread.currentThread().getName()).get(type)[1] + elapse;
    timeCostMaps.get(Thread.currentThread().getName()).put(type, a);
  }

  public enum MemTableWriteTimeCostType {
    EXPAND_ARRAY_1,
    EXPAND_ARRAY_2,
    CAPACITY_1,
    CAPACITY_2,
    WRITE_1,
    WRITE_2,
    PUT_TIMESTAMP_1,
    PUT_TIMESTAMP_2,
    PUT_TIMESTAMP_3,
  }
}


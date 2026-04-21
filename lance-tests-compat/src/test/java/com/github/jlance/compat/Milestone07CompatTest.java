package com.github.jlance.compat;

import static org.junit.jupiter.api.Assertions.*;

import com.github.jlance.format.LanceFileReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.DurationVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;

public class Milestone07CompatTest {

  private static Path findLanceFile(String name) throws Exception {
    Path base = Paths.get("..", "compat_tests", "data", "milestone_07", name, "data");
    try (var stream = Files.list(base)) {
      return stream.filter(p -> p.toString().endsWith(".lance")).findFirst().orElseThrow();
    }
  }

  @Test
  public void testTemporal() throws Exception {
    Path file = findLanceFile("test_temporal");
    try (BufferAllocator allocator = new RootAllocator();
        LanceFileReader reader = new LanceFileReader(file)) {
      try (VectorSchemaRoot root = reader.readBatch(allocator)) {
        assertEquals(5, root.getRowCount());

        // timestamp:s
        TimeStampVector tsS = (TimeStampVector) root.getVector("ts_s");
        assertFalse(tsS.isNull(0));
        assertEquals(0L, tsS.get(0));
        assertEquals(1L, tsS.get(1));
        assertEquals(2L, tsS.get(2));
        assertTrue(tsS.isNull(3));
        assertEquals(4L, tsS.get(4));

        // timestamp:ms
        TimeStampVector tsMs = (TimeStampVector) root.getVector("ts_ms");
        assertEquals(0L, tsMs.get(0));
        assertEquals(1000L, tsMs.get(1));
        assertTrue(tsMs.isNull(3));

        // timestamp:us
        TimeStampVector tsUs = (TimeStampVector) root.getVector("ts_us");
        assertEquals(0L, tsUs.get(0));
        assertEquals(1_000_000L, tsUs.get(1));
        assertTrue(tsUs.isNull(3));

        // timestamp:ns
        TimeStampVector tsNs = (TimeStampVector) root.getVector("ts_ns");
        assertEquals(0L, tsNs.get(0));
        assertEquals(1_000_000_000L, tsNs.get(1));
        assertTrue(tsNs.isNull(3));

        // date32:day
        DateDayVector date32 = (DateDayVector) root.getVector("date32");
        assertEquals(0, date32.get(0));
        assertEquals(1, date32.get(1));
        assertTrue(date32.isNull(3));

        // date64:ms
        DateMilliVector date64 = (DateMilliVector) root.getVector("date64");
        assertEquals(0L, date64.get(0));
        assertEquals(86_400_000L, date64.get(1));
        assertEquals(172_800_000L, date64.get(2));
        assertTrue(date64.isNull(3));

        // time:s
        TimeSecVector timeS = (TimeSecVector) root.getVector("time_s");
        assertEquals(0, timeS.get(0));
        assertEquals(1, timeS.get(1));
        assertTrue(timeS.isNull(3));

        // time:ms
        TimeMilliVector timeMs = (TimeMilliVector) root.getVector("time_ms");
        assertEquals(0, timeMs.get(0));
        assertEquals(1000, timeMs.get(1));
        assertTrue(timeMs.isNull(3));

        // time:us
        TimeMicroVector timeUs = (TimeMicroVector) root.getVector("time_us");
        assertEquals(0L, timeUs.get(0));
        assertEquals(1_000_000L, timeUs.get(1));
        assertTrue(timeUs.isNull(3));

        // time:ns
        TimeNanoVector timeNs = (TimeNanoVector) root.getVector("time_ns");
        assertEquals(0L, timeNs.get(0));
        assertEquals(1_000_000_000L, timeNs.get(1));
        assertTrue(timeNs.isNull(3));

        // duration:s
        DurationVector durS = (DurationVector) root.getVector("duration_s");
        assertEquals(0L, DurationVector.get(durS.getDataBuffer(), 0));
        assertEquals(1L, DurationVector.get(durS.getDataBuffer(), 1));
        assertTrue(durS.isNull(3));

        // duration:ms
        DurationVector durMs = (DurationVector) root.getVector("duration_ms");
        assertEquals(0L, DurationVector.get(durMs.getDataBuffer(), 0));
        assertEquals(1000L, DurationVector.get(durMs.getDataBuffer(), 1));
        assertTrue(durMs.isNull(3));

        // duration:us
        DurationVector durUs = (DurationVector) root.getVector("duration_us");
        assertEquals(0L, DurationVector.get(durUs.getDataBuffer(), 0));
        assertEquals(1_000_000L, DurationVector.get(durUs.getDataBuffer(), 1));
        assertTrue(durUs.isNull(3));

        // duration:ns
        DurationVector durNs = (DurationVector) root.getVector("duration_ns");
        assertEquals(0L, DurationVector.get(durNs.getDataBuffer(), 0));
        assertEquals(1_000_000_000L, DurationVector.get(durNs.getDataBuffer(), 1));
        assertTrue(durNs.isNull(3));
      }
    }
  }

  @Test
  public void testTimestampMultiPage() throws Exception {
    Path file = findLanceFile("test_timestamp_multi_page");
    try (BufferAllocator allocator = new RootAllocator();
        LanceFileReader reader = new LanceFileReader(file)) {
      try (VectorSchemaRoot root = reader.readBatch(allocator)) {
        assertEquals(100000, root.getRowCount());

        TimeStampVector ts = (TimeStampVector) root.getVector("ts_us");
        assertNotNull(ts);
        assertEquals(100000, ts.getValueCount());

        assertEquals(0L, ts.get(0));
        assertEquals(1L, ts.get(1));
        assertEquals(50000L, ts.get(50000));
        assertEquals(99999L, ts.get(99999));
      }
    }
  }
}

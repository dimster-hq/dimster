package dev.dimster.stats;

import static org.junit.Assert.assertEquals;

import java.time.Instant;

import dev.dimster.MessageType;
import dev.dimster.datagen.DatasetGenerator;
import dev.dimster.datagen.DatasetResolver;
import dev.dimster.datagen.DatasetSpec;
import dev.dimster.datagen.RecordFormat;
import dev.dimster.datagen.RowGenerator;
import org.junit.Test;

/**
 * StatsCollector must recognise connectivity probes in every record format
 * (they are counted separately and excluded from throughput), and count the
 * full encoded record as received bytes.
 */
public class StatsCollectorRecordFormatTest {

    @Test
    public void probesAreRecognisedInEachFormat() {
        for (RecordFormat format : new RecordFormat[]{RecordFormat.AVRO, RecordFormat.JSON}) {
            StatsCollector stats = new StatsCollector();
            RowGenerator rows = rows(format);

            byte[] normal = rows.next(new byte[]{MessageType.NORMAL});
            receive(stats, format, rows.next(new byte[]{MessageType.PROBE}));
            receive(stats, format, normal);

            assertEquals(format + " probes", 1, stats.getProbesReceived());
            assertEquals(format + " bytes", normal.length, stats.snapshotAndReset().bytesReceived);
        }
    }

    @Test
    public void probesAreRecognisedInRawPayloads() {
        StatsCollector stats = new StatsCollector();
        byte[] probe = new byte[24];
        probe[0] = MessageType.PROBE;
        receive(stats, RecordFormat.RAW, probe);
        receive(stats, RecordFormat.RAW, new byte[100]);
        assertEquals(1, stats.getProbesReceived());
    }

    private static void receive(StatsCollector stats, RecordFormat format, byte[] value) {
        stats.messageReceived(value, format, "topic", 0, System.currentTimeMillis(), Instant.now(), Instant.now());
    }

    private static RowGenerator rows(RecordFormat format) {
        DatasetSpec spec = new DatasetSpec();
        spec.preset = "clicks";
        spec.format = format;
        return new DatasetGenerator(DatasetResolver.resolve(spec)).newRowGenerator("producer-0", 0);
    }
}

package org.cognitor.cassandra.migration.util;

import java.util.zip.CRC32;

public class ChecksumUtil {

	public static int calculateCRC32(String script) {
        final CRC32 crc32 = new CRC32();
        crc32.update(script.getBytes());
        return (int) crc32.getValue();
    }
}

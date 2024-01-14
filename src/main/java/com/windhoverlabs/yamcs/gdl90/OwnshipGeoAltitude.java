package com.windhoverlabs.yamcs.gdl90;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

public class OwnshipGeoAltitude {

  byte FlagByte = 0x7E;
  //	First Byte
  private byte MessageID = 11;

  public int ownshipAltitude;

  public int verticalFigureOfMerit;

  public boolean verticalWarningIndicator;

  public byte[] toBytes() {

    ByteArrayOutputStream messageStream = new ByteArrayOutputStream();

    messageStream.write(FlagByte);

    messageStream.write(MessageID);

    ownshipAltitude /= 5;

    ByteBuffer altBuffer = ByteBuffer.allocate(4).putInt(ownshipAltitude);

    byte[] altBufferBytes = altBuffer.array();

    messageStream.write(altBufferBytes[2]);
    messageStream.write(altBufferBytes[3]);

    int VerticalMetrics = verticalFigureOfMerit;
    if (verticalWarningIndicator) {
      VerticalMetrics = (VerticalMetrics | (1 << 15));
    }

    ByteBuffer VerticalMetricsBuffer = ByteBuffer.allocate(4).putInt(VerticalMetrics);

    byte[] VerticalMetricsBufferBytes = VerticalMetricsBuffer.array();

    messageStream.write(VerticalMetricsBufferBytes[2]);
    messageStream.write(VerticalMetricsBufferBytes[3]);

    byte[] crcData = messageStream.toByteArray();
    int crc = CrcTable.crcCompute(crcData, 1, crcData.length - 1);
    //
    // Go through message data and escape characters as per the spec
    // ....
    //

    byte[] crcBytes = ByteBuffer.allocate(4).putInt(crc).array();
    messageStream.write(crcBytes[3]);
    messageStream.write(crcBytes[2]);

    messageStream.write(FlagByte);

    byte[] dataOut = messageStream.toByteArray();

    messageStream.toByteArray();

    return dataOut;
  }
}

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
    int crc = CrcTable.crcCompute(crcData, 0, crcData.length);
    //
    // Go through message data and escape characters as per the spec
    // ....
    //

    byte[] crcBytes = ByteBuffer.allocate(4).putInt(crc).array();
    messageStream.write(crcBytes[3]);
    messageStream.write(crcBytes[2]);

    ByteArrayOutputStream messageStreamOut = escapeBytes(messageStream.toByteArray());

    ByteBuffer bbOut = ByteBuffer.allocate(messageStreamOut.toByteArray().length + 2).put(FlagByte);

    bbOut.put(messageStreamOut.toByteArray());

    bbOut.put(FlagByte);

    byte[] dataOut = bbOut.array();

    messageStream.toByteArray();

    return dataOut;
  }

  public ByteArrayOutputStream escapeBytes(byte[] bb) {
    ByteArrayOutputStream newBB = new ByteArrayOutputStream();
    for (int i = 0; i < bb.length; i++) {
      if (bb[i] == 0x7d
          || bb[i] == 0x7e) // XOR with 0x20 when found flag or control-escape character
      {
        newBB.write((byte) 0x7d);
        int temp = bb[i];
        int xored = temp ^ 0x20;
        newBB.write((byte) xored);
      } else {
        newBB.write(bb[i]); // Copy unchanged bytes
      }
    }

    return newBB;
  }
}

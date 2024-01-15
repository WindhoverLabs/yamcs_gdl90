package com.windhoverlabs.yamcs.gdl90;

import java.nio.ByteBuffer;

public class GDL90Heartbeat {
  byte FlagByte = 0x7E;

  //  According to https://www.foreflight.com/connect/spec/ Everything is ignore except for
  // GPSPosValid
  // Payload

  /**********/
  //	First Byte
  private byte MessageID = 0x00;
  //	Second Byte
  public boolean GPSPosValid, MaintRquired, INDENT, AddrType, GPSBattLow, RATCS, UATInitialized;

  // Third Byte
  public boolean TimeStampStatus, CSARequested, CSANotAvailable, UTC_OK;

  // Fourth and Fifth bytes
  short TimeStamp;
  //	Sixth and Seventh bytes
  short MesssageCounts;

  byte[] testSample = {
    0x7e,
    0x00,
    (byte) 0x81,
    0x01,
    (byte) 0xad,
    (byte) 0xa9,
    (byte) 0x00,
    (byte) 0x00,
    (byte) 0x5d,
    (byte) 0xd3,
    0x7e
  };

  public byte[] toBytes() {
    byte[] data = new byte[11];
    byte secondByte = 0;
    if (GPSPosValid) {
      secondByte = (byte) (secondByte | (byte) (1 << 7));
    }

    if (UATInitialized) {
      secondByte = (byte) (secondByte | (byte) (1 << 0));
    }
    data[1] = MessageID;
    data[2] = secondByte;

    byte thirdByte = 0;

    //    Not documented on ForeFlight docs, but this HAS to be set in order for it to display GPS
    // altitude correctly
    if (UTC_OK) {
      thirdByte = (byte) (thirdByte | (byte) (1 << 0));
    }

    data[3] = thirdByte;

    //    TODO:Use for unit tests
    //    0x00 0x81 0x41 0xDB 0xD0 0x08 0x02

    //    data[2] = (byte) 0x81;
    //    data[3] = (byte) 0x41;
    //    data[4] = (byte) 0xDB;
    //    data[5] = (byte) 0xD0;
    //    data[6] = (byte) 0x08;
    //    data[7] = (byte) 0x02;

    int heartBeatCrc = 0;

    heartBeatCrc = CrcTable.crcCompute(data, 1, 7);

    data[0] = FlagByte;

    byte[] bytes = ByteBuffer.allocate(4).putInt(heartBeatCrc).array();

    data[8] = bytes[3];
    data[9] = bytes[2];
    data[10] = FlagByte;

    return data;
  }
}

package com.windhoverlabs.yamcs.gdl90;

public class GDL90Heartbeat {
  byte FlagByte = 0x7E;

  // Payload
  /**********/
  //	First Byte
  private byte MessageID = 0x00;
  //	Second Byte
  public boolean GPSPosValid, MaintRquired, INDENT, AddrType, GPSBattLow, RATCS, UATInitialized;

  // Third Byte
  public boolean TimeStampStatus, CSARequested, CSANotAvailable, UTCOK;

  // Fourth and Fifth bytes
  short TimeStamp;
  //	Sixth and Seventh bytes
  short MesssageCounts;

  /**********/

  public byte[] toBytes() {
    byte[] data = new byte[11];
    //    int GPSPosValidValue = 0x80;
    //    GPSPosValid = (byte) GPSPosValidValue;

    byte GPSPosValidByte = 1;
    if (GPSPosValid) {
      GPSPosValidByte |= (byte) (1 << 6);
    }
    data[1] = MessageID;
    //    data[2] = GPSPosValidByte;

    //    CrcCciitCalculator crc = new CrcCciitCalculator();
    //    int heartBeatCrc = crc.compute(data, 1, 7);
    //
    //    //    The offset for the checksum will have to be calculated based on payload size which
    // can be
    //    // vary if FlagByte needs to be escaped
    //    data = ByteArrayUtils.encodeUnsignedShort(heartBeatCrc, data, 8);

    data[0] = FlagByte;
    data[10] = FlagByte;

    return data;
  }
}

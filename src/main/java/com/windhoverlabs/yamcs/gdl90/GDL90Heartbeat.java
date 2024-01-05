package com.windhoverlabs.yamcs.gdl90;

public class GDL90Heartbeat {
  //	First Byte
  public short MessageID;
  //	Second Byte
  public boolean GPSPosValid, MaintRquired, INDENT, AddrType, GPSBattLow, RATCS, UATInitialized;

  // Third Byte
  public boolean TimeStampStatus, CSARequested, CSANotAvailable, UTCOK;

  // Fourth and Fifth bytes
  short TimeStamp;
  //	Sixth and Seventh bytes
  short MesssageCounts;

  public byte[] toBytes() {
    byte[] data = new byte[32];

    return data;
  }
}

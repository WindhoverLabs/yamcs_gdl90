package com.windhoverlabs.yamcs.gdl90;

public class OwnshipReport {
  //	First Byte
  private byte MessageID = 10;

  //	  boolean

  public byte[] toBytes() {
    byte[] data = new byte[28];
    //    int GPSPosValidValue = 0x80;
    //    GPSPosValid = (byte) GPSPosValidValue;

    //		    byte GPSPosValidByte = 0;
    //		    if (GPSPosValid) {
    //		      GPSPosValidByte |= (byte) (1 << 6);
    //		    }
    data[0] = MessageID;
    //		    data[1] = GPSPosValidByte;
    data[4] = 100;
    return data;
  }
}

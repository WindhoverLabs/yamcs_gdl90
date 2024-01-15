package com.windhoverlabs.yamcs.gdl90;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

/**
 * As per the spec:
 * https://www.faa.gov/sites/faa.gov/files/air_traffic/technology/adsb/archival/GDL90_Public_ICD_RevA.PDF
 * Pg 18
 *
 * <p>This message is an extension of the GDL90 protocol by ForeFlight
 */
public class ForeFlightIDMessage {

  byte FlagByte = 0x7E;
  private byte MessageID = 0x65;
  private byte ForeFlightSubMessageID = 0x00;
  private byte Version = 0x01;

  public long DeviceSerialNum;
  public String DeviceName;
  public String DeviceLongName = "";

  public int CapibilitiesMask;

  public byte[] toBytes() throws Exception {

    ByteArrayOutputStream messageStream = new ByteArrayOutputStream();

    messageStream.write(MessageID);
    messageStream.write(ForeFlightSubMessageID);

    messageStream.write(Version);

    //    for (int i = 0; i < 36; i++) {
    //      messageStream.write(0x00);
    //    }

    byte[] DeviceSerialNumBytes = ByteBuffer.allocate(8).putLong(DeviceSerialNum).array();
    messageStream.write(DeviceSerialNumBytes[7]);
    messageStream.write(DeviceSerialNumBytes[6]);
    messageStream.write(DeviceSerialNumBytes[5]);
    messageStream.write(DeviceSerialNumBytes[4]);
    messageStream.write(DeviceSerialNumBytes[3]);
    messageStream.write(DeviceSerialNumBytes[2]);
    messageStream.write(DeviceSerialNumBytes[1]);
    messageStream.write(DeviceSerialNumBytes[0]);

    byte[] deviceNameBytes = this.DeviceName.getBytes();
    if (deviceNameBytes.length > 8) {
      throw new Exception("DeviceName is greater than 8 characters");
    }

    for (byte b : deviceNameBytes) {
      messageStream.write(b);
    }

    int deviceNameBytesRemainder = 8 - deviceNameBytes.length;

    for (int i = 0; i < deviceNameBytesRemainder; i++) {
      messageStream.write(0x20);
    }

    byte[] deviceLongNameBytes = this.DeviceLongName.getBytes();
    if (deviceLongNameBytes.length > 16) {
      throw new Exception("DeviceName is greater than 16 characters");
    }

    for (byte b : deviceLongNameBytes) {
      messageStream.write(b);
    }

    int deviceNameLongBytesRemainder = 16 - deviceLongNameBytes.length;

    for (int i = 0; i < deviceNameLongBytesRemainder; i++) {
      messageStream.write(0x20);
    }

    //    TODO:Set CapibilitiesMask accordingly
    byte[] CapibilitiesMaskBytes = ByteBuffer.allocate(4).putInt(CapibilitiesMask).array();
    messageStream.write(CapibilitiesMaskBytes[3]);
    messageStream.write(CapibilitiesMaskBytes[2]);
    messageStream.write(CapibilitiesMaskBytes[1]);
    messageStream.write(CapibilitiesMaskBytes[0]);

    byte[] crcData = messageStream.toByteArray();
    int crc = CrcTable.crcCompute(crcData, 0, crcData.length);
    //
    // Go through message data and escape characters as per the spec
    // ....
    //

    byte[] crcBytes = ByteBuffer.allocate(4).putInt(crc).array();
    messageStream.write(crcBytes[3]);
    messageStream.write(crcBytes[2]);

    ByteArrayOutputStream messageStreamOut =
        OwnshipGeoAltitude.escapeBytes(messageStream.toByteArray());

    ByteBuffer bbOut = ByteBuffer.allocate(messageStreamOut.toByteArray().length + 2).put(FlagByte);

    bbOut.put(messageStreamOut.toByteArray());

    bbOut.put(FlagByte);

    byte[] dataOut = bbOut.array();

    messageStream.toByteArray();

    return dataOut;
  }

  public static int setNibble(int num, int nibble, int which) {
    int shiftNibble = nibble << (4 * which);
    int shiftMask = 0x0000000F << (4 * which);
    return (num & ~shiftMask) | shiftNibble;
  }

  public int packLatLong(double LatLon) {

    Double doubleVal = LatLon;

    int valLon = (int) (doubleVal / (180.0 / 8388608.0));

    return valLon;
  }

  public int packAltitude(int altFt) {
    //	  Double doubleVal = altFt;
    return (int) ((1000 + altFt) / 25);
  }

  public int packDegrees(float heading) {
    return Math.round((heading / 10) * 1);
  }
}

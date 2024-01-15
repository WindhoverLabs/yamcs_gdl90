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
public class AHRS {

  byte FlagByte = 0x7E;
  private byte MessageID = 0x65;
  private byte AHRSSubMessageID = 0x01;
  public int Roll;
  public int Pitch;
  public int Heading;
  public int IndicatedAirspeed;
  public int TrueAirspeed;

  public byte[] toBytes() throws Exception {

    ByteArrayOutputStream messageStream = new ByteArrayOutputStream();

    messageStream.write(MessageID);
    messageStream.write(AHRSSubMessageID);

    int packedRoll = packDegrees(Roll);

    byte[] packedRollBytes = ByteBuffer.allocate(4).putInt(packedRoll).array();
    messageStream.write(packedRollBytes[2]);
    messageStream.write(packedRollBytes[3]);

    int packedPitch = packDegrees(Pitch);

    byte[] packedPitchBytes = ByteBuffer.allocate(4).putInt(packedPitch).array();
    messageStream.write(packedPitchBytes[2]);
    messageStream.write(packedPitchBytes[3]);

    int packedHeading = packDegrees(Heading);

    byte[] packedHeadingBytes = ByteBuffer.allocate(4).putInt(packedHeading).array();
    messageStream.write(packedHeadingBytes[2]);
    messageStream.write(packedHeadingBytes[3]);

    byte[] IndicatedAirspeedBytes = ByteBuffer.allocate(4).putInt(IndicatedAirspeed).array();
    messageStream.write(IndicatedAirspeedBytes[2]);
    messageStream.write(IndicatedAirspeedBytes[3]);

    byte[] TrueAirspeedBytes = ByteBuffer.allocate(4).putInt(TrueAirspeed).array();
    messageStream.write(TrueAirspeedBytes[2]);
    messageStream.write(TrueAirspeedBytes[3]);

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
    return (int) ((1000 + altFt) / 25);
  }

  public int packDegrees(int deg) {
    return ((deg * 10));
  }
}

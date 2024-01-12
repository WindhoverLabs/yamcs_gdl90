package com.windhoverlabs.yamcs.gdl90;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

/**
 * As per the spec:
 * https://www.faa.gov/sites/faa.gov/files/air_traffic/technology/adsb/archival/GDL90_Public_ICD_RevA.PDF
 * Pg 18
 */
public class OwnshipReport {

  byte FlagByte = 0x7E;
  //	First Byte
  private byte MessageID = 10;

  //	  Traffic Alert Status (s)
  public boolean TrafficAlertStatus;

  //  Address Type (t)
  /**
   * t = 0 : ADS-B with ICAO address t = 1 : ADS-B with Self-assigned address t = 2 : TIS-B with
   * ICAO address t = 3 : TIS-B with track file ID. t = 4 : Surface Vehicle t = 5 : Ground Station
   * Beacon t = 6-15 : reserved
   */
  public int AddressType;

  //  Participant Address (aa aa aa)
  public int ParticipantAddress;

  //  ll ll ll
  public float Latitude;
  //  nn nn nn
  public float Longitude;

  //  ddd
  public float Altitude;

  // Miscellaneous indicators:
  public int Miscellaneous;

  public byte[] toBytes() {

    ByteArrayOutputStream messageStream = minimallyFunctionalMessage();

    System.out.println("toBytes5");

    byte[] dataOut = messageStream.toByteArray();

    System.out.println("Size of dataOut:" + dataOut.length);
    return dataOut;
  }

  private ByteArrayOutputStream minimallyFunctionalMessage() {
    // Minimally functional message for ForeFlight
    // TODO:Write unit test
    ByteArrayOutputStream messageStream = new ByteArrayOutputStream();
    System.out.println("toBytes1");
    byte[] data = exampleMesssage();
    messageStream.write(FlagByte);

    messageStream.write(MessageID);
    System.out.println("toBytes2");

    messageStream.write(0x00);
    messageStream.write(0xAB); // "Magic" Byte that makes ForeFlight recognize the device
    messageStream.write(0);
    messageStream.write(0);
    messageStream.write(0);
    messageStream.write(0);
    messageStream.write(0);
    messageStream.write(0);
    messageStream.write(0);
    messageStream.write(0);
    messageStream.write(0);
    messageStream.write(0);
    messageStream.write(0xA9); // "Magic" Byte that makes ForeFlight recognize the device
    messageStream.write(0);
    messageStream.write(0);
    messageStream.write(0);
    messageStream.write(0);
    messageStream.write(0);
    messageStream.write(0);
    messageStream.write(0);
    messageStream.write(0);
    messageStream.write(0);
    messageStream.write(0);
    messageStream.write(0);
    messageStream.write(0);
    messageStream.write(0);
    messageStream.write(0);

    System.out.println("toBytes3");

    // CRC

    //    TODO:Offset needs to be re-calculated for escape characters
    byte[] crcData = messageStream.toByteArray();
    int crc = CrcTable.crcCompute(crcData, 1, crcData.length - 1);

    //
    // Go through message data and escape characters as per the spec
    // ....
    //

    byte[] crcBytes = ByteBuffer.allocate(4).putInt(crc).array();
    messageStream.write(crcBytes[3]);
    messageStream.write(crcBytes[2]);

    System.out.println("toBytes4:" + messageStream.size());
    messageStream.write(FlagByte);
    return messageStream;
  }

  private byte[] exampleMesssage() {
    // TODO: Use this message for unit tests
    //	  Minimum length: message length(including Message ID) + 2 Flag Bytes+ CRC16(2 bytes)
    byte[] data = new byte[32];
    data[0] = FlagByte;

    data[1] = MessageID;
    data[2] = 0x00;
    data[3] = (byte) 0xAB;
    data[4] = 0x45;
    data[5] = 0x49;
    data[6] = 0x1F;
    data[7] = (byte) 0xEF;
    data[8] = 0x15;
    data[9] = (byte) 0xA8;
    data[10] = (byte) 0x89;

    data[11] = 0x78;
    data[12] = 0x0F;

    Miscellaneous = 0xffffffff; // 12th byte
    byte[] MiscellaneousBytes = ByteBuffer.allocate(4).putInt(Miscellaneous).array();

    data[13] = 0x09;

    data[14] = (byte) 0xA9;
    data[15] = 0x07;
    data[16] = (byte) 0xB0;
    data[17] = 0x01;
    data[18] = 0x20;
    data[19] = 0x01;
    data[20] = 0x4E;
    data[21] = 0x38;
    data[22] = 0x32;
    data[23] = 0x35;
    data[24] = 0x56;

    data[25] = 0x20;
    data[26] = 0x20;
    data[27] = 0x20;
    data[28] = 0x00;

    int crc = 0;

    //    TODO:Offset needs to be re-calculated for escape characters
    crc = CrcTable.crcCompute(data, 1, 28);

    byte[] crcBytes = ByteBuffer.allocate(4).putInt(crc).array();

    data[data.length - 3] = crcBytes[3];
    data[data.length - 2] = crcBytes[2];
    data[data.length - 1] = FlagByte;
    return data;
  }
}

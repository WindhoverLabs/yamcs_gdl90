/****************************************************************************
 *
 *   Copyright (c) 2024 Windhover Labs, L.L.C. All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in
 *    the documentation and/or other materials provided with the
 *    distribution.
 * 3. Neither the name Windhover Labs nor the names of its
 *    contributors may be used to endorse or promote products derived
 *    from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
 * FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
 * COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS
 * OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED
 * AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
 * ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 *
 *****************************************************************************/

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
  public static final byte MessageID = 10;

  //	  Traffic Alert Status (s)
  public boolean TrafficAlertStatus;

  //  Address Type (t)
  /**
   * t = 0 : ADS-B with ICAO address t = 1 : ADS-B with Self-assigned address t = 2 : TIS-B with
   * ICAO address t = 3 : TIS-B with track file ID. t = 4 : Surface Vehicle t = 5 : Ground Station
   * Beacon t = 6-15 : reserved
   */
  public int AddressType;

  //  Participant Address in base 8 (aa aa aa)
  public int ParticipantAddress;

  //  ll ll ll
  public double Latitude;
  //  nn nn nn
  public double Longitude;

  //  ddd
  public int Altitude;

  // Miscellaneous indicators:

  public boolean TrueTrackAngle;
  public boolean MagneticHeading;
  public boolean TrueHeading;
  public boolean ReportUpdated;
  public boolean ReportExtrapolated;
  public boolean OnGround;
  public boolean Airborne;
  public int Miscellaneous;
  public byte i;
  public byte a;

  public int horizontalVelocity;

  public int verticalVelocity;

  public int trackHeading;

  //  Emitter Category. Should be an enum.
  public byte ee;

  public String callSign;

  //  EMERGENCY /PRIORITY CODE
  public byte px;

  public byte[] toBytes() throws Exception {

    ByteArrayOutputStream messageStream = new ByteArrayOutputStream();

    messageStream.write(MessageID);

    byte st = 0x00;

    if (TrafficAlertStatus) {
      st = (byte) (st & 0xf0);
    }

    st = (byte) (st & AddressType);

    messageStream.write(st);

    // aaaaaa Big Endian
    byte[] ParticipantAddressBytes =
        ByteBuffer.allocate(4)
            .putInt(Integer.parseUnsignedInt(Integer.toString(ParticipantAddress), 8))
            .array();
    messageStream.write(ParticipantAddressBytes[1]);
    messageStream.write(ParticipantAddressBytes[2]);
    messageStream.write(ParticipantAddressBytes[3]);

    int packed = packLatLong(Latitude);

    String tempLat = Integer.toHexString(packed);

    // llllll Big Endian
    byte[] LatitudeBytes = ByteBuffer.allocate(4).putInt(packed).array();
    messageStream.write(LatitudeBytes[1]);
    messageStream.write(LatitudeBytes[2]);
    messageStream.write(LatitudeBytes[3]);

    packed = packLatLong(Longitude);

    // llllll Big Endian
    byte[] LongitudeBytes = ByteBuffer.allocate(4).putInt(packed).array();
    messageStream.write(LongitudeBytes[1]);
    messageStream.write(LongitudeBytes[2]);
    messageStream.write(LongitudeBytes[3]);

    int packedAltitude = packAltitude(Altitude);

    // ddd Big Endian
    byte[] AltitudeBytes = ByteBuffer.allocate(4).putInt(packedAltitude).array();

    byte dmByte = (byte) AltitudeBytes[2];

    if (TrueHeading) {
      //      dmByte = (byte) (dmByte | (1 << 0));
      dmByte = (byte) (dmByte | (1 << 1));
      dmByte = (byte) (dmByte | (1 << 3));
    }
    //    if (Airborne) {
    //      //    	TODO:Set bit accordingly
    //    }
    //
    //    dmByte = (byte) setNibble(dmByte, 0x03, 1);

    //    dmByte = 0x09;
    int dddm = packedAltitude << 20 | (dmByte);

    // hhh Big Endian
    byte[] dddmBytes = ByteBuffer.allocate(4).putInt(dddm).array();

    //        TODO:Needs to be revisited

    messageStream.write(dddmBytes[0]);
    messageStream.write(dddmBytes[3]);

    byte iaByte = 0;

    iaByte = (byte) setNibble(iaByte, a, 0);
    iaByte = (byte) setNibble(iaByte, i, 1);

    messageStream.write(iaByte);

    int c = horizontalVelocity << 20 | (verticalVelocity / 64);

    // hhh Big Endian
    byte[] cBytes = ByteBuffer.allocate(4).putInt(c).array();

    messageStream.write(cBytes[0]);
    messageStream.write(cBytes[1]);
    messageStream.write(cBytes[3]);

    int packedHeading = packHeading(trackHeading);

    // tt Big Endian
    byte[] HeadingBytes = ByteBuffer.allocate(4).putInt(packedHeading).array();
    messageStream.write(HeadingBytes[3]);

    messageStream.write(ee);
    byte[] callSignBytes = this.callSign.getBytes();
    if (callSignBytes.length > 8) {
      throw new Exception("callSign is greater than 8 characters");
    }

    for (byte b : callSignBytes) {
      messageStream.write(b);
    }

    int callSignBytesRemainder = 8 - callSignBytes.length;

    for (int i = 0; i < callSignBytesRemainder; i++) {
      messageStream.write(0x20);
    }

    messageStream.write(px);

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
        com.windhoverlabs.yamcs.gdl90.OwnshipGeoAltitude.escapeBytes(messageStream.toByteArray());

    ByteBuffer bbOut = ByteBuffer.allocate(messageStreamOut.toByteArray().length + 2).put(FlagByte);

    bbOut.put(messageStreamOut.toByteArray());

    bbOut.put(FlagByte);

    byte[] dataOut = bbOut.array();

    messageStream.toByteArray();

    return dataOut;
  }

  private ByteArrayOutputStream minimallyFunctionalMessage() {
    // Minimally functional message for ForeFlight
    // TODO:Write unit test
    ByteArrayOutputStream messageStream = new ByteArrayOutputStream();
    byte[] data = exampleMesssage();
    messageStream.write(FlagByte);

    messageStream.write(MessageID);
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

    messageStream.write(FlagByte);
    return messageStream;
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

  public int packHeading(float heading) {
    return Math.round((heading / 360) * 256);
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

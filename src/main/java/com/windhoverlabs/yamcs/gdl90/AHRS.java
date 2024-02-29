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

import com.windhoverlabs.yamcs.gdl90.GDL90Link.AHRS_MODE;
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
  public static final byte MessageID = 0x65;
  public static final byte AHRSSubMessageID = 0x01;
  public double Roll;
  public double Pitch;
  public double Heading;
  public int IndicatedAirspeed;
  public int TrueAirspeed;

  public AHRSHeadingType HeadingType;

  public AHRS_MODE ahrsMode;

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
    int packedHeading = packDegrees(FFB_PackForeFlightHeading((Heading)));

    byte[] packedHeadingBytes = ByteBuffer.allocate(4).putInt(packedHeading).array();

    byte iaByte = packedHeadingBytes[2];
    switch (HeadingType) {
      case TRUE_HEADING:
        iaByte = (byte) (iaByte | (0 << 7));

        break;
      case MAGNETIC:
        iaByte = (byte) (iaByte | (1 << 7));
        break;
      default:
        break;
    }
    messageStream.write(iaByte);
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

  public int packDegrees(double deg) {
    int tenth = ((int) ((deg % ((int) deg)) * 10));
    return ((int) ((deg * 10)) + tenth);
  }

  //  public int packDegrees(double deg) {
  //    return ((int) ((deg * 10)));
  //  }

  public double FFB_PackForeFlightHeading(double heading) {

    // Connvert heading of [-180, 180] to [-360,360]
    double PackedHeading = heading;
    if (heading < 0) {
      PackedHeading = 360 + heading;
    } else {
      PackedHeading = heading;
    }
    return (PackedHeading);
  }
}

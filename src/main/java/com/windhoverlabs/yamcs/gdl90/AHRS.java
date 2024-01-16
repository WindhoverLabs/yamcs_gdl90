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

  public AHRSHeading HeadingSource;

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

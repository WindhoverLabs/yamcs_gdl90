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
import java.nio.ByteOrder;

/**
 * As per the spec:
 * https://www.faa.gov/sites/faa.gov/files/air_traffic/technology/adsb/archival/GDL90_Public_ICD_RevA.PDF
 * Pg 18
 *
 * <p>This message is an extension of the GDL90 protocol by Windhover Labs
 */
public class WHL_AHRS {

  byte FlagByte = 0x7E;
  public static final byte MessageID = 0x66;
  public static final byte AHRSSubMessageID = 0x00;
  public double Roll;
  public double Pitch;
  public double Heading;

  public double Lat;
  public double Lon;
  public double Alt;

  public double Groundspeed;

  public byte[] toBytes() throws Exception {

    ByteArrayOutputStream messageStream = new ByteArrayOutputStream();

    messageStream.write(MessageID);
    messageStream.write(AHRSSubMessageID);

    byte[] packedRollBytes =
        ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putDouble(Roll).array();
    messageStream.write(packedRollBytes[0]);
    messageStream.write(packedRollBytes[1]);
    messageStream.write(packedRollBytes[2]);
    messageStream.write(packedRollBytes[3]);
    messageStream.write(packedRollBytes[4]);
    messageStream.write(packedRollBytes[5]);
    messageStream.write(packedRollBytes[6]);
    messageStream.write(packedRollBytes[7]);

    byte[] packedPitchBytes =
        ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putDouble(Pitch).array();
    messageStream.write(packedPitchBytes[0]);
    messageStream.write(packedPitchBytes[1]);
    messageStream.write(packedPitchBytes[2]);
    messageStream.write(packedPitchBytes[3]);
    messageStream.write(packedPitchBytes[4]);
    messageStream.write(packedPitchBytes[5]);
    messageStream.write(packedPitchBytes[6]);
    messageStream.write(packedPitchBytes[7]);

    //    Heading = -80;
    double packedHeading = WHL_PackWHLHeading(Heading);

    //    0x01C2 = 450
    //    int packedHeading = packDegrees(45);

    byte[] packedHeadingBytes =
        ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putDouble(packedHeading).array();
    messageStream.write(packedHeadingBytes[0]);
    messageStream.write(packedHeadingBytes[1]);
    messageStream.write(packedHeadingBytes[2]);
    messageStream.write(packedHeadingBytes[3]);
    messageStream.write(packedHeadingBytes[4]);
    messageStream.write(packedHeadingBytes[5]);
    messageStream.write(packedHeadingBytes[6]);
    messageStream.write(packedHeadingBytes[7]);

    byte[] packedLatBytes =
        ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putDouble(Lat).array();
    messageStream.write(packedLatBytes[0]);
    messageStream.write(packedLatBytes[1]);
    messageStream.write(packedLatBytes[2]);
    messageStream.write(packedLatBytes[3]);
    messageStream.write(packedLatBytes[4]);
    messageStream.write(packedLatBytes[5]);
    messageStream.write(packedLatBytes[6]);
    messageStream.write(packedLatBytes[7]);

    byte[] packedLonBytes =
        ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putDouble(Lon).array();
    messageStream.write(packedLonBytes[0]);
    messageStream.write(packedLonBytes[1]);
    messageStream.write(packedLonBytes[2]);
    messageStream.write(packedLonBytes[3]);
    messageStream.write(packedLonBytes[4]);
    messageStream.write(packedLonBytes[5]);
    messageStream.write(packedLonBytes[6]);
    messageStream.write(packedLonBytes[7]);

    byte[] packedAltBytes =
        ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putDouble(Alt).array();
    messageStream.write(packedAltBytes[0]);
    messageStream.write(packedAltBytes[1]);
    messageStream.write(packedAltBytes[2]);
    messageStream.write(packedAltBytes[3]);
    messageStream.write(packedAltBytes[4]);
    messageStream.write(packedAltBytes[5]);
    messageStream.write(packedAltBytes[6]);
    messageStream.write(packedAltBytes[7]);

    byte[] GroundspeedAltBytes =
        ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putDouble(Groundspeed).array();
    messageStream.write(GroundspeedAltBytes[0]);
    messageStream.write(GroundspeedAltBytes[1]);
    messageStream.write(GroundspeedAltBytes[2]);
    messageStream.write(GroundspeedAltBytes[3]);
    messageStream.write(GroundspeedAltBytes[4]);
    messageStream.write(GroundspeedAltBytes[5]);
    messageStream.write(GroundspeedAltBytes[6]);
    messageStream.write(GroundspeedAltBytes[7]);

    byte[] crcData = messageStream.toByteArray();
    int crc = CrcTable.crcCompute(crcData, 0, crcData.length);
    //
    // Go through message data and escape characters as per the spec
    // ....
    //

    byte[] crcBytes = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putInt(crc).array();
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

  public double WHL_PackWHLHeading(double heading) {

    // Connvert heading of [-180, 180] to [0,360]
    double PackedHeading = heading;
    if (heading < 0) {
      PackedHeading = 360 + heading;
    } else {
      PackedHeading = heading;
    }
    return (PackedHeading);
  }
}

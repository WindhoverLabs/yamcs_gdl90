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

public class OwnshipGeoAltitude {

  byte FlagByte = 0x7E;
  //	First Byte
  private byte MessageID = 11;

  public int ownshipAltitude;

  public int verticalFigureOfMerit;

  public boolean verticalWarningIndicator;

  public byte[] toBytes() {

    ByteArrayOutputStream messageStream = new ByteArrayOutputStream();

    messageStream.write(MessageID);

    ownshipAltitude /= 5;

    ByteBuffer altBuffer = ByteBuffer.allocate(4).putInt(ownshipAltitude);

    byte[] altBufferBytes = altBuffer.array();

    messageStream.write(altBufferBytes[2]);
    messageStream.write(altBufferBytes[3]);

    int VerticalMetrics = verticalFigureOfMerit;
    if (verticalWarningIndicator) {
      VerticalMetrics = (VerticalMetrics | (1 << 15));
    }

    ByteBuffer VerticalMetricsBuffer = ByteBuffer.allocate(4).putInt(VerticalMetrics);

    byte[] VerticalMetricsBufferBytes = VerticalMetricsBuffer.array();

    messageStream.write(VerticalMetricsBufferBytes[2]);
    messageStream.write(VerticalMetricsBufferBytes[3]);

    byte[] crcData = messageStream.toByteArray();
    int crc = CrcTable.crcCompute(crcData, 0, crcData.length);
    //
    // Go through message data and escape characters as per the spec
    // ....
    //

    byte[] crcBytes = ByteBuffer.allocate(4).putInt(crc).array();
    messageStream.write(crcBytes[3]);
    messageStream.write(crcBytes[2]);

    ByteArrayOutputStream messageStreamOut = escapeBytes(messageStream.toByteArray());

    ByteBuffer bbOut = ByteBuffer.allocate(messageStreamOut.toByteArray().length + 2).put(FlagByte);

    bbOut.put(messageStreamOut.toByteArray());

    bbOut.put(FlagByte);

    byte[] dataOut = bbOut.array();

    messageStream.toByteArray();

    return dataOut;
  }

  public static ByteArrayOutputStream escapeBytes(byte[] bb) {
    ByteArrayOutputStream newBB = new ByteArrayOutputStream();
    for (int i = 0; i < bb.length; i++) {
      if (bb[i] == 0x7d
          || bb[i] == 0x7e) // XOR with 0x20 when found flag or control-escape character
      {
        newBB.write((byte) 0x7d);
        int temp = bb[i];
        int xored = temp ^ 0x20;
        newBB.write((byte) xored);
      } else {
        newBB.write(bb[i]); // Copy unchanged bytes
      }
    }

    return newBB;
  }
}

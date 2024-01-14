/****************************************************************************
 *
 *   Copyright (c) 2022 Windhover Labs, L.L.C. All rights reserved.
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

import com.google.common.io.BaseEncoding;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.yamcs.Spec;
import org.yamcs.TmPacket;
import org.yamcs.YConfiguration;
import org.yamcs.parameter.ParameterValue;
import org.yamcs.parameter.SystemParametersProducer;
import org.yamcs.parameter.SystemParametersService;
import org.yamcs.protobuf.Yamcs;
import org.yamcs.tctm.AbstractTmDataLink;
import org.yamcs.tctm.Link.Status;
import org.yamcs.tctm.PacketInputStream;
import org.yamcs.xtce.Parameter;
import org.yamcs.yarch.FileSystemBucket;
import org.yamcs.yarch.Stream;
import org.yamcs.yarch.StreamSubscriber;
import org.yamcs.yarch.Tuple;
import org.yamcs.yarch.protobuf.Db.Event;

public class GDL90Link extends AbstractTmDataLink
    implements Runnable, StreamSubscriber, SystemParametersProducer {
  /* Configuration Defaults */
  static long POLLING_PERIOD_DEFAULT = 1000;
  static int INITIAL_DELAY_DEFAULT = -1;
  static boolean IGNORE_INITIAL_DEFAULT = true;
  static boolean CLEAR_BUCKETS_AT_STARTUP_DEFAULT = false;
  static boolean DELETE_FILE_AFTER_PROCESSING_DEFAULT = false;

  private boolean outOfSync = false;

  private Parameter outOfSyncParam;
  private Parameter streamEventCountParam;
  private Parameter logEventCountParam;
  private int streamEventCount;
  private int logEventCount;

  /* Configuration Parameters */
  protected long initialDelay;
  protected long period;
  protected boolean ignoreInitial;
  protected boolean clearBucketsAtStartup;
  protected boolean deleteFileAfterProcessing;
  protected int EVS_FILE_HDR_SUBTYPE;
  protected int DS_TOTAL_FNAME_BUFSIZE;

  /* Internal member attributes. */
  protected List<FileSystemBucket> buckets;
  protected YConfiguration packetInputStreamArgs;
  protected PacketInputStream packetInputStream;
  protected WatchService watcher;
  protected List<WatchKey> watchKeys;
  protected Thread thread;

  private String eventStreamName;

  static final String RECTIME_CNAME = "rectime";
  static final String DATA_EVENT_CNAME = "data";

  private DatagramSocket foreFlightSocket;
  private DatagramSocket GDL90Socket;

  //  int MAX_LENGTH = 32 * 1024;
  int MAX_LENGTH = 32;
  DatagramPacket foreFlightdatagram = new DatagramPacket(new byte[MAX_LENGTH], MAX_LENGTH);

  DatagramPacket GDL90Datagram;

  /* Constants */
  static final byte[] CFE_FS_FILE_CONTENT_ID_BYTE =
      BaseEncoding.base16().lowerCase().decode("63464531".toLowerCase());

  private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

  private boolean foreFlighConnected = false;

  String GDL90Hostname;

  Integer appNameMax;
  Integer eventMsgMax;

  @Override
  public Spec getSpec() {
    Spec spec = new Spec();

    return spec;
  }

  @Override
  public void init(String yamcsInstance, String serviceName, YConfiguration config) {
    super.init(yamcsInstance, serviceName, config);

    try {
      foreFlightSocket = new DatagramSocket(63093);

      // TODO: Port will eventually be read from brodacasted JSON on 63093 from ForeFlight
      try {
        GDL90Socket = new DatagramSocket();
        GDL90Datagram =
            new DatagramPacket(
                new byte[MAX_LENGTH],
                MAX_LENGTH,
                InetAddress.getByName(config.getString("gdl90_host")),
                config.getInt("gdl90_port"));
      } catch (UnknownHostException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    } catch (SocketException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    scheduler.scheduleAtFixedRate(
        () -> {
          if (isRunningAndEnabled()) {
            try {
              System.out.println("Triggered");
              sendHeartbeat();
              sendOwnshipReport();
              OwnshipGeoAltitude();
              //              sendOwnshipReport();

            } catch (IOException e) {
              // TODO Auto-generated catch block
              e.printStackTrace();
            }
          }
        },
        100,
        1000,
        TimeUnit.MILLISECONDS);
  }

  @Override
  public void doDisable() {
    /* If the thread is created, interrupt it. */
    if (thread != null) {
      thread.interrupt();
    }
  }

  @Override
  public void doEnable() {
    /* Create and start the new thread. */
    thread = new Thread(this);
    thread.setName(this.getClass().getSimpleName() + "-" + linkName);
    thread.start();
  }

  @Override
  public String getDetailedStatus() {
    if (isDisabled()) {
      return String.format("DISABLED");
    } else {
      return String.format("OK, received %d packets", packetCount.get());
    }
  }

  @Override
  protected Status connectionStatus() {
    return Status.OK;
  }

  @Override
  protected void doStart() {
    if (!isDisabled()) {
      doEnable();
    }
    notifyStarted();
  }

  @Override
  protected void doStop() {
    if (thread != null) {
      thread.interrupt();
    }

    notifyStopped();
  }

  @Override
  public void run() {
    /* Delay the start, if configured to do so. */
    if (initialDelay > 0) {
      try {
        Thread.sleep(initialDelay);
        initialDelay = -1;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
    }

    /* Enter our main loop */
    while (isRunningAndEnabled()) {

      //      try {
      ////    	  TODO:Finish implementation
      ////        if (!foreFlighConnected) {
      ////          foreFlightSocket.receive(foreFlightdatagram);
      ////          //          Gson obj = new Gson();
      ////
      ////          Gson gson = new GsonBuilder().setLenient().create();
      ////          String foreFlightJSON = new String(foreFlightdatagram.getData());
      ////          org.json.JSONObject j = new JSONObject(foreFlightJSON);
      ////
      ////          System.out.println("address:" +
      // foreFlightdatagram.getAddress().getHostAddress());
      ////          //          System.out.println("String JSON:" + foreFlightJSON);
      ////          ForeFlightBroadcast ffJSON = gson.fromJson(foreFlightJSON,
      // ForeFlightBroadcast.class);
      ////          foreFlighConnected = true;
      ////        }
      //      } catch (IOException e) {
      //        // TODO Auto-generated catch block
      //        e.printStackTrace();
      //      }
      //      }

      //      try {
      //        //    	  At the moment ForeFlight only looks at the GPSPosValid flag and ignores
      // everything
      //        // else in the heartbeat message
      ////        sendHeartbeat();
      //
      //      } catch (IOException e) {
      //        // TODO Auto-generated catch block
      //        e.printStackTrace();
      //      }
    }
  }

  private void sendHeartbeat() throws IOException {
    GDL90Heartbeat beat = new GDL90Heartbeat();
    beat.GPSPosValid = true;
    beat.UATInitialized = true;
    beat.UTC_OK = true;
    GDL90Datagram.setData(beat.toBytes());
    System.out.println(
        "Sending Heartbeat:"
            + org.yamcs.utils.StringConverter.arrayToHexString(GDL90Datagram.getData(), true));
    GDL90Socket.send(GDL90Datagram);
  }

  private void sendOwnshipReport() throws IOException {

    com.windhoverlabs.yamcs.gdl90.OwnshipReport ownership =
        new com.windhoverlabs.yamcs.gdl90.OwnshipReport();

    /**
     * Report Data: No Traffic Alert ICAO ADS-B Address (octal): 52642511 8 Latitude: 44.90708
     * (North) Longitude: -122.99488 (West) Altitude: 5,000 feet (pressure altitude) Airborne with
     * True Track HPL = 20 meters, HFOM = 25 meters (NIC = 10, NACp = 9) Horizontal velocity: 123
     * knots at 45 degrees (True Track) Vertical velocity: 64 FPM climb Emergency/Priority Code:
     * none Emitter Category: Light Tail Number: N825
     */
    ownership.TrafficAlertStatus = false;
    ownership.AddressType = 0;
    ownership.ParticipantAddress = 52642511; // base 8
    ownership.Latitude = 44.90708;
    ownership.Longitude = -122.99488;

    ownership.Altitude = 5000;
    ownership.TrueTrackAngle = true;
    ownership.Airborne = true;

    ownership.i = 10;
    ownership.a = 9;

    ownership.horizontalVelocity = 630; // Knots

    ownership.verticalVelocity = 64; // FPM

    ownership.trackHeading = 45; // Degrees

    ownership.ee = 1; // Should be an enum

    ownership.callSign = "N825V";
    try {
      GDL90Datagram.setData(ownership.toBytes());
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    ownership.px = 0;

    System.out.println(
        "Sending OwnshipReport:"
            + org.yamcs.utils.StringConverter.arrayToHexString(GDL90Datagram.getData(), true));
    GDL90Socket.send(GDL90Datagram);
  }

  private void OwnshipGeoAltitude() throws IOException {

    com.windhoverlabs.yamcs.gdl90.OwnshipGeoAltitude geoAlt =
        new com.windhoverlabs.yamcs.gdl90.OwnshipGeoAltitude();

    geoAlt.ownshipAltitude = 3000;
    geoAlt.verticalFigureOfMerit = 50;
    geoAlt.verticalWarningIndicator = false;
    try {
      GDL90Datagram.setData(geoAlt.toBytes());
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    System.out.println(
        "Sending OwnshipGeoAltitude:"
            + org.yamcs.utils.StringConverter.arrayToHexString(GDL90Datagram.getData(), true));
    GDL90Socket.send(GDL90Datagram);
  }

  public TmPacket getNextPacket() {
    TmPacket pwt = null;
    while (isRunningAndEnabled()) {}

    return pwt;
  }

  @Override
  public void onTuple(Stream stream, Tuple tuple) {
    if (isRunningAndEnabled()) {
      Event event = (Event) tuple.getColumn("body");
      updateStats(event.getMessage().length());
      streamEventCount++;
    }
  }

  @Override
  public void setupSystemParameters(SystemParametersService sysParamCollector) {
    super.setupSystemParameters(sysParamCollector);
    outOfSyncParam =
        sysParamCollector.createSystemParameter(
            linkName + "/outOfSync",
            Yamcs.Value.Type.BOOLEAN,
            "Are the downlinked events not in sync wtih the ones from the log?");
    streamEventCountParam =
        sysParamCollector.createSystemParameter(
            linkName + "/streamEventCountParam",
            Yamcs.Value.Type.UINT64,
            "Event count in realtime event stream");
    logEventCountParam =
        sysParamCollector.createSystemParameter(
            linkName + "/logEventCountParam",
            Yamcs.Value.Type.UINT64,
            "Event count from log files");
  }

  @Override
  public List<ParameterValue> getSystemParameters() {
    long time = getCurrentTime();

    ArrayList<ParameterValue> list = new ArrayList<>();
    try {
      collectSystemParameters(time, list);
    } catch (Exception e) {
      log.error("Exception caught when collecting link system parameters", e);
    }
    return list;
  }

  @Override
  protected void collectSystemParameters(long time, List<ParameterValue> list) {
    super.collectSystemParameters(time, list);
    list.add(SystemParametersService.getPV(outOfSyncParam, time, outOfSync));
    list.add(SystemParametersService.getPV(streamEventCountParam, time, streamEventCount));
    list.add(SystemParametersService.getPV(logEventCountParam, time, logEventCount));
  }
}

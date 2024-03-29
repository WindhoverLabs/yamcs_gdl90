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

import com.google.gson.Gson;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.yamcs.ConfigurationException;
import org.yamcs.Processor;
import org.yamcs.Spec;
import org.yamcs.YConfiguration;
import org.yamcs.client.ClientException;
import org.yamcs.client.ConnectionListener;
import org.yamcs.client.ParameterSubscription;
import org.yamcs.client.YamcsClient;
import org.yamcs.parameter.ParameterValue;
import org.yamcs.parameter.SystemParametersProducer;
import org.yamcs.parameter.SystemParametersService;
import org.yamcs.protobuf.SubscribeParametersRequest;
import org.yamcs.protobuf.SubscribeParametersRequest.Action;
import org.yamcs.protobuf.Yamcs;
import org.yamcs.protobuf.Yamcs.NamedObjectId;
import org.yamcs.tctm.AbstractLink;
import org.yamcs.tctm.PacketInputStream;
import org.yamcs.xtce.Parameter;
import org.yamcs.yarch.ColumnDefinition;
import org.yamcs.yarch.DataType;
import org.yamcs.yarch.FileSystemBucket;
import org.yamcs.yarch.Stream;
import org.yamcs.yarch.Tuple;
import org.yamcs.yarch.TupleDefinition;
import org.yamcs.yarch.YarchDatabase;
import org.yamcs.yarch.YarchDatabaseInstance;

public class GDL90Link extends AbstractLink
    implements Runnable,
        SystemParametersProducer,
        ParameterSubscription.Listener,
        ConnectionListener {

  class GDL90Device {
    String host;
    String port;
    DatagramPacket datagram;
    boolean alive;
    int keepAliveSeconds;
    Instant lastBroadcastTime;

    public GDL90Device(
        String newHost,
        String newPort,
        DatagramPacket newDatagram,
        boolean newAlive,
        int newKeepAliveSeconds,
        Instant newLastBradcastTime) {
      this.host = newHost;
      this.port = newPort;
      this.datagram = newDatagram;
      this.alive = newAlive;
      this.keepAliveSeconds = newKeepAliveSeconds;
      this.lastBroadcastTime = newLastBradcastTime;
    }

    public String toString() {
      return "Host:" + this.host + ", Port:" + this.port + ", Sending:" + this.alive;
    }
  }
  /* Configuration Defaults */
  static long POLLING_PERIOD_DEFAULT = 1000;
  static int INITIAL_DELAY_DEFAULT = -1;
  static boolean IGNORE_INITIAL_DEFAULT = true;
  static boolean CLEAR_BUCKETS_AT_STARTUP_DEFAULT = false;
  static boolean DELETE_FILE_AFTER_PROCESSING_DEFAULT = false;
  private static TupleDefinition gftdef;

  private boolean outOfSync = false;

  private Parameter outOfSyncParam;
  private Parameter streamEventCountParam;
  private Parameter logEventCountParam;
  private int streamEventCount;
  private int logEventCount;

  /* Configuration Parameters */
  protected long initialDelay;
  protected long period;

  /* Internal member attributes. */
  protected List<FileSystemBucket> buckets;
  protected YConfiguration packetInputStreamArgs;
  protected PacketInputStream packetInputStream;
  protected WatchService watcher;
  protected List<WatchKey> watchKeys;
  protected Thread thread;

  private String eventStreamName;

  private DatagramSocket foreFlightSocket;
  private DatagramSocket GDL90Socket;

  private ParameterSubscription subscription;

  private ConcurrentHashMap<String, org.yamcs.protobuf.Pvalue.ParameterValue> paramsToSend =
      new ConcurrentHashMap<String, org.yamcs.protobuf.Pvalue.ParameterValue>();

  private String yamcsHost;
  private int yamcsPort;

  private String processorName;
  private Processor processor;

  private YamcsClient yclient;

  int MAX_LENGTH = 1024;
  DatagramPacket foreFlightdatagram = new DatagramPacket(new byte[MAX_LENGTH], MAX_LENGTH);

  private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

  private ConcurrentHashMap<String, String> pvMap;
  private int heartBeatCount = 0;
  private int ownShipReportCount = 0;
  private int ownshipGeoAltitudeCount = 0;
  private int foreFlightIDCount = 0;
  private int AHRSCount = 0;
  String GDL90Hostname;

  Integer appNameMax;
  Integer eventMsgMax;
  private String heartbeatStreamName;
  private Stream heartbeatStream;

  private String ownShipReportStreamName;
  private Stream ownShipReportStream;

  private String ownShipGeoAltitudeStreamName;
  private Stream ownShipGeoAltitudeStream;

  private String AHRSStreamName;
  private Stream AHRSStream;

  private String ForeFlightIDStreamName;
  private Stream ForeFlightIDStream;

  static final String RECTIME_CNAME = "rectime";
  static final String MSG_NAME_CNAME = "MSG_NAME_CNAME";
  static final String DATA_CNAME = "data";

  //  Make keepAliveConfig configurable, maybe...

  public int keepAliveConfig = 30; // Seconds

  ConcurrentHashMap<String, GDL90Device> gdl90Devices =
      new ConcurrentHashMap<String, GDL90Device>();

  static {
    gftdef = new TupleDefinition();
    gftdef.addColumn(new ColumnDefinition(RECTIME_CNAME, DataType.TIMESTAMP));

    gftdef.addColumn(new ColumnDefinition(MSG_NAME_CNAME, DataType.STRING));
    gftdef.addColumn(new ColumnDefinition(DATA_CNAME, DataType.BINARY));
  }

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
      GDL90Socket = new DatagramSocket();
      if (this.getConfig().containsKey("gdl90Devices")) {
        List<Map<String, Object>> devices = this.getConfig().getList("gdl90Devices");
        for (Map<String, Object> d : devices) {
          gdl90Devices.put(
              d.get("gdl90_host").toString(),
              new GDL90Device(
                  d.get("gdl90_host").toString(),
                  d.get("gdl90_port").toString(),
                  new DatagramPacket(
                      new byte[MAX_LENGTH],
                      MAX_LENGTH,
                      InetAddress.getByName(d.get("gdl90_host").toString()),
                      Integer.parseInt(d.get("gdl90_port").toString())),
                  true,
                  keepAliveConfig,
                  Instant.now()));
        }
      }

    } catch (SocketException | NumberFormatException | UnknownHostException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    scheduler.scheduleAtFixedRate(
        () -> {
          if (isRunningAndEnabled()) {
            try {
              sendHeartbeat();
              sendOwnshipReport();
              sendOwnshipGeoAltitude();
              sendForeFlightID();

            } catch (IOException e) {
              // TODO Auto-generated catch block
              e.printStackTrace();
            }
          }
        },
        100,
        1000,
        TimeUnit.MILLISECONDS);

    scheduler.scheduleAtFixedRate(
        () -> {
          if (isRunningAndEnabled()) {
            try {
              AHRSMessage();
            } catch (IOException e) {
              // TODO Auto-generated catch block
              e.printStackTrace();
            }
          }
        },
        100,
        200,
        TimeUnit.MILLISECONDS);

    scheduler.scheduleAtFixedRate(
        () -> {
          if (isRunningAndEnabled()) {
            for (GDL90Device d : gdl90Devices.values()) {
              Instant now = Instant.now();

              Instant end = d.lastBroadcastTime;
              Duration timeElapsed = Duration.between(end, now);

              if (timeElapsed.toMillis() / 1000 > d.keepAliveSeconds) {
                gdl90Devices.remove(d.host);
              }
            }
          }
        },
        1,
        10,
        TimeUnit.SECONDS);

    yamcsHost = this.getConfig().getString("yamcsHost", "http://localhost");
    yamcsPort = this.getConfig().getInt("yamcsPort", 8090);

    pvMap = new ConcurrentHashMap<String, String>(this.config.getMap("pvMap"));

    initStreams();
  }

  private void initStreams() {
    YarchDatabaseInstance ydb = YarchDatabase.getInstance(this.yamcsInstance);

    heartbeatStreamName = this.getConfig().getString("heartbeatStream", null);

    if (heartbeatStreamName != null) {
      this.heartbeatStream = getStream(ydb, heartbeatStreamName);
    }

    ownShipReportStreamName = this.getConfig().getString("ownShipReportStreamName", null);

    if (ownShipReportStreamName != null) {
      this.ownShipReportStream = getStream(ydb, ownShipReportStreamName);
    }

    ownShipGeoAltitudeStreamName = this.getConfig().getString("ownShipGeoAltitudeStreamName", null);

    if (ownShipGeoAltitudeStreamName != null) {
      this.ownShipGeoAltitudeStream = getStream(ydb, ownShipGeoAltitudeStreamName);
    }

    ForeFlightIDStreamName = this.getConfig().getString("ForeFlightIDStreamName", null);

    if (ForeFlightIDStreamName != null) {
      this.ForeFlightIDStream = getStream(ydb, ForeFlightIDStreamName);
    }

    AHRSStreamName = this.getConfig().getString("AHRSStreamName", null);

    if (AHRSStreamName != null) {
      this.AHRSStream = getStream(ydb, AHRSStreamName);
    }
  }

  private static Stream getStream(YarchDatabaseInstance ydb, String streamName) {
    Stream stream = ydb.getStream(streamName);
    if (stream == null) {
      try {
        ydb.execute("create stream " + streamName + gftdef.getStringDefinition());
      } catch (Exception e) {
        throw new ConfigurationException(e);
      }

      stream = ydb.getStream(streamName);

    } else {
      throw new ConfigurationException("Stream " + streamName + " already exists");
    }
    return stream;
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
      return String.format(
          "OK, Sent %d heartbeats, %d OwnshipReports, %d ownShipGeoAltitude(s), %d foreFlightIDs, %d AHRS(s) ",
          heartBeatCount,
          ownShipReportCount,
          ownshipGeoAltitudeCount,
          foreFlightIDCount,
          AHRSCount);
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

    //    TODO: This is unnecessarily complicated
    yclient =
        YamcsClient.newBuilder(yamcsHost + ":" + yamcsPort)
            //            .withConnectionAttempts(config.getInt("connectionAttempts", 20))
            //            .withRetryDelay(reconnectionDelay)
            //            .withVerifyTls(config.getBoolean("verifyTls", true))
            .build();
    yclient.addConnectionListener(this);

    try {
      yclient.connectWebSocket();
    } catch (ClientException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
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

      try {
        foreFlightSocket.receive(foreFlightdatagram);
        Gson gson = new Gson();
        if (!gdl90Devices.containsKey(foreFlightdatagram.getAddress().getHostAddress())) {
          String foreFlightJSON =
              new String(foreFlightdatagram.getData(), 0, foreFlightdatagram.getLength());
          ForeFlightBroadcast ffJSON = gson.fromJson(foreFlightJSON, ForeFlightBroadcast.class);

          gdl90Devices.put(
              foreFlightdatagram.getAddress().getHostAddress(),
              new GDL90Device(
                  foreFlightdatagram.getAddress().getHostAddress(),
                  Integer.toString(ffJSON.GDL90.port),
                  new DatagramPacket(
                      new byte[MAX_LENGTH],
                      MAX_LENGTH,
                      InetAddress.getByName(foreFlightdatagram.getAddress().getHostAddress()),
                      Integer.parseInt(Integer.toString(ffJSON.GDL90.port))),
                  true,
                  keepAliveConfig,
                  Instant.now()));
        } else {
          gdl90Devices.get(foreFlightdatagram.getAddress().getHostAddress()).lastBroadcastTime =
              Instant.now();
        }
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
  }

  private synchronized void sendHeartbeat() throws IOException {
    for (GDL90Device d : gdl90Devices.values()) {
      if (d.alive) {
        GDL90Heartbeat beat = new GDL90Heartbeat();
        beat.GPSPosValid = true;
        beat.UATInitialized = true;
        beat.UTC_OK = true;
        byte[] gdlPacket = beat.toBytes();
        d.datagram.setData(gdlPacket);
        GDL90Socket.send(d.datagram);
        heartBeatCount++;
        if (this.heartbeatStream != null) {
          this.heartbeatStream.emitTuple(
              new Tuple(
                  gftdef, Arrays.asList(timeService.getMissionTime(), "Heartbeat", gdlPacket)));
        }
      }
    }
  }

  private synchronized void sendForeFlightID() throws IOException {
    for (GDL90Device d : gdl90Devices.values()) {
      if (d.alive) {

        ForeFlightIDMessage id = new ForeFlightIDMessage();

        id.DeviceSerialNum = 12;
        id.DeviceName = "Airliner";
        id.DeviceLongName = "Airliner";

        try {
          d.datagram.setData(id.toBytes());
        } catch (Exception e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
        GDL90Socket.send(d.datagram);

        if (this.ForeFlightIDStream != null) {
          this.ForeFlightIDStream.emitTuple(
              new Tuple(
                  gftdef,
                  Arrays.asList(
                      timeService.getMissionTime(), "ForeFlightID", d.datagram.getData())));
        }

        foreFlightIDCount++;
      }
    }
  }

  private synchronized void sendOwnshipReport() throws IOException {

    for (GDL90Device d : gdl90Devices.values()) {
      if (d.alive) {

        com.windhoverlabs.yamcs.gdl90.OwnshipReport ownship =
            new com.windhoverlabs.yamcs.gdl90.OwnshipReport();

        /**
         * Report Data: No Traffic Alert ICAO ADS-B Address (octal): 52642511 8 Latitude: 44.90708
         * (North) Longitude: -122.99488 (West) Altitude: 5,000 feet (pressure altitude) Airborne
         * with True Track HPL = 20 meters, HFOM = 25 meters (NIC = 10, NACp = 9) Horizontal
         * velocity: 123 knots at 45 degrees (True Track) Vertical velocity: 64 FPM climb
         * Emergency/Priority Code: none Emitter Category: Light Tail Number: N825
         */
        ownship.TrafficAlertStatus = false;
        ownship.AddressType = 0;
        //    The ParticipantAddress seems to impact the way Altitude gets displayed on ForeFlight
        ownship.ParticipantAddress = 0; // base 8
        ownship.Latitude = 44.90708;
        ownship.Longitude = -122.99488;

        org.yamcs.protobuf.Pvalue.ParameterValue pvLatitude = paramsToSend.get("Latitude");

        if (pvLatitude != null) {
          switch (pvLatitude.getEngValue().getType()) {
            case AGGREGATE:
              break;
            case ARRAY:
              break;
            case BINARY:
              break;
            case BOOLEAN:
              break;
            case DOUBLE:
              ownship.Latitude = pvLatitude.getEngValue().getDoubleValue();
              break;
            case ENUMERATED:
              break;
            case FLOAT:
              ownship.Latitude = pvLatitude.getEngValue().getFloatValue();
              break;
            case NONE:
              break;
            case SINT32:
              break;
            case SINT64:
              break;
            case STRING:
              break;
            case TIMESTAMP:
              break;
            case UINT32:
              break;
            case UINT64:
              break;
            default:
              break;
          }
        }

        org.yamcs.protobuf.Pvalue.ParameterValue pvLongitude = paramsToSend.get("Longitude");

        if (pvLongitude != null) {
          switch (pvLongitude.getEngValue().getType()) {
            case AGGREGATE:
              break;
            case ARRAY:
              break;
            case BINARY:
              break;
            case BOOLEAN:
              break;
            case DOUBLE:
              ownship.Longitude = pvLongitude.getEngValue().getDoubleValue();
              break;
            case ENUMERATED:
              break;
            case FLOAT:
              ownship.Longitude = pvLongitude.getEngValue().getFloatValue();
              break;
            case NONE:
              break;
            case SINT32:
              break;
            case SINT64:
              break;
            case STRING:
              break;
            case TIMESTAMP:
              break;
            case UINT32:
              break;
            case UINT64:
              break;
            default:
              break;
          }
        }

        ownship.Altitude = 1000;
        ownship.TrueTrackAngle = true;
        ownship.Airborne = true;

        ownship.i = 10;
        ownship.a = 9;

        ownship.horizontalVelocity = 90; // Knots

        ownship.verticalVelocity = 64; // FPM

        ownship.trackHeading = 45; // Degrees

        ownship.ee = 1; // Should be an enum

        ownship.callSign = "N825V";

        try {
          d.datagram.setData(ownship.toBytes());
        } catch (Exception e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }

        ownship.px = 0;
        GDL90Socket.send(d.datagram);

        ownShipReportCount++;

        if (this.ownShipReportStream != null) {
          this.ownShipReportStream.emitTuple(
              new Tuple(
                  gftdef,
                  Arrays.asList(
                      timeService.getMissionTime(), "ownShipReport", d.datagram.getData())));
        }
      }
    }
  }

  private synchronized void AHRSMessage() throws IOException {

    for (GDL90Device d : gdl90Devices.values()) {

      if (d.alive) {

        AHRS ahrs = new AHRS();

        org.yamcs.protobuf.Pvalue.ParameterValue pvRoll = paramsToSend.get("Roll");

        if (pvRoll != null) {
          switch (pvRoll.getEngValue().getType()) {
            case AGGREGATE:
              break;
            case ARRAY:
              break;
            case BINARY:
              break;
            case BOOLEAN:
              break;
            case DOUBLE:
              ahrs.Roll = (int) pvRoll.getEngValue().getDoubleValue();
              break;
            case ENUMERATED:
              break;
            case FLOAT:
              ahrs.Roll = (int) pvRoll.getEngValue().getFloatValue();
              break;
            case NONE:
              break;
            case SINT32:
              break;
            case SINT64:
              break;
            case STRING:
              break;
            case TIMESTAMP:
              break;
            case UINT32:
              break;
            case UINT64:
              break;
            default:
              break;
          }
        }

        org.yamcs.protobuf.Pvalue.ParameterValue pvPitch = paramsToSend.get("Pitch");

        if (pvPitch != null) {
          switch (pvPitch.getEngValue().getType()) {
            case AGGREGATE:
              break;
            case ARRAY:
              break;
            case BINARY:
              break;
            case BOOLEAN:
              break;
            case DOUBLE:
              ahrs.Pitch = (int) pvPitch.getEngValue().getDoubleValue();
              break;
            case ENUMERATED:
              break;
            case FLOAT:
              ahrs.Pitch = (int) pvPitch.getEngValue().getFloatValue();
              break;
            case NONE:
              break;
            case SINT32:
              break;
            case SINT64:
              break;
            case STRING:
              break;
            case TIMESTAMP:
              break;
            case UINT32:
              break;
            case UINT64:
              break;
            default:
              break;
          }
        }

        org.yamcs.protobuf.Pvalue.ParameterValue pvAHRS_Heading = paramsToSend.get("AHRS_Heading");

        if (pvAHRS_Heading != null) {
          switch (pvAHRS_Heading.getEngValue().getType()) {
            case AGGREGATE:
              break;
            case ARRAY:
              break;
            case BINARY:
              break;
            case BOOLEAN:
              break;
            case DOUBLE:
              ahrs.Heading = (int) pvAHRS_Heading.getEngValue().getDoubleValue();
              break;
            case ENUMERATED:
              break;
            case FLOAT:
              ahrs.Heading = (int) pvAHRS_Heading.getEngValue().getFloatValue();
              break;
            case NONE:
              break;
            case SINT32:
              break;
            case SINT64:
              break;
            case STRING:
              break;
            case TIMESTAMP:
              break;
            case UINT32:
              break;
            case UINT64:
              break;
            default:
              break;
          }
        }

        try {
          d.datagram.setData(ahrs.toBytes());
        } catch (Exception e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
        GDL90Socket.send(d.datagram);

        if (this.AHRSStream != null) {
          try {
            this.AHRSStream.emitTuple(
                new Tuple(
                    gftdef,
                    Arrays.asList(timeService.getMissionTime(), "AHRSStream", ahrs.toBytes())));
          } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
          }
        }

        AHRSCount++;
      }
    }
  }

  private synchronized void sendOwnshipGeoAltitude() throws IOException {

    for (GDL90Device d : gdl90Devices.values()) {
      if (d.alive) {

        com.windhoverlabs.yamcs.gdl90.OwnshipGeoAltitude geoAlt =
            new com.windhoverlabs.yamcs.gdl90.OwnshipGeoAltitude();

        geoAlt.ownshipAltitude = 3000;

        org.yamcs.protobuf.Pvalue.ParameterValue pvAltitude = paramsToSend.get("Altitude");

        if (pvAltitude != null) {
          switch (pvAltitude.getEngValue().getType()) {
            case AGGREGATE:
              break;
            case ARRAY:
              break;
            case BINARY:
              break;
            case BOOLEAN:
              break;
            case DOUBLE:
              geoAlt.ownshipAltitude = (int) pvAltitude.getEngValue().getDoubleValue();
              break;
            case ENUMERATED:
              break;
            case FLOAT:
              geoAlt.ownshipAltitude = (int) pvAltitude.getEngValue().getFloatValue();
              break;
            case NONE:
              break;
            case SINT32:
              break;
            case SINT64:
              break;
            case STRING:
              break;
            case TIMESTAMP:
              break;
            case UINT32:
              break;
            case UINT64:
              break;
            default:
              break;
          }
        }
        geoAlt.verticalFigureOfMerit = 50;
        geoAlt.verticalWarningIndicator = false;

        try {
          d.datagram.setData(geoAlt.toBytes());
        } catch (Exception e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
        GDL90Socket.send(d.datagram);

        if (this.ownShipGeoAltitudeStream != null) {
          this.ownShipGeoAltitudeStream.emitTuple(
              new Tuple(
                  gftdef,
                  Arrays.asList(
                      timeService.getMissionTime(), "ownShipGeoAltitude", d.datagram.getData())));
        }

        ownshipGeoAltitudeCount++;
      }
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

  @Override
  public void connecting() {
    // TODO Auto-generated method stub

  }

  public static NamedObjectId identityOf(String pvName) {
    return NamedObjectId.newBuilder().setName(pvName).build();
  }

  /** Async adds a Yamcs PV for receiving updates. */
  public void register(String pvName) {
    NamedObjectId id = identityOf(pvName);
    try {
      subscription.sendMessage(
          SubscribeParametersRequest.newBuilder()
              .setInstance(this.yamcsInstance)
              .setProcessor("realtime")
              .setSendFromCache(true)
              .setAbortOnInvalid(false)
              .setUpdateOnExpiration(false)
              .addId(id)
              .setAction(Action.ADD)
              .build());
    } catch (Exception e) {
      System.out.println("e:" + e);
    }
  }

  @Override
  public void connected() {
    // TODO Auto-generated method stub

    subscription = yclient.createParameterSubscription();
    subscription.addListener(this);
    // TODO:Make this configurable
    for (Map.Entry<String, String> pvName : pvMap.entrySet()) {
      register(pvName.getValue());
    }
  }

  @Override
  public void connectionFailed(Throwable cause) {
    // TODO Auto-generated method stub

  }

  @Override
  public void disconnected() {
    // TODO Auto-generated method stub

  }

  @Override
  public void onData(List<org.yamcs.protobuf.Pvalue.ParameterValue> values) {
    // TODO Auto-generated method stub

    for (org.yamcs.protobuf.Pvalue.ParameterValue p : values) {
      if (pvMap.containsValue(p.getId().getName())) {
        String pvLabel =
            pvMap.entrySet().stream()
                .filter(entry -> entry.getValue().equals(p.getId().getName()))
                .map(Entry::getKey)
                .collect(Collectors.toList())
                .get(0);
        paramsToSend.put(pvLabel, p);
      }
    }
  }

  @Override
  public long getDataInCount() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public long getDataOutCount() {
    // TODO Auto-generated method stub
    return heartBeatCount
        + ownShipReportCount
        + ownshipGeoAltitudeCount
        + foreFlightIDCount
        + AHRSCount;
  }

  @Override
  public void resetCounters() {
    // TODO Auto-generated method stub
    heartBeatCount = 0;
    ownshipGeoAltitudeCount = 0;
    foreFlightIDCount = 0;
    AHRSCount = 0;
  }
}

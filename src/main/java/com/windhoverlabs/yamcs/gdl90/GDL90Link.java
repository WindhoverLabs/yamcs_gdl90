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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.yamcs.Processor;
import org.yamcs.Spec;
import org.yamcs.TmPacket;
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
    implements Runnable,
        StreamSubscriber,
        SystemParametersProducer,
        ParameterSubscription.Listener,
        ConnectionListener {
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

  private ParameterSubscription subscription;

  private HashMap<String, org.yamcs.protobuf.Pvalue.ParameterValue> paramsToSend =
      new HashMap<String, org.yamcs.protobuf.Pvalue.ParameterValue>();

  private String yamcsHost;
  private int yamcsPort;

  private String processorName;
  private Processor processor;

  private YamcsClient yclient;

  //  int MAX_LENGTH = 32 * 1024;
  int MAX_LENGTH = 32;
  DatagramPacket foreFlightdatagram = new DatagramPacket(new byte[MAX_LENGTH], MAX_LENGTH);

  DatagramPacket GDL90Datagram;

  /* Constants */
  static final byte[] CFE_FS_FILE_CONTENT_ID_BYTE =
      BaseEncoding.base16().lowerCase().decode("63464531".toLowerCase());

  private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

  private boolean foreFlighConnected = false;

  private Map<String, String> pvMap;

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

    yamcsHost = this.getConfig().getString("yamcsHost", "http://localhost");
    yamcsPort = this.getConfig().getInt("yamcsPort", 8090);

    pvMap = this.config.getMap("pvMap");
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

    com.windhoverlabs.yamcs.gdl90.OwnshipReport ownship =
        new com.windhoverlabs.yamcs.gdl90.OwnshipReport();

    /**
     * Report Data: No Traffic Alert ICAO ADS-B Address (octal): 52642511 8 Latitude: 44.90708
     * (North) Longitude: -122.99488 (West) Altitude: 5,000 feet (pressure altitude) Airborne with
     * True Track HPL = 20 meters, HFOM = 25 meters (NIC = 10, NACp = 9) Horizontal velocity: 123
     * knots at 45 degrees (True Track) Vertical velocity: 64 FPM climb Emergency/Priority Code:
     * none Emitter Category: Light Tail Number: N825
     */
    ownship.TrafficAlertStatus = false;
    ownship.AddressType = 0;
//    The ParticipantAddress seems to impact the way Altitude gets displayed on ForeFlight
    ownship.ParticipantAddress = 0; // base 8
    ownship.Latitude = 44.90708;
    ownship.Longitude = -122.99488;

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
      GDL90Datagram.setData(ownship.toBytes());
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    ownship.px = 0;

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
              .setProcessor(processorName)
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

    //	  TODO:Send Event instead?
    //    System.out.println("*****connected*****");
    //    subscription = yclient.createParameterSubscription();
    //    subscription.addListener(this);
    //    // TODO:Make this configurable
    //    for (Map.Entry<String, String> pvName : pvMap.entrySet()) {
    //      register(pvName.getValue());
    //    }

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
    //	    SimMessage.VehicleStateMessage.Builder msgBuilder =
    // SimMessage.VehicleStateMessage.newBuilder();
    //	    for (Map.Entry<String, ParameterValue> pSet : paramsToSend.entrySet()) {
    //	      org.yamcs.protobuf.Yamcs.Value pv = pSet.getValue().getEngValue();
    //	      switch (pv.getType()) {
    //	        case AGGREGATE:
    //	          break;
    //	        case ARRAY:
    //	          break;
    //	        case BINARY:
    //	          break;
    //	        case BOOLEAN:
    //	          break;
    //	        case DOUBLE:
    //	          msgBuilder.setField(
    //	              SimMessage.VehicleStateMessage.getDescriptor().findFieldByName(pSet.getKey()),
    //	              pv.getDoubleValue());
    //	          break;
    //	        case ENUMERATED:
    //	          break;
    //	        case FLOAT:
    //	          msgBuilder.setField(
    //	              SimMessage.VehicleStateMessage.getDescriptor().findFieldByName(pSet.getKey()),
    //	              pv.getFloatValue());
    //	          break;
    //	        case NONE:
    //	          break;
    //	        case SINT32:
    //	          break;
    //	        case SINT64:
    //	          break;
    //	        case STRING:
    //	          break;
    //	        case TIMESTAMP:
    //	          break;
    //	        case UINT32:
    //	          break;
    //	        case UINT64:
    //	          break;
    //	        default:
    //	          break;
    //	      }
    //	    }
    //	    SimMessage.VehicleStateMessage msg = msgBuilder.build();
    //	    DatagramPacket dtg =
    //	        new DatagramPacket(msg.toByteArray(), msg.toByteArray().length, udpAddress, udpPort);
    //
    //	    try {
    //	      outSocket.send(dtg);
    //	    } catch (IOException e) {
    //	      // TODO Auto-generated catch block
    //	      e.printStackTrace();
    //	    }
  }
}

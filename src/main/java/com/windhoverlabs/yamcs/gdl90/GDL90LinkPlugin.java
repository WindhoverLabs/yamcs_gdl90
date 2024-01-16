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

import org.yamcs.Plugin;
import org.yamcs.PluginException;
import org.yamcs.YConfiguration;
import org.yamcs.logging.Log;

public class GDL90LinkPlugin implements Plugin {
  private static final Log log = new Log(GDL90LinkPlugin.class);

  @Override
  public void onLoad(YConfiguration config) throws PluginException {
    //    YamcsServer yamcs = YamcsServer.getServer();
    //
    //    EventProducer eventProducer =
    //        EventProducerFactory.getEventProducer(null, this.getClass().getSimpleName(), 10000);
    //
    //    eventProducer.sendInfo("EvsCSVModePlugin loaded");
    //    List<HttpServer> httpServers = yamcs.getGlobalServices(HttpServer.class);
    //    if (httpServers.isEmpty()) {
    //      log.warn(
    //          "Yamcs does not appear to be running an HTTP Server. The EvsCSVModePlugin was not
    // initialized.");
    //      return;
    //    }
    //
    //    HttpServer httpServer = httpServers.get(0);
    //    // Add API to server
    //    try (InputStream in = getClass().getResourceAsStream("/yamcs-cfs-evs.protobin")) {
    //      httpServer.getProtobufRegistry().importDefinitions(in);
    //    } catch (IOException e) {
    //      throw new PluginException(e);
    //    }

    //    httpServer.addApi(new org.yamcs.http.api.GDL90API());
  }

  /**
   * The workspace directory is the directory that holds all of our yamcs configuration.
   *
   * @return
   */
  public String getWorkspaceDir() {
    return System.getProperty("user.dir");
  }
}

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


package org.yamcs.http.api;

import com.windhoverlabs.yamcs.csv.api.AbstractCSVModeApi;
import com.windhoverlabs.yamcs.csv.api.EvsCSVModeConfig;
import com.windhoverlabs.yamcs.csv.api.SetModeRequest;
import org.yamcs.YamcsServer;
import org.yamcs.api.Observer;
import org.yamcs.events.EventProducer;
import org.yamcs.events.EventProducerFactory;
import org.yamcs.http.Context;
import org.yamcs.tctm.Link;

public class GDL90API extends AbstractCSVModeApi<Context> {

  private EventProducer eventProducer =
      EventProducerFactory.getEventProducer(null, this.getClass().getSimpleName(), 10000);

  @Override
  public void setMode(Context c, SetModeRequest request, Observer<EvsCSVModeConfig> observer) {
    Link l =
        YamcsServer.getServer()
            .getInstance(request.getInstance())
            .getLinkManager()
            .getLink(request.getLinkName());

    if (l == null) {
      eventProducer.sendInfo("Link:" + request.getLinkName() + " does not exist");
      observer.complete();
      return;
    }
    //    ((com.windhoverlabs.yamcs.gdl90.GDL90Link) l).setMode(request.getMode());
    //    observer.complete(EvsCSVModeConfig.newBuilder().build());
  }
}

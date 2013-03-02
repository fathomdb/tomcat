/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.catalina.ha.session;

import java.io.IOException;

import org.apache.catalina.Container;
import org.apache.catalina.Context;
import org.apache.catalina.Engine;
import org.apache.catalina.LifecycleException;
import org.apache.catalina.Session;
import org.apache.catalina.core.StandardEngine;
import org.apache.catalina.ha.ClusterListener;
import org.apache.catalina.ha.ClusterMessage;
import org.apache.juli.logging.Log;
import org.apache.juli.logging.LogFactory;
import org.apache.tomcat.util.res.StringManager;

/**
 * Receive SessionID cluster change from other backup node after primary session
 * node is failed.
 *
 * @author Peter Rossbach
 * @version $Id$
 */
public class JvmRouteSessionIDBinderListener extends ClusterListener {

    private static final Log log =
        LogFactory.getLog(JvmRouteSessionIDBinderListener.class);

    private static final StringManager sm =
        StringManager.getManager(Constants.Package);

    //--Instance Variables--------------------------------------


    protected boolean started = false;

    //--Constructor---------------------------------------------

    public JvmRouteSessionIDBinderListener() {
        // NO-OP
    }

    //--Logic---------------------------------------------------

    /**
     * Add this Mover as Cluster Listener ( receiver)
     *
     * @throws LifecycleException
     */
    public void start() throws LifecycleException {
        if (started)
            return;
        getCluster().addClusterListener(this);
        started = true;
        if (log.isInfoEnabled())
            log.info(sm.getString("jvmRoute.clusterListener.started"));
    }

    /**
     * Remove this from Cluster Listener
     *
     * @throws LifecycleException
     */
    public void stop() throws LifecycleException {
        started = false;
        getCluster().removeClusterListener(this);
        if (log.isInfoEnabled())
            log.info(sm.getString("jvmRoute.clusterListener.stopped"));
    }

    /**
     * Callback from the cluster, when a message is received, The cluster will
     * broadcast it invoking the messageReceived on the receiver.
     *
     * @param msg
     *            ClusterMessage - the message received from the cluster
     */
    @Override
    public void messageReceived(ClusterMessage msg) {
        if (msg instanceof SessionIDMessage) {
            SessionIDMessage sessionmsg = (SessionIDMessage) msg;
            if (log.isDebugEnabled())
                log.debug(sm.getString(
                        "jvmRoute.receiveMessage.sessionIDChanged", sessionmsg
                                .getOrignalSessionID(), sessionmsg
                                .getBackupSessionID(), sessionmsg
                                .getContextName()));
            Container container = getCluster().getContainer();
            Container host = null ;
            if(container instanceof Engine) {
                host = container.findChild(sessionmsg.getHost());
            } else {
                host = container ;
            }
            if (host != null) {
                Context context = (Context) host.findChild(sessionmsg
                        .getContextName());
                if (context != null) {
                    try {
                        Session session = context.getManager().findSession(
                                sessionmsg.getOrignalSessionID());
                        if (session != null) {
                            session.setId(sessionmsg.getBackupSessionID());
                        } else if (log.isInfoEnabled())
                            log.info(sm.getString("jvmRoute.lostSession",
                                    sessionmsg.getOrignalSessionID(),
                                    sessionmsg.getContextName()));
                    } catch (IOException e) {
                        log.error(e);
                    }

                } else if (log.isErrorEnabled())
                    log.error(sm.getString("jvmRoute.contextNotFound",
                            sessionmsg.getContextName(), ((StandardEngine) host
                                    .getParent()).getJvmRoute()));
            } else if (log.isErrorEnabled())
                log.error(sm.getString("jvmRoute.hostNotFound", sessionmsg.getContextName()));
        }
        return;
    }

    /**
     * Accept only SessionIDMessages
     *
     * @param msg
     *            ClusterMessage
     * @return boolean - returns true to indicate that messageReceived should be
     *         invoked. If false is returned, the messageReceived method will
     *         not be invoked.
     */
    @Override
    public boolean accept(ClusterMessage msg) {
        return (msg instanceof SessionIDMessage);
    }
}

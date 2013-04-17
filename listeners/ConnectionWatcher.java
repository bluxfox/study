package com.sohu.smc.common.zk.listeners;

import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.state.ConnectionState;
import com.netflix.curator.framework.state.ConnectionStateListener;
import com.sohu.smc.common.zk.DiscoveryClientUtil;
import com.twitter.ostrich.stats.Stats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created with IntelliJ IDEA.
 * User: shijinkui
 * Date: 12-7-27
 * Time: 下午3:02
 * To change this template use File | Settings | File Templates.
 */
public class ConnectionWatcher implements ConnectionStateListener {
    private final Logger log = LoggerFactory.getLogger(ConnectionWatcher.class.getName());
    private final String path;

    public ConnectionWatcher(String path) {
        this.path = path;
    }

    @Override
    public void stateChanged(CuratorFramework client, ConnectionState newState) {
        switch (newState) {
            case LOST:
                //reconnect
                DiscoveryClientUtil.getInstance(path).rebuildConnection(path);
                Stats.incr("zk-connect-lost-to-reconnect");
                log.warn("zk client connection lost, reconnect it.");
                break;
            default:
                break;
        }
    }
}

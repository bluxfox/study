package com.sohu.smc.common.zk.listeners;

import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.api.CuratorEvent;
import com.netflix.curator.framework.api.CuratorListener;
import com.twitter.ostrich.stats.Stats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created with IntelliJ IDEA.
 * User: shijinkui
 * Date: 12-7-27
 * Time: 下午2:40
 * To change this template use File | Settings | File Templates.
 */
public class DataWatcher implements CuratorListener {
    private final Logger log = LoggerFactory.getLogger(DataWatcher.class.getName());

    @Override
    public void eventReceived(CuratorFramework client, CuratorEvent event) throws Exception {
        switch (event.getType()) {
            case DELETE:
            case CREATE:
            case SET_DATA:
                Stats.incr("service-" + event.getName());
                log.warn("service stat change:" + event.getName() + "|path:" + event.getPath());
                client.sync(event.getPath(), event.getContext());       //sync the server
                break;

            case CLOSING:

                client.getConnectionStateListenable();
                break;

            default:
                break;
        }
    }
}

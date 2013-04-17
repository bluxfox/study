package com.sohu.smc.common.zk.listeners;

import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.state.ConnectionState;
import com.netflix.curator.x.discovery.details.ServiceCacheListener;
import com.sohu.smc.common.zk.DiscoveryClientUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.netflix.curator.framework.state.ConnectionState.LOST;

/**
 * Created with IntelliJ IDEA.
 * User: shijinkui
 * Date: 12-7-27
 * Time: 下午4:29
 * To change this template use File | Settings | File Templates.
 */
public class ServiceCacheListenerImpl implements ServiceCacheListener {
    private final Logger log = LoggerFactory.getLogger(ServiceCacheListenerImpl.class.getName());
    private final String path;

    public ServiceCacheListenerImpl(String path) {
        this.path = path;
    }


    @Override
    public void cacheChanged() {
        log.warn("cache changed...");
        System.out.println(DiscoveryClientUtil.getInstance(path).getService("logmerge"));
    }

    @Override
    public void stateChanged(CuratorFramework client, ConnectionState newState) {
        if (newState == LOST) {
            DiscoveryClientUtil.getInstance(path).rebuildConnection(path);
            log.error("connection state changed to:" + newState);
        }
    }
}

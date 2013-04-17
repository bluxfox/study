package com.sohu.smc.common.zk;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Closeables;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.framework.state.ConnectionState;
import com.netflix.curator.retry.RetryNTimes;
import com.netflix.curator.retry.RetryOneTime;
import com.netflix.curator.x.discovery.ServiceCache;
import com.netflix.curator.x.discovery.ServiceDiscovery;
import com.netflix.curator.x.discovery.ServiceDiscoveryBuilder;
import com.netflix.curator.x.discovery.ServiceInstance;
import com.netflix.curator.x.discovery.details.JsonInstanceSerializer;
import com.netflix.curator.x.discovery.details.ServiceCacheListener;
import com.sohu.smc.common.zk.listeners.ConnectionWatcher;
import com.sohu.smc.common.zk.listeners.DataWatcher;
import com.sohu.smc.common.zk.listeners.ServiceCacheListenerImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;

/**
 * 客户端获取服务, 服务实体的格式：
 * <p/>
 * base path
 * <pre>
 * |_______ service A name
 *              |__________ instance 1 id --> (serialized ServiceInstance)
 *              |__________ instance 2 id --> (serialized ServiceInstance)
 *              |__________ ...
 * |_______ service B name
 *              |__________ instance 1 id --> (serialized ServiceInstance)
 *              |__________ instance 2 id --> (serialized ServiceInstance)
 *              |__________ ...
 * |_______ ...
 * </pre>
 * User: shijinkui
 * Date: 12-7-27
 * Time: 下午12:01
 * To change this template use File | Settings | File Templates.
 */
public class DiscoveryClientUtil {

    private final Logger log = LoggerFactory.getLogger(DiscoveryClientUtil.class.getName());
    private ServiceDiscovery<String> serviceDiscovery;
    private ConcurrentMap<String, ServiceCache<String>> listcache = Maps.newConcurrentMap();
    private CuratorFramework client = null;
    private final String path;

    private static DiscoveryClientUtil factory = null;
    private static final Object lock = new Object();

    public DiscoveryClientUtil(String path) {
        this.path = path;
    }

    public static DiscoveryClientUtil getInstance(String path) {
        if (factory != null) {
            return factory;
        }

        synchronized (lock) {
            if (factory == null) {
                factory = new DiscoveryClientUtil(path);
            }
        }
        return factory;
    }

    private synchronized void buildConnection() {
        try {
            client = CuratorFrameworkFactory.builder().connectString(PropertyConfig.getZookeeperAddress()).retryPolicy(new RetryNTimes(3, 20)).sessionTimeoutMs(3000).build();
            client.start();

            client.getCuratorListenable().addListener(new DataWatcher());
            client.getConnectionStateListenable().addListener(new ConnectionWatcher(path));
            serviceDiscovery = ServiceDiscoveryBuilder.builder(String.class).basePath(path).serializer(new JsonInstanceSerializer<String>(String.class)).client(client).build();
            serviceDiscovery.start();

            log.info("build a new zk connection:" + client.toString());

        } catch (IOException e) {
            log.error("create connection err,", e);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public synchronized void rebuildConnection(String path) {
        listcache.clear();
        buildConnection();
    }

    /**
     * 客户端使用
     *
     * @param serviceName
     * @return
     */
    public ServiceInstance<String> getService(final String serviceName) {
        ServiceInstance<String> serviceInstance = null;

        try {
            if (listcache.containsKey(serviceName)) {
                serviceInstance = getRandomService(serviceName);
            } else {
                ServiceCache<String> cache = serviceDiscovery.serviceCacheBuilder().name(serviceName).build();
                cache.start();
                cache.addListener(new ServiceCacheListenerImpl(path));
                listcache.put(serviceName, cache);
                serviceInstance = getRandomService(serviceName);
            }
        } catch (Exception e) {
            log.error("", e);
        }

        return serviceInstance;
    }

    /**
     * 客户端使用
     *
     * @param serviceName
     * @return
     */
    public List<ServiceInstance<String>> getAllService(final String serviceName) {

        try {
            if (listcache.containsKey(serviceName)) {
                return listcache.get(serviceName).getInstances();
            } else {
                ServiceCache<String> cache = serviceDiscovery.serviceCacheBuilder().name(serviceName).build();
                cache.start();
                cache.addListener(new ServiceCacheListenerImpl(path));
                listcache.put(serviceName, cache);
                return cache.getInstances();
            }
        } catch (Exception e) {
            log.error("", e);
        }

        return null;
    }

    private final Random random = new Random();

    private ServiceInstance<String> getRandomService(String serviceName) {

        List<ServiceInstance<String>> listservice = listcache.get(serviceName).getInstances();
        for (ServiceInstance<String> ins : listservice) {
            System.out.println(ins);
        }

        int index = random.nextInt(listservice.size());
        return listservice.get(index);
    }


    public void testUpdate() throws Exception {
        List<Closeable> closeables = Lists.newArrayList();
        try {
            CuratorFramework client = CuratorFrameworkFactory.newClient(PropertyConfig.getZookeeperAddress(), new RetryOneTime(1));
            closeables.add(client);
            client.start();

            ServiceInstance<String> instance = ServiceInstance.<String>builder().payload("thing").name("test").port(10064).build();
            ServiceDiscovery<String> discovery = ServiceDiscoveryBuilder.builder(String.class).basePath("/test").client(client).thisInstance(instance).build();
            closeables.add(discovery);
            discovery.start();

            final CountDownLatch latch = new CountDownLatch(1);
            ServiceCache<String> cache = discovery.serviceCacheBuilder().name("test").build();
            closeables.add(cache);
            ServiceCacheListener listener = new ServiceCacheListener() {
                @Override
                public void cacheChanged() {
                    latch.countDown();
                }

                @Override
                public void stateChanged(CuratorFramework client, ConnectionState newState) {
                }
            };
            cache.addListener(listener);
            cache.start();

            instance = ServiceInstance.<String>builder().payload("changed").name("test").port(10064).id(instance.getId()).build();
            discovery.registerService(instance);

        } finally {
            Collections.reverse(closeables);
            for (Closeable c : closeables) {
                Closeables.closeQuietly(c);
            }
        }
    }

}

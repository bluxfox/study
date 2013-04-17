package com.sohu.smc.common.zk;


import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.RetryOneTime;
import com.netflix.curator.x.discovery.ServiceDiscovery;
import com.netflix.curator.x.discovery.ServiceDiscoveryBuilder;
import com.netflix.curator.x.discovery.ServiceInstance;
import com.netflix.curator.x.discovery.ServiceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * server端服务注册
 * User: shijinkui
 * Date: 12-7-26
 * Time: 上午11:59
 */
public class DiscoveryServiceUtil {

    private final Logger log = LoggerFactory.getLogger(DiscoveryServiceUtil.class.getName());

    private final ServiceType nodetype;
    private final String basePath;

    public DiscoveryServiceUtil(ServiceType nodetype, String basePath) {
        this.nodetype = nodetype;
        this.basePath = basePath;
    }

    /**
     * 注册服务校验，唯一标识一个服务的条件：name, port
     * <pre>
     *         final ServerRegistService rs = new ServerRegistService();
     *         rs.regist(conf.getServiceName(), conf.getPort(), conf.getDesc());
     * </pre>
     *
     * @param name
     * @param port
     * @param description
     * @return
     */
    public boolean regist(String name, String address, int port, String description) {
        boolean existed = false, ret = false;
        Collection<ServiceInstance<String>> list = getServiceList(name);
        ServiceInstance<String> instance = null;

        if (list != null && list.size() > 0) {

            Iterator<ServiceInstance<String>> it = list.iterator();
            while (it.hasNext()) {
                instance = it.next();
                if (instance.getName().equals(name) && instance.getPort() == port && instance.getAddress().equals(address)) {
                    existed = true;
                    log.warn("regist service falure, the service[" + name + "-" + port + "] has existed.");
                    break;
                }
            }
        }

        try {
            if (existed) {
                return false;
            }

            instance = ServiceInstance.<String>builder().address(address).port(port).payload(description).name(name).serviceType(nodetype).build();
            ret = regist(instance);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return ret;
    }

    /**
     * service value model:
     * <pre>
     * {
     *      name='logmerge',
     *      id='1f379a40-6cae-4465-918e-81957274afa3',
     *      address='10.37.129.2', port=8090, sslPort=null,
     *      payload=logmergeaslfjalsf,
     *      registrationTimeUTC=1343317256032, serviceType=PERMANENT
     * }
     * </pre>
     *
     * @param instance
     * @return
     */
    private boolean regist(ServiceInstance instance) {
        log.info("===>>" + instance);
        List<Closeable> closeables = Lists.newArrayList();
        boolean ret = false;
        try {
            CuratorFramework client = CuratorFrameworkFactory.newClient(PropertyConfig.getZookeeperAddress(), new RetryOneTime(2));
            closeables.add(client);
            client.start();

            //regist service
            ServiceDiscovery discovery = ServiceDiscoveryBuilder.builder(String.class).client(client).basePath(basePath).thisInstance(instance).build();
            closeables.add(discovery);
//            discovery.registerService(instance);
            discovery.start();
            ret = true;
        } catch (IOException e) {
            e.printStackTrace();
            ret = false;
        } catch (Exception e) {
            e.printStackTrace();
            ret = false;
        } finally {
            System.out.println("===regist service successfully.=== \n" + instance.toString());
//            Collections.reverse(closeables);
//            for (Closeable c : closeables) {
//                Closeables.closeQuietly(c);
//            }
        }

        return ret;
    }

    /**
     * 检查是否存在服务
     *
     * @param serviceName
     * @param port
     * @return
     */
    public ServiceInstance<String> exist(String serviceName, String address, int port) {

        Collection<ServiceInstance<String>> list = getServiceList(serviceName);
        Preconditions.checkNotNull((list != null && list.size() > 0), "there no service to unregist.");

        Iterator<ServiceInstance<String>> it = list.iterator();
        ServiceInstance<String> instance = null, tmp;
        while (it.hasNext()) {
            tmp = it.next();
            if (tmp.getName().equals(serviceName) && tmp.getAddress().equals(address) && tmp.getPort() == port) {
                instance = tmp;
                break;
            }
        }

        return instance;
    }

    /**
     * unregist all service instance or single service.
     *
     * @return
     */
    public boolean removeService(String serviceName, String address, int port) {
        ServiceInstance instance = exist(serviceName, address, port);
        if (instance == null) {
            return false;
        }

        List<Closeable> closeables = Lists.newArrayList();
        boolean ret = false;
        try {
            CuratorFramework client = CuratorFrameworkFactory.newClient(PropertyConfig.getZookeeperAddress(), new RetryOneTime(1));
            closeables.add(client);
            client.start();

            //regist service
            ServiceDiscovery discovery = ServiceDiscoveryBuilder.builder(String.class).client(client).basePath(basePath).build();
            closeables.add(discovery);
            discovery.start();
            discovery.unregisterService(instance);
            ret = true;
        } catch (IOException e) {
            e.printStackTrace();
            ret = false;
        } catch (Exception e) {
            e.printStackTrace();
            ret = false;
        } finally {
            Collections.reverse(closeables);
            for (Closeable c : closeables) {
                Closeables.closeQuietly(c);
            }
        }

        return ret;
    }


    public Collection<ServiceInstance<String>> getServiceList(String serviceName) {

        Preconditions.checkArgument(serviceName != null, "service name must't be null.");
        List<Closeable> closeables = Lists.newArrayList();
        Collection<ServiceInstance<String>> list = Lists.newArrayList();
        try {
            CuratorFramework client = CuratorFrameworkFactory.newClient(PropertyConfig.getZookeeperAddress(), new RetryOneTime(1));
            closeables.add(client);
            client.start();

            ServiceDiscovery discovery = ServiceDiscoveryBuilder.builder(String.class).basePath(basePath).client(client).build();
            closeables.add(discovery);
            discovery.start();
            list = discovery.queryForInstances(serviceName);

            for (ServiceInstance<String> service : list) {
                System.out.println(service.toString());
            }

        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            Collections.reverse(closeables);
            for (Closeable c : closeables) {
                Closeables.closeQuietly(c);
            }
        }

        return list;
    }

    protected void forceDelete(String serviceName) {
        String path = basePath + "/" + serviceName;
        CuratorFramework client = CuratorFrameworkFactory.newClient(PropertyConfig.getZookeeperAddress(), new RetryOneTime(1));
        client.start();
        try {
            List<String> instanceIds = client.getChildren().forPath(path);
            for (String str : instanceIds) {
                System.out.println("delete note: " + path + "/" + str);
                client.delete().forPath(path + "/" + str);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        client.close();
    }

}

package com.sohu.smc.common.zk;

import com.sohu.smc.common.util.PropertyUtil;

import java.util.Properties;

/**
 * Created with IntelliJ IDEA.
 * User: shijinkui
 * Date: 12-12-28
 * Time: 下午10:54
 * To change this template use File | Settings | File Templates.
 */
public final class PropertyConfig {

    private static String address;
    private static String kafkaPort;

    static {
        load();
    }

    public static String getKafkaPort() {
        return kafkaPort;
    }

    public static String getZookeeperAddress() {
        return address;
    }

    private static void load() {
        Properties prop = PropertyUtil.load("zoo.properties");
        address = prop.getProperty("address");
        kafkaPort = prop.getProperty("kafkaPort");
        prop.clear();
        prop = null;
    }

}

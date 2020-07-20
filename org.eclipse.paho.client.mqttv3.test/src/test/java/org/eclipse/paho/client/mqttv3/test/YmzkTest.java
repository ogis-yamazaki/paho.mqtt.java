
package org.eclipse.paho.client.mqttv3.test;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.eclipse.paho.client.mqttv3.DisconnectedBufferOptions;
import org.eclipse.paho.client.mqttv3.IMqttAsyncClient;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.IMqttToken;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttClientPersistence;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.test.client.MqttClientFactoryPaho;
import org.eclipse.paho.client.mqttv3.test.logging.LoggingUtilities;
import org.eclipse.paho.client.mqttv3.test.properties.TestProperties;
import org.eclipse.paho.client.mqttv3.test.utilities.MqttV3Receiver;
import org.eclipse.paho.client.mqttv3.test.utilities.Utility;
import org.eclipse.paho.client.mqttv3.test.utilities.ConnectionManipulationProxyServer;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 *
 */
public class YmzkTest {

    static final Class<?> cclass = YmzkTest.class;
    static final String className = cclass.getName();
    static final Logger log = Logger.getLogger(className);

    private static URI serverURI;
    private static String serverURIString;
    private static MqttClientFactoryPaho clientFactory;
    private static String topicPrefix;

    static ConnectionManipulationProxyServer proxy;

    // @Parameters
    // public static Collection<Object[]> data() throws Exception {

    // return Arrays.asList(new Object[][] {
    // { TestProperties.getServerURI() }
    // });

    // }

    // public YmzkTest(URI serverURI) {
    // this.serverURI = serverURI;
    // }

    /**
     * @throws Exception
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {

        try {
            String methodName = Utility.getMethodName();
            LoggingUtilities.banner(log, cclass, methodName);

            clientFactory = new MqttClientFactoryPaho();
            clientFactory.open();
            topicPrefix = "SendReceiveAsyncTest-" + UUID.randomUUID().toString() + "-";

            serverURI = TestProperties.getServerURI();
            serverURIString = "tcp://" + serverURI.getHost() + ":" + serverURI.getPort();

            // Use 0 for the first time.
            proxy = new ConnectionManipulationProxyServer(serverURI.getHost(), serverURI.getPort(), 2883);
            proxy.startProxy();
            while (!proxy.isPortSet()) {
                Thread.sleep(0);
            }
            log.log(Level.INFO, "Proxy Started, port set to: " + proxy.getLocalPort());
        } catch (Exception exception) {
            log.log(Level.SEVERE, "caught exception:", exception);
            throw exception;
        }
    }

    /**
     * @throws Exception
     */
    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        String methodName = Utility.getMethodName();
        LoggingUtilities.banner(log, cclass, methodName);

        try {
            if (clientFactory != null) {
                clientFactory.close();
                clientFactory.disconnect();
            }
        } catch (Exception exception) {
            log.log(Level.SEVERE, "caught exception:", exception);
        }

        proxy.disableProxy();
    }

    /**
     * オフラインパブリッシュが有効な状態で、Subscribeしたらどうなるか
     * 
     * @throws Exception
     */
    @Test
    public void test_offline_subscribe() throws Exception {

        String methodName = Utility.getMethodName();
        LoggingUtilities.banner(log, cclass, methodName);

        MqttClientPersistence persistence = new MemoryPersistence();
        MqttAsyncClient asyncClient = new MqttAsyncClient("tcp://localhost:" + proxy.getLocalPort(), methodName,
                persistence);
        MqttConnectOptions opts = new MqttConnectOptions();
        opts.setCleanSession(false);
        opts.setMaxInflight(10); // 小さめ
        opts.setKeepAliveInterval(0);

        // オフラインバッファー有効化
        DisconnectedBufferOptions disconnectedOpts = new DisconnectedBufferOptions();
        disconnectedOpts.setBufferEnabled(true);
        disconnectedOpts.setPersistBuffer(true);
        asyncClient.setBufferOpts(disconnectedOpts);

        // Enable Proxy & Connect to server
        proxy.enableProxy();


        asyncClient.subscribe("topic1", 2);


        // // オフライン用メッセージを大量に用意する
        // int no_of_offline_messages = 1000;
        // List<MqttMessage> offlineTestMessages = new ArrayList<MqttMessage>(no_of_offline_messages);
        // for (int i = 0; i < no_of_offline_messages; i++) {
        //     String msg = String.format("[offline]Test Payload number %05d", i);
        //     MqttMessage testMessage = new MqttMessage(msg.getBytes());
        //     testMessage.setQos(2);
        //     testMessage.setRetained(false);
        //     offlineTestMessages.add(testMessage);
        // }
        // // オンライン用メッセージを大量に用意する
        // int no_of_online_messages = 9;
        // List<MqttMessage> onlineTestMessages = new ArrayList<MqttMessage>(no_of_online_messages);
        // for (int i = 0; i < no_of_online_messages; i++) {
        //     String msg = String.format("[online]Test Payload number %05d", i);
        //     MqttMessage testMessage = new MqttMessage(msg.getBytes());
        //     testMessage.setQos(2);
        //     testMessage.setRetained(false);
        //     onlineTestMessages.add(testMessage);
        // }

        // // オフラインパブリッシュ開始
        // for (MqttMessage msg : offlineTestMessages) {
        //     asyncClient.publish("topic1", msg);
        // }

        // 接続
        IMqttToken connectToken = asyncClient.connect(opts);
        connectToken.waitForCompletion(5000);


        // // オンラインパブリッシュ開始org.eclipse.paho.client.mqttv3/src/main/java/org/eclipse/paho/client/mqttv3/persist/MemoryPersistence.java


        // ArrayList<IMqttDeliveryToken> tokens = new ArrayList<IMqttDeliveryToken>();
        // // publish messages.
        // for (MqttMessage msg : onlineTestMessages) {
        //     IMqttDeliveryToken token = asyncClient.publish("topic1", msg);
        //     log.info("Client ID = " + token.getClient().getClientId());
        //     log.info("msgId = " + token.getMessageId());
        //     log.info("msg = " + msg);
        //     tokens.add(token);
        // }
        
        // // オンラインパブリッシュの完了を待つ
        // for (IMqttDeliveryToken token : tokens) {
        //     token.waitForCompletion(1000);
        // }


    
        // テスト終了
        IMqttToken disconnectToken;
        disconnectToken = asyncClient.disconnect(null, null);
        log.info("Disconnecting...");
        disconnectToken.waitForCompletion();

        asyncClient.close();

    }

    /**
     * オフラインパブリッシュが溜まっている状態で、オンラインパブリッシュがそれを追い越すことがあるか。
     * メッセージを大量に貯め、MaxInFlightのサイズを小さめにして、少しずつオフラインパブリッシュがはける状態を演出
     * 
     * @throws Exception
     */
    //@Test
    public void test_offline_publish_order() throws Exception {

        String methodName = Utility.getMethodName();
        LoggingUtilities.banner(log, cclass, methodName);

        MqttClientPersistence persistence = new MemoryPersistence();
        MqttAsyncClient asyncClient = new MqttAsyncClient("tcp://localhost:" + proxy.getLocalPort(), methodName,
                persistence);
        MqttConnectOptions opts = new MqttConnectOptions();
        opts.setCleanSession(false);
        opts.setMaxInflight(10); // 小さめ
        opts.setKeepAliveInterval(0);

        // オフラインバッファー有効化
        DisconnectedBufferOptions disconnectedOpts = new DisconnectedBufferOptions();
        disconnectedOpts.setBufferEnabled(true);
        disconnectedOpts.setPersistBuffer(true);
        asyncClient.setBufferOpts(disconnectedOpts);

        // Enable Proxy & Connect to server
        proxy.enableProxy();


        // オフライン用メッセージを大量に用意する
        int no_of_offline_messages = 1000;
        List<MqttMessage> offlineTestMessages = new ArrayList<MqttMessage>(no_of_offline_messages);
        for (int i = 0; i < no_of_offline_messages; i++) {
            String msg = String.format("[offline]Test Payload number %05d", i);
            MqttMessage testMessage = new MqttMessage(msg.getBytes());
            testMessage.setQos(2);
            testMessage.setRetained(false);
            offlineTestMessages.add(testMessage);
        }
        // オンライン用メッセージを大量に用意する
        int no_of_online_messages = 9;
        List<MqttMessage> onlineTestMessages = new ArrayList<MqttMessage>(no_of_online_messages);
        for (int i = 0; i < no_of_online_messages; i++) {
            String msg = String.format("[online]Test Payload number %05d", i);
            MqttMessage testMessage = new MqttMessage(msg.getBytes());
            testMessage.setQos(2);
            testMessage.setRetained(false);
            onlineTestMessages.add(testMessage);
        }

        // オフラインパブリッシュ開始
        for (MqttMessage msg : offlineTestMessages) {
            asyncClient.publish("topic1", msg);
        }

        // 接続
        IMqttToken connectToken = asyncClient.connect(opts);
        connectToken.waitForCompletion(5000);


        // オンラインパブリッシュ開始org.eclipse.paho.client.mqttv3/src/main/java/org/eclipse/paho/client/mqttv3/persist/MemoryPersistence.java


        ArrayList<IMqttDeliveryToken> tokens = new ArrayList<IMqttDeliveryToken>();
        // publish messages.
        for (MqttMessage msg : onlineTestMessages) {
            IMqttDeliveryToken token = asyncClient.publish("topic1", msg);
            log.info("Client ID = " + token.getClient().getClientId());
            log.info("msgId = " + token.getMessageId());
            log.info("msg = " + msg);
            tokens.add(token);
        }
        
        // オンラインパブリッシュの完了を待つ
        for (IMqttDeliveryToken token : tokens) {
            token.waitForCompletion(1000);
        }


    
        // テスト終了
        IMqttToken disconnectToken;
        disconnectToken = asyncClient.disconnect(null, null);
        log.info("Disconnecting...");
        disconnectToken.waitForCompletion();

        asyncClient.close();

    }

    @Test
    public void test_QoS0_publish() throws Exception {

        String methodName = Utility.getMethodName();
        LoggingUtilities.banner(log, cclass, methodName);

        MqttClientPersistence persistence = new MemoryPersistence();
        MqttAsyncClient asyncClient = new MqttAsyncClient("tcp://localhost:" + proxy.getLocalPort(), methodName,
                persistence);
        // MqttAsyncClient asyncClient = new MqttAsyncClient(serverURI.toString(),
        // "id_1", persistence);
        MqttConnectOptions opts = new MqttConnectOptions();
        opts.setCleanSession(false);
        opts.setMaxInflight(10000);
        opts.setKeepAliveInterval(0);

        // disconnectedOptsでBufferEnableとしておくと、このテストは成功する。
        // DisconnectedBufferOptions disconnectedOpts = new DisconnectedBufferOptions();
        // disconnectedOpts.setBufferEnabled(true);
        // disconnectedOpts.setPersistBuffer(true);
        // asyncClient.setBufferOpts(disconnectedOpts);

        // Enable Proxy & Connect to server
        proxy.enableProxy();

        // Connect to the server
        IMqttToken connectToken = asyncClient.connect(opts);
        connectToken.waitForCompletion(5000);

        int no_of_messages = 100;
        // generate test messages.
        List<MqttMessage> testMessages = new ArrayList<MqttMessage>(no_of_messages);
        for (int i = 0; i < no_of_messages; i++) {
            String msg = String.format("Test Payload number %05d", i);
            MqttMessage testMessage = new MqttMessage(msg.getBytes());
            testMessage.setQos(2);
            testMessage.setRetained(false);
            testMessages.add(testMessage);
        }

        ArrayList<IMqttDeliveryToken> tokens = new ArrayList<IMqttDeliveryToken>();
        // publish messages.
        for (MqttMessage msg : testMessages) {
            IMqttDeliveryToken token = asyncClient.publish("topic1", msg);
            log.info("Client ID = " + token.getClient().getClientId());
            log.info("msgId = " + token.getMessageId());
            log.info("msg = " + msg);
            // token.waitForCompletion(5000000);
            tokens.add(token);
        }

        for (IMqttDeliveryToken token : tokens) {
            token.waitForCompletion(1000);
        }

        IMqttToken disconnectToken;
        disconnectToken = asyncClient.disconnect(null, null);
        log.info("Disconnecting...");
        disconnectToken.waitForCompletion();

        asyncClient.close();

    }

    @Test
    public void test_一度も繋がずにpublishできる() throws Exception {

        String methodName = Utility.getMethodName();
        LoggingUtilities.banner(log, cclass, methodName);

        MqttClientPersistence persistence = new MemoryPersistence();
        MqttAsyncClient asyncClient = new MqttAsyncClient("tcp://localhost:" + proxy.getLocalPort(), methodName,
                persistence);
        // MqttAsyncClient asyncClient = new MqttAsyncClient(serverURI.toString(),
        // "id_1", persistence);
        MqttConnectOptions opts = new MqttConnectOptions();
        opts.setCleanSession(false);
        opts.setMaxInflight(10000);
        opts.setKeepAliveInterval(0);

        // disconnectedOptsでBufferEnableとしておくと、このテストは成功する。
        DisconnectedBufferOptions disconnectedOpts = new DisconnectedBufferOptions();
        disconnectedOpts.setBufferEnabled(true);
        disconnectedOpts.setPersistBuffer(true);
        asyncClient.setBufferOpts(disconnectedOpts);

        // Connect to the server
        // IMqttToken connectToken = asyncClient.connect(opts);
        // connectToken.waitForCompletion(5000);

        int no_of_messages = 100;
        // generate test messages.
        List<MqttMessage> testMessages = new ArrayList<MqttMessage>(no_of_messages);
        for (int i = 0; i < no_of_messages; i++) {
            MqttMessage testMessage = new MqttMessage("Test Payload".getBytes());
            testMessage.setQos(2);
            testMessage.setRetained(false);
            testMessages.add(testMessage);
        }

        // publish messages.
        for (MqttMessage msg : testMessages) {
            asyncClient.publish("topic1", msg);
        }

        boolean isConnected = asyncClient.isConnected();
        Assert.assertFalse(isConnected);

        List<String> list = Collections.list(persistence.keys());
        list.size();
        log.info("persistence count: " + list.size());
        Assert.assertTrue("MemoryPersistenceの要素数は0ではいはず", list.size() != 0);

        asyncClient.close();

    }

    @Test
    public void test_MemoryPersistenceは切断されても中身はクリアされないはず() throws Exception {

        String methodName = Utility.getMethodName();
        LoggingUtilities.banner(log, cclass, methodName);

        MqttClientPersistence persistence = new MemoryPersistence();
        MqttAsyncClient asyncClient = new MqttAsyncClient("tcp://localhost:" + proxy.getLocalPort(), methodName,
                persistence);
        // MqttAsyncClient asyncClient = new MqttAsyncClient(serverURI.toString(),
        // "id_1", persistence);
        MqttConnectOptions opts = new MqttConnectOptions();
        opts.setCleanSession(false);
        opts.setMaxInflight(10000);
        opts.setKeepAliveInterval(0);

        // disconnectedOptsでBufferEnableとしておくと、このテストは成功する。
        // DisconnectedBufferOptions disconnectedOpts = new DisconnectedBufferOptions();
        // disconnectedOpts.setBufferEnabled(true);
        // disconnectedOpts.setPersistBuffer(true);
        // asyncClient.setBufferOpts(disconnectedOpts);

        // Enable Proxy & Connect to server
        proxy.enableProxy();

        // Connect to the server
        IMqttToken connectToken = asyncClient.connect(opts);
        connectToken.waitForCompletion(5000);

        int no_of_messages = 100;
        // generate test messages.
        List<MqttMessage> testMessages = new ArrayList<MqttMessage>(no_of_messages);
        for (int i = 0; i < no_of_messages; i++) {
            MqttMessage testMessage = new MqttMessage("Test Payload".getBytes());
            testMessage.setQos(2);
            testMessage.setRetained(false);
            testMessages.add(testMessage);
        }

        // publish messages.
        for (MqttMessage msg : testMessages) {
            IMqttDeliveryToken token = asyncClient.publish("topic1", msg);
            log.info("Client ID = " + token.getClient().getClientId());
            log.info("msgId = " + token.getMessageId());
            // token.waitForCompletion(5000000);
        }

        // Enable Proxy & Connect to server
        proxy.disableProxy();
        boolean isConnected = asyncClient.isConnected();
        log.info("Proxy Disconnect isConnected: " + isConnected);
        Assert.assertFalse(isConnected);

        List<String> list = Collections.list(persistence.keys());
        list.size();
        log.info("persistence count: " + list.size());
        Assert.assertTrue("MemoryPersistenceの要素数は0ではいはず", list.size() != 0);

        asyncClient.close();

    }

}

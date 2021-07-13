package com.fh.kafka.kafkahelper.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

public class MyWriter {
    private ZooKeeper mZooKeeper;

    public static void main(String[] args) {
        //初始化log4j，zookeeper否则报错。
        org.apache.log4j.BasicConfigurator.configure();

        try {
            MyWriter m = new MyWriter();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public MyWriter() throws Exception {
        String ip = "localhost";
        String addrs = ip + ":2181," + ip + ":2182";

        //连接zookeeper服务器。
        //addrs是一批地址，如果其中某一个服务器挂掉，其他仍可用。
        mZooKeeper = new ZooKeeper(addrs, 300 * 1000, new MyWatcher());

        synchronized (MyWriter.class) {
            System.out.println("等待连接建立...");
            MyWriter.class.wait();
        }

        System.out.println("继续执行");

        operation(mZooKeeper);
        //mZooKeeper.close();
    }

    private class MyWatcher implements Watcher {
        @Override
        public void process(WatchedEvent event) {
            if (mZooKeeper.getState() == ZooKeeper.States.CONNECTED) {
                System.out.println("连接建立！");

                synchronized (MyWriter.class) {
                    System.out.println("唤醒主线程的等待!");
                    MyWriter.class.notifyAll();
                }
            }

            if (event.getState() == Event.KeeperState.SyncConnected) {
                System.out.println("状态:" + Event.KeeperState.SyncConnected);
            }
        }
    }

    private void operation(ZooKeeper zk) throws Exception {
        if (zk.getState().equals(ZooKeeper.States.CONNECTED)) {
            System.out.println("已经连接到zookeeper服务器");
        } else {
            System.out.println("连接zookeeper服务器失败!");

            return;
        }

        String node = "/zhang_phil_node";
        String data = "hello,world!";

        //检测node节点是否存在。
        Stat stat = zk.exists(node, false);

        if (stat == null) {
            //创建节点。
            String result = zk.create(node, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            System.out.println(result);
        }

        //获取节点的值。
        byte[] b = zk.getData(node, false, stat);
        System.out.println(new String(b));

        zk.close();
    }
}



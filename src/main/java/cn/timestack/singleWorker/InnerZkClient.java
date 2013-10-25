package cn.timestack.singleWorker;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.Transaction;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.data.Stat;

import cn.timestack.singleWorker.object.Config;

/**
 * @author guanqing-liu 2013年9月13日 处理worker节点的操作
 */
public class InnerZkClient {

    // 当前server的节点路径:/singleWorker/${name}/servers/${sid}
    private String sidNodePath;

    private ZooKeeper zookeeper;

    private static Logger log = Logger.getLogger(InnerZkClient.class);

    // 是否可用 ,如果schedule被人为的close,那么isAlive将为false.
    private boolean isAlive = true;

    private Config config = Config.getInstance();

    // 控制zookeer事件的对象锁,所有与zk连接有关的同步控制,使用此锁
    private Object waiter;

    private boolean init = false;

    public InnerZkClient() {
        waiter = config.waiter();
        connectZK();
    }

    /**
     * core method,启动zk服务 本实例基于自动重连策略,如果zk连接没有建立成功或者在运行时断开,将会自动重连.
     */
    private void connectZK() {
        synchronized (waiter) {
            try {
                SessionWatcher watcher = new SessionWatcher();
                // session的构建是异步的
                zookeeper = new ZooKeeper(config.getConnectString(), config.getSessionTimeout(), watcher, false);
                if (StringUtils.isNotBlank(config.getNamespaceAuth())) {
                    zookeeper.addAuthInfo("digest", config.getNamespaceAuth().getBytes(Config.CHARSET));
                }
                // 准备服务所需要的所有meta信息
                // 同步,直到zk可用
                prepareMetaData();
            } catch (Exception e) {
                log.error(e);
            }
            waiter.notifyAll();
        }
    }

    /**
     * 准备zk znode信息: /singleWorker/${name}/servers/
     * /singleWorker/${name}/servers/${sid}
     *
     * @throws Exception
     */
    protected void prepareMetaData() throws Exception {
        // /workerSchedule节点书否存在
        try {
            zookeeper.create(Config.PREFIX, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (NodeExistsException e) {
            //
        }
        String namespace = config.getNamespace();
        try {
            zookeeper.create(namespace, null, config.getAcls(), CreateMode.PERSISTENT);
        } catch (NodeExistsException e) {
            //
        }

        // 检测根节点是否存在,如果不存在,则异常退出
        zookeeper.setData(namespace, null, -1);

        // 创建"/singleWorker/${name}/servers/${sid}"节点
        createSidNode();
        // 创建"/singleWorker/${name}/workers"节点
        createWorkersNode();

        createBalanceNode();
        createBalanceLeaderNode();
    }

    /**
     * 检测/workers节点是否存在,如果不存在,则创建
     *
     * @return
     * @throws Exception
     */
    private boolean createBalanceNode() throws Exception {
        // balance节点上保存最后"均衡的时间"
        try {
            String lastTime = String.valueOf(System.currentTimeMillis());
            zookeeper.create(config.getBalancePath(), lastTime.getBytes(Config.CHARSET), config.getAcls(), CreateMode.PERSISTENT);
        } catch (NodeExistsException e) {
            //
        }
        return true;
    }

    private boolean createBalanceLeaderNode() throws Exception {
        // leader节点上保存,负责balance的实例
        try {
            String leader = config.getSid();
            zookeeper.create(config.getBalancePath() + "/leader", leader.getBytes(Config.CHARSET), config.getAcls(), CreateMode.EPHEMERAL); //临时节点
        } catch (NodeExistsException e) {
            //
        }
        return true;
    }

    /**
     * 检测/workers节点是否存在,如果不存在,则创建
     *
     * @return
     * @throws Exception
     */
    private boolean createWorkersNode() throws Exception {
        try {
            zookeeper.create(config.getWorkersPath(), null, config.getAcls(), CreateMode.PERSISTENT);
        } catch (NodeExistsException e) {
            //
        }
        return true;
    }

    /**
     * 如果节点已经存在,则返回false 如果创建了新的server节点,则返回true.
     *
     * @return
     * @throws Exception
     */
    private boolean createSidNode() throws Exception {
        // 如果是闪断
        if (sidNodePath != null) {
            if (zookeeper.exists(sidNodePath, false) != null) {
                return false;
            }
        }
        String serversPath = config.getNamespace() + "/servers";
        try {
            zookeeper.create(serversPath, null, config.getAcls(), CreateMode.PERSISTENT);
        } catch (NodeExistsException e) {
            //
        }
        // 创建"/singleWorker/${name}/servers/${sid}"节点
        // 临时节点,当server失效后,节点会被删除
        sidNodePath = zookeeper.create(serversPath + "/" + config.getSid(), null, config.getAcls(), CreateMode.EPHEMERAL);
        return true;
    }

    /**
     * 关闭任务调度器，关闭zookeeper链接 此后将导致任务被立即取消，singleWorkerManager实例将无法被重用
     */
    public void close() {
        isAlive = false;
        innerClose();
    }

    private void innerClose() {
        try {
            synchronized (waiter) {
                if (zookeeper != null) {
                    zookeeper.close();
                }
                waiter.notifyAll();
            }
            log.info(">>>>>>>>>>InnerZkClient,close successfully.");
        } catch (Exception e) {
            log.error(">>>>>>>>>>InnerZkClient,close.", e);
        }
    }

    /**
     * 当前实例的zkClient是否链接正常.
     *
     * @return
     */
    protected boolean isReady() {
        if (!isAlive) {
            return false;
        }
        if (zookeeper.getState().isConnected() && init) {
            return true;
        }
        return false;
    }

    protected boolean isAlive() {
        return this.isAlive;
    }

    /**
     * 获取当前zk的状态,如果zk被开发者close或者尚未实例化,则返回null
     *
     * @return
     */
    protected States getZkStates() {
        if (zookeeper == null) {
            return null;
        }
        return zookeeper.getState();
    }

    /**
     * 新增一个worker节点,如果此workerId已经存在或者插入成功,则返回true.否则返回false
     * 节点被创建后,等到sync线程调度
     * 节点的path为:${namespace}/workers/${workerId}
     *
     * @param workerId
     * @return
     * @throws Exception
     */
    public boolean createWorkerNode(String workerId, String cronExpression) {
        try {
            String data = config.getSid();
            //对于那些使用workerCaller的方式调度任务的,需要把cronExpression保存下来
            if (StringUtils.isNotBlank(cronExpression)) {
                data += "," + cronExpression;
            }
            zookeeper.create(config.getWorkersPath() + "/" + workerId, data.getBytes(Config.CHARSET), config.getAcls(), CreateMode.PERSISTENT);
        } catch (NodeExistsException e) {
            //
        } catch (Exception e) {
            log.debug(">>>>>>>>>>InnerZkClient.createWokrerNode,error:" + workerId, e);
            return false;
        }
        return true;
    }


    /**
     * 获取一个worker的cronExpression,如果节点不存在或者数据位空,则返回null
     *
     * @param workerId
     * @return
     * @throws Exception
     */
    public String getWorkerCronExpression(String workerId) {
        String workerPath = config.getWorkersPath() + "/" + workerId;
        try {
            byte[] source = zookeeper.getData(workerPath, null, null);
            String data = new String(source, Config.CHARSET);
            if (data.indexOf(",") == -1) {
                return null;
            }
            return data.substring(data.lastIndexOf(",") + 1);
        } catch (NoNodeException e) {
            //
        } catch (Exception e) {
            log.debug(">>>>>>>>>>InnerZkClient.getWorkerCronExpression,error:" + workerId, e);
        }
        return null;
    }


    /**
     * 更新worker信息,如果此worker为自己运行,且更新成功,返回true,否则返回false.
     * 同时更新此workerPath/workerId的node时间.
     *
     * @param workerId
     * @return
     */
    public boolean updateWorkerNode(String workerId) {
        try {
            String workerPath = config.getWorkersPath() + "/" + workerId;
            Stat stat = new Stat();
            String data = new String(zookeeper.getData(workerPath, null, stat), Config.CHARSET);
            int index = data.lastIndexOf(",");
            String csid = (index == -1) ? data : data.substring(0,index);
            // 如果当前任务执行者不是自己,则返回
            if (!config.getSid().equalsIgnoreCase(csid)) {
                return false;
            }
            //更新workerId的时间
            zookeeper.setData(workerPath, data.getBytes(Config.CHARSET), stat.getVersion());
            return true;
        } catch (Exception e) {
            log.info(">>>>>>>>>>InnerZkClient.updateWorkerNode,workerId:" + workerId, e);
        }
        return false;
    }

    /**
     * 尝试更新当前workerId的信息 如果workerId不被当前server持有且没有过期,则返回false
     * 如果被当前server持有,且更新znode时间成功,返回true
     * 如果不被当前server持有,但是znode已经失去更新(比如workerId的原持有者
     * ,失去链接超时),则尝试更新为自己的sid,如果更新成功返回true,否则返回false 异常,将返回false
     *
     * @param workerId
     * @return
     */
    public boolean touchSelfWorker(String workerId) {
        String workerPath = config.getWorkersPath() + "/" + workerId;
        try {
            Stat stat = new Stat();
            String data = new String(zookeeper.getData(workerPath, null, stat), Config.CHARSET);
            int index = data.lastIndexOf(",");
            String csid = (index == -1) ? data : data.substring(0, index);
            //可能吗?,那就数据校验一下
            if (config.getSid().equalsIgnoreCase(csid)) {
                return true;
            }
            //接管任务的另外一个额外的条件:当前实例稳定时长必须超过sessionTimeout毫秒数
            long _time = System.currentTimeMillis() - config.getMtime();
            if (_time < config.getSessionTimeout()) {
                return false;
            }
            //如果任务属于其他实例,则检测存活
            long mtime = stat.getMtime();
            long currentTime = System.currentTimeMillis();
            // 如果此worker节点已经失去更新,过期时间超过sessionTimeout * 2
            if (currentTime < mtime + 2 * config.getSessionTimeout()) {
                return false;
            }
            data = data.replaceFirst(csid,config.getSid());
            Transaction tx = zookeeper.transaction();
            tx.setData(workerPath, data.getBytes(Config.CHARSET), stat.getVersion());
            tx.commit();
            return true;
        } catch (Exception e) {
            log.info(">>>>>>>>>>InnerZkClient.touchSelfWorker,workerId:" + workerId, e);
        }
        return false;
    }


    public boolean touchBalanceLeader() {
        String balancePath = config.getBalancePath();
        try {
            String data = new String(zookeeper.getData(balancePath, false, null), Config.CHARSET);
            Long lastTime = Long.parseLong(data);
            //时间间隔尚未到期 ,直接返回
            if (lastTime > System.currentTimeMillis() - TimeUnit.HOURS.toMillis(config.getPeriod())) {
                return false;
            }
        } catch (Exception e) {
            return false;
        }

        try {
            String leaderPath = config.getBalancePath() + "/leader";
            Stat stat = zookeeper.exists(leaderPath, false);
            //如果leader节点不存在,则尝试创建
            if (stat == null) {
                this.createBalanceLeaderNode();  //创建成功
                return true;
            }
            String csid = new String(zookeeper.getData(leaderPath, false, null), Config.CHARSET);
            //如果leader是自己
            if (config.getSid().equalsIgnoreCase(csid)) {
                return true;
            }
        } catch (Exception e) {
            log.error(">>>>>>>>>>>touchBalance error,", e);
        }
        return false;
    }


    public String getWorkerNodeData(String workerId) {
        String workerPath = config.getWorkersPath() + "/" + workerId;
        try {
            return new String(zookeeper.getData(workerPath, false, null), Config.CHARSET);
        } catch (Exception e) {
            log.error(">>>>>>>>>>getScheduleNodeData error", e);
        }
        return null;
    }

    public List<String> getWorkersList() throws Exception {
        String workerPath = config.getWorkersPath();
        return zookeeper.getChildren(workerPath, false);
    }

    public List<String> getScheduleList() throws Exception {
        String schedulePath = config.getSchedulePath();
        return zookeeper.getChildren(schedulePath, false);
    }

    public List<String> getServersList() throws Exception {
        String serverPath = config.getServersPath();
        return zookeeper.getChildren(serverPath, false);
    }

    public boolean exist(String workerId) throws Exception {
        String workerPath = config.getWorkersPath() + "/" + workerId;
        Stat stat = zookeeper.exists(workerPath, false);
        if (stat == null) {
            return false;
        }
        return true;
    }

    public boolean balanceWorkerNode(String workerId, String toSid) {
        String workerPath = config.getWorkersPath() + "/" + workerId;
        try {
            String data = new String(zookeeper.getData(workerPath, false, null), Config.CHARSET);
            int index = data.lastIndexOf(",");
            String csid = (index == -1) ? data : data.substring(0, index);
            if (csid.equalsIgnoreCase(toSid)) {
                return true;
            }
            data = data.replaceFirst(csid, toSid);
            zookeeper.setData(workerPath, data.getBytes(Config.CHARSET),-1);
            return true;
        } catch (Exception e) {
            //
            log.error(">>>>>>>>>", e);
        }
        return false;
    }

    public void updateBalanceNode() {
        String balancePath = config.getBalancePath();
        try {
            String lastTime = String.valueOf(System.currentTimeMillis());
            zookeeper.setData(balancePath, lastTime.getBytes(Config.CHARSET), -1);
        } catch (Exception e) {
            //
        }
    }

    /**
     * 取消任务
     *
     * @param workerId
     * @return
     */
    public boolean deleteWorkerNode(String workerId) {
        try {
            Transaction tx = zookeeper.transaction();
            tx.delete(config.getWorkersPath() + "/" + workerId, -1);
            tx.delete(config.getSchedulePath() + "/" + workerId, -1);
            tx.commit();
            return true;
            // 如果节点已经不存在,则字节返回
        } catch (NoNodeException e) {
            log.debug(">>>>>>>>>>>InnerZkClient,deleteWokrerNode,NoNodeException,workerId:" + workerId, e);
            return true;
        } catch (Exception e) {
            log.error(">>>>>>>>>>>InnerZkClient,deleteWokrerNode,workerId:" + workerId, e);
        }
        return false;
    }

    /**
     * @author guanqing-liu 2013年9月13日 检测链接状况,用来检测zk连接是否正常
     *         无论是测试环境,还是正式环境,我们都无法预料到zk cluster是否一直处于正常.
     *         如果不幸,网络异常导致zk断开,我们期望一个自动重连的策略,容错性的忍耐zk故障; 当zk有效后,能够继续服务,而不是需要zk
     *         client应用重新启动.
     */
    class SessionWatcher implements Watcher {

        public void process(WatchedEvent event) {
            // 如果是“数据变更”事件
            if (event.getType() != EventType.None) {
                return;
            }

            // 如果是链接状态迁移
            // 参见keeperState
            synchronized (waiter) {
                config.setMtime(System.currentTimeMillis());
                switch (event.getState()) {
                    // zk连接建立成功,或者重连成功
                    case SyncConnected:
                        log.debug("Connected...");
                        if (!init) {
                            init = true;
                        }
                        waiter.notifyAll();
                        break;
                    // session过期,这是个非常严重的问题,有可能client端出现了问题,也有可能zk环境故障
                    // 此处仅仅是重新实例化zk client
                    case Expired:
                        log.debug("Expired...");
                        // 重连
                        connectZK();
                        break;
                    // session过期
                    case Disconnected:
                        // 链接断开，或session迁移
                        log.debug("Connecting....");
                        break;
                    case AuthFailed:
                        close();
                        throw new RuntimeException("ZK Connection auth failed...");
                    default:
                        break;
                }
            }
        }
    }

}

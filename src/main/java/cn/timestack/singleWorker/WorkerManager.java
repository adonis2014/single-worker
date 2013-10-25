package cn.timestack.singleWorker;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.log4j.Logger;
import org.apache.zookeeper.ZooKeeper.States;
import org.quartz.CronScheduleBuilder;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.TriggerKey;
import org.quartz.impl.StdSchedulerFactory;

import cn.timestack.singleWorker.object.CallerJobProxy;
import cn.timestack.singleWorker.object.Config;
import cn.timestack.singleWorker.object.WorkerJob;

/**
 * @author guanqing-liu
 *         2013年9月17日
 *         任务调度器,用于管理集群中任务的提交.
 */
public class WorkerManager {

    private static final String GROUP = "single-worker";

    private static Logger log = Logger.getLogger(WorkerManager.class);

    private InnerZkClient zkClient;

    //当前application中调度成功的worker列表
    private Set<String> selfWorkers = new HashSet<String>();

    //当前application中所有的worker列表,key为workerId
    private Map<String, WorkerJob> allWorkers = new HashMap<String, WorkerJob>();

    private Scheduler scheduler;

    private Config config = Config.getInstance();

    //任务同步线程,定时将zk环境中的任务与本地同步
    private Thread syncThread = new SyncHandler();


    public WorkerManager() throws Exception {
        zkClient = new InnerZkClient();// 同步操作,建立zk连接
        scheduler = StdSchedulerFactory.getDefaultScheduler();
        scheduler.start();
        syncThread.setDaemon(true);
        syncThread.start();
    }

    public void close() {
        try {
            selfWorkers.clear();
            allWorkers.clear();
            scheduler.shutdown();
        } catch (Exception e) {
            log.error(e);
        }
        try {
            syncThread.interrupt();
            zkClient.close();
        } catch (Exception e) {
            log.error(">>>>>>>>>>WorkerManager,close.", e);
        }
    }

    /**
     * workerCaller方式
     *
     * @param clientWorkerId
     * @param cronExpression
     * @return
     */
    public boolean schedule(String clientWorkerId, String cronExpression) {
        if (!zkClient.isReady()) {
            log.error(">>>>>>>>>>WorkerManager,zk is disconnected,worker schedule failure...");
            return false;
        }
        WorkerJob worker = this.buildWorker(clientWorkerId, cronExpression);
        String workerId = worker.getWorkerId();
        try {
            //生成zk数据节点
            //任务调度,有syncThread统一管理
            boolean isSuccess = zkClient.createWorkerNode(workerId, cronExpression);
            if (isSuccess) {
                //任务信息在本地保存
                allWorkers.put(workerId, worker);
            }
            return isSuccess;
            //wait for syncThread to schedule it.
        } catch (Exception e) {
            log.error(">>>>>>>>>>WorkerManager,schedule failure:" + workerId, e);
            return false;
        }
    }


    /**
     * quartz模式
     *
     * @param jobDetail
     * @param trigger
     * @return
     */
    public boolean schedule(JobDetail jobDetail, Trigger trigger) {
        if (!zkClient.isReady()) {
            log.error(">>>>>>>>>>WorkerManager,zk is disconnected,worker schedule failure...");
            return false;
        }
        //
        WorkerJob worker = new WorkerJob(jobDetail, trigger);
        String workerId = worker.getWorkerId();
        try {
            //生成zk数据节点
            //任务调度,有syncThread统一管理
            boolean isSuccess = zkClient.createWorkerNode(workerId, null);
            if (isSuccess) {
                allWorkers.put(workerId, worker);
                //wait for syncThread to schedule it.
            }
            return isSuccess;
        } catch (Exception e) {
            log.error(">>>>>>>>>>WorkerManager,schedule failure:" + workerId, e);
            return false;
        }
    }

    private WorkerJob buildWorker(String clientWorkerId, String cronExpression) {
        //build jobDetail and trigger
        CronScheduleBuilder sb = CronScheduleBuilder.cronSchedule(cronExpression);
        Trigger trigger = TriggerBuilder.newTrigger().withIdentity(clientWorkerId, GROUP).withSchedule(sb).build();
        Class<CallerJobProxy> jobClass = CallerJobProxy.class;
        JobDetail job = JobBuilder.newJob(jobClass).withIdentity(clientWorkerId, GROUP).build();
        //通过workerCaller回调方式的任务,需要这么做
        job.getJobDataMap().put(Utils.WORKER_ID, clientWorkerId);
        job.getJobDataMap().put(Utils.WORKER_CALLER, config.getCaller());
        //for schedule
        return new WorkerJob(job, trigger);
    }

    /**
     * workerCaller 模式
     *
     * @param clientWorkerId
     * @return
     */
    public boolean unSchedule(String clientWorkerId) {
        //原始的id,转换成一下
        TriggerKey tk = new TriggerKey(clientWorkerId, GROUP);
        return this.unSchedule(tk);
    }

    /**
     * quartz模式
     *
     * @param triggerKey
     * @return
     */
    public boolean unSchedule(TriggerKey triggerKey) {
        // 删除远端zk任务节点
        String workerId = WorkerJob.id(triggerKey);
        boolean isSuccess = zkClient.deleteWorkerNode(workerId);
        if (!isSuccess) {
            return false;
        }
        //allWorkers.remove(workerId); //not delete the stub of local
        //如果任务非当前server执行
        if (selfWorkers.contains(workerId)) {
            WorkerJob worker = allWorkers.get(workerId);
            this.localUnschedule(worker);
        }
        return true;
    }

    private boolean localUnschedule(WorkerJob worker) {
        if (worker == null) {
            return false;
        }
        TriggerKey triggerKey = worker.getTrigger().getKey();
        try {
            if (scheduler.checkExists(triggerKey)) {
                scheduler.unscheduleJob(triggerKey);
            }
            return true;
        } catch (Exception e) {
            log.error(">>>>>>>>>>WokrerManager localUnschedule,workerId:" + WorkerJob.id(triggerKey), e);
        }
        return false;
    }


    private boolean localSchedule(WorkerJob worker) {
        if (worker == null) {
            return false;
        }
        try {
            Trigger trigger = worker.getTrigger();
            if (scheduler.checkExists(trigger.getKey())) {
                return true;
            }
            scheduler.scheduleJob(worker.getJobDetail(), trigger);
            // 任务调度列表
            return true;
        } catch (Exception e) {
            log.error(">>>>>>>>>>WokrerManager,localSchedule,workerId:" + worker.getWorkerId(), e);
        }
        return false;
    }

    /**
     * 同步selfWorkers列表，和zk环境中的列表进行比较，查看是否有任务冲突
     */
    protected void syncSelfWorkers() throws Exception {
        // 首先检测自己持有的任务列表，是否和zk一致，首次同步，selfWorkers肯定是空，需要sync后续去做调度。
        Iterator<String> it = selfWorkers.iterator();
        while (it.hasNext()) {
            String workerId = it.next();
            // 如果此任务已经被远程取消,则取消本地job执行
            // 所有的实例都会做同样的事情，一定会把那些“取消的任务”取消
            // 如果任务的执行者是自己,则更新node时间
            //任务是否在远端被取消
            boolean existWorker = zkClient.exist(workerId);
            //任务是否为自己执行
            boolean isSelfWorker = zkClient.updateWorkerNode(workerId);
            //如果任务已经被其它实例接管,则取消本地任务
            //如果此任务已经被远端取消,如果某种异常,导致本地任务还没有取消,则尝试取消
            WorkerJob worker = allWorkers.get(workerId);
            if (!existWorker || !isSelfWorker) {
                it.remove();
                this.localUnschedule(worker);
                continue;
            }
            //如果zk中节点属于自己,那么开始检测此任务是否在本地被正确调度
            this.localSchedule(worker);
        }
    }

    /**
     * 检测是否有调度失败的任务,这些任务尚未被任何server接管.
     *
     * @throws Exception
     */
    protected void syncAllWorkers() throws Exception {
        //remote端任务列表
        List<String> workers = zkClient.getWorkersList();
        if (workers == null || workers.isEmpty()) {
            return;
        }
        for (String workerId : workers) {
            //如果此认为已经被自己调度,则直接返回
            //具体任务是否真的属于自己接管,则有syncSelfWorkers方法检测
            if (selfWorkers.contains(workerId)) {
                continue;
            }
            // 如果当前server没有持有worker的实例信息,则重新构建
            if (!allWorkers.containsKey(workerId)) {
                String cronExpression = zkClient.getWorkerCronExpression(workerId);
                // 这类worker通常为workerCaller类型的,
                // 对于quartz类型,如果本实例中(allWorkers)不包含,则会在本机忽略,因为这种
                // 类型的任务,跨VM是有很大问题的
                if (cronExpression != null) {
                    TriggerKey tk = WorkerJob.triggerKey(workerId);
                    WorkerJob worker = this.buildWorker(tk.getName(), cronExpression);
                    allWorkers.put(workerId, worker);
                }
            }
            WorkerJob worker = allWorkers.get(workerId);
            //如果任务未能获取,比如其他JVM实例动态提交的quartz-job,导致不能跨VM
            //目前还没有一个完美的办法来协调这个特性
            if (worker == null) {
                continue;
            }
            //此worker不属于自己执行,但是我们需要检测,这个worker是否有其他server正在接管
            //如果此worker处于"游离"状态,那么当前server就需要尽力接管它.
            if (zkClient.touchSelfWorker(workerId)) {
                //接管成功
                selfWorkers.add(workerId);
                //下一次syncSelf时调度
            }
        }
        rebalance();
    }

    protected void rebalance() {
        //如果没有启用"balance",则返回
        if (!config.isReBalance()) {
            return;
        }
        //如果当前实例稳定时间少于 2 * sessionTimeout,也不能进行balance
        //网络不稳定的情况下,会对任务调度有很大干扰
        long _time = System.currentTimeMillis() - config.getMtime();
        if (_time < 2 * config.getSessionTimeout()) {
            return;
        }
        //检测balance leader是否为自己
        if (!zkClient.touchBalanceLeader()) {
            return;
        }
        try {
            log.info(">>>>>>>>>>>relance begin.....");
            List<String> workers = zkClient.getWorkersList();
            if (workers.isEmpty()) {
                return;
            }
            //key为sid,value为当前sid下的所有任务列表
            Map<String, List<String>> workersMap = new HashMap<String, List<String>>();
            List<String> servers = zkClient.getServersList();//所有存活的server列表
            for (String sid : servers) {
                workersMap.put(sid, new ArrayList<String>());
            }
            for (String id : workers) {
                String d = zkClient.getWorkerNodeData(id);
                if (d == null) {
                    continue;
                }
                int i = d.lastIndexOf(",");  //index
                String s = (i == -1) ? d : d.substring(0, i); //sid
                List<String> wl = workersMap.get(s);

                if (wl == null) {
                    wl = new ArrayList<String>();
                    workersMap.put(s, wl);
                }
                wl.add(id);
            }
            //根据每个sid上的worker个数,整理成一个排序的map
            TreeMap<Integer, List<String>> counterMap = new TreeMap<Integer, List<String>>();
            for (Map.Entry<String, List<String>> entry : workersMap.entrySet()) {
                int total = entry.getValue().size();
                List<String> sl = workersMap.get(total);
                if (sl == null) {
                    sl = new ArrayList<String>();
                    counterMap.put(total, sl);
                }
                sl.add(entry.getKey());//sid
            }
            int totalWorkers = workers.size();
            int totalServers = workersMap.keySet().size();
            int avg = totalWorkers / totalServers;//每个server实例可以接管任务的平均数
            Map<String, String> finalMap = new HashMap<String, String>();//最终计算的map,key为workerId,value为需要接管的sid
            while (true) {
                Map.Entry<Integer, List<String>> gt = counterMap.higherEntry(avg);  //大于平均数的列表, >= avg + 1
                Map.Entry<Integer, List<String>> lt = counterMap.lowerEntry(avg); //与平均数差值为2的 <= arg  - 1
                //允许任务个数与avg上线浮动1各个,不是绝对的平均

                if (gt == null || lt == null) {
                    break;
                }
                Integer gtKey = gt.getKey();
                Integer ltKey = lt.getKey();
                if (gt.getKey() - lt.getKey() < 2) {
                    break;
                }
                if (gt.getValue().size() == 0) {
                    counterMap.remove(gt.getKey());
                }
                if (lt.getValue().size() == 0) {
                    counterMap.remove(lt.getKey());
                }
                Iterator<String> it = gt.getValue().iterator(); //sid列表
                while (it.hasNext()) {
                    String _fromSid = it.next();
                    List<String> _currentWorkers = workersMap.get(_fromSid);
                    if (_currentWorkers == null || _currentWorkers.isEmpty()) {
                        it.remove();
                        workersMap.remove(_fromSid);
                        continue;
                    }
                    List<String> _ltServers = lt.getValue();
                    if (_ltServers.isEmpty()) {
                        counterMap.remove(ltKey);
                        break;
                    }
                    String _wid = _currentWorkers.get(0); //取出需要交换出去的任务id
                    String _toSid = _ltServers.get(0);
                    //
                    //本地allWorkers列表中没有,有可能这个任务为quartz-job
                    if (config.getSid().equalsIgnoreCase(_toSid) && !allWorkers.containsKey(_wid)) {
                        continue;
                    }
                    //从_fromSid的worker列表中移除低workerId
                    _currentWorkers.remove(0);
                    it.remove();
                    _ltServers.remove(0);
                    //将此workerId添加到_toSid的worker列表中
                    List<String> _ltWorkers = workersMap.get(_toSid);
                    if (_ltWorkers == null) {
                        _ltWorkers = new ArrayList<String>();
                        workersMap.put(_toSid, _ltWorkers);
                    }
                    _ltWorkers.add(_wid);
                    //将gt的key降低一个数字
                    List<String> _next = counterMap.get(gtKey - 1);
                    if (_next == null) {
                        _next = new ArrayList<String>();
                        counterMap.put(gtKey - 1, _next);
                    }
                    _next.add(_fromSid);
                    //将lt的key提升一个数字
                    List<String> _prev = counterMap.get(ltKey + 1);
                    //从lt的countMap中移除,因为它将被放置在key + 1的新位置
                    Iterator<String> _ltIt = _ltServers.iterator();
                    while (_ltIt.hasNext()) {
                        if (_ltIt.next().equalsIgnoreCase(_toSid)) {
                            _ltIt.remove();
                            break;
                        }
                    }
                    if (_prev == null) {
                        _prev = new ArrayList<String>();
                        counterMap.put(ltKey + 1, _prev);
                    }
                    _prev.add(_toSid);
                    finalMap.put(_wid, _toSid);
                }
            }
            //同步任务数据
            int successTotal = 0;
            for (Map.Entry<String, String> entry : finalMap.entrySet()) {
                String wid = entry.getKey();
                String toSide = entry.getValue();
                //如果任务迁移给自己,但是自己的allWorkers中却没有,则直接忽略
                if (config.getSid().equalsIgnoreCase(toSide) && !allWorkers.containsKey(wid)) {
                    continue;
                }
                //如果从自己的任务中迁移给其他实例,则首先把在本地取消任务执行
                if (selfWorkers.contains(wid)) {
                    selfWorkers.remove(wid);
                    WorkerJob worker = allWorkers.get(wid);
                    this.localUnschedule(worker);
                }
                boolean isSuccess = zkClient.balanceWorkerNode(wid, entry.getValue());
                if(isSuccess){
                    successTotal++;
                }
            }
            //至少一半的任务正确被reblance,才能确认此次balance为成功的
            if(2 * successTotal >= finalMap.size()) {
                //更新最后"balance"时间
                zkClient.updateBalanceNode();
            }
            log.info(">>>>>>>>>>>relance end.....");
        } catch (Exception e) {
            log.error(">>>>>>>>>>>>rebalance error", e);
        }
    }


    /**
     * 同步任务信息，将当前实例中scheduler运行的任务和zk进行比较，进行冲突检测。 1)
     * 检测自己正在运行的任务，是否和zk中心中分配给自己的任务列表一致。 2) 获得当前serverType下所有的任务列表
     */
    protected void sync() {
        try {
            if (!zkClient.isReady()) {
                throw new RuntimeException("Scheduler error..");
            }

            // +++++++++++++++++++
            // 检测自己本地正在调度的任务,是否和zk中保持同步
            syncSelfWorkers();
            // +++++++++++++++++++

            // +++++++++++++++++++
            // 检测zk环境中,所有的任务,是否状态正常
            syncAllWorkers();
            // +++++++++++++++++++
            // 获得所有任务列表
            //rebalance

        } catch (Exception e) {
            log.error(">>>>>>>>>>>WorkerManager,sync", e);
        }
    }

    // /core helper threads------very important-----------//
    /*
     * 用于同步任务,间歇性执行,本地任务和zk数据进行对比;如果zk中节点被删除,如果本地的任务还在执行,就应该被删除.
	 * 如果zk中有任务节点尚没有被分配,那么此时就应该接管任务.
	 * 之所以有同步线程,是因为zk的网络异常(比如网络断开),在重新连接之后,可能会导致期间的数据变更事件丢失. 因为需要额外的线程来检测数据变动.
	 */
    class SyncHandler extends Thread {

        private Object waiter = config.waiter();

        @Override
        public void run() {
            try {
                while (true) {
                    log.debug(">>>>>>>>>>Sync handler,running...tid: " + Thread.currentThread().getId());
                    //如果zk已经被关闭,直接退出
                    if (!zkClient.isAlive()) {
                        break;
                    }
                    synchronized (waiter) {
                        States states = zkClient.getZkStates();
                        //如果client尚未就绪,则阻塞
                        if (states == null || !zkClient.isReady()) {
                            waiter.wait();
                            continue;
                        }
                    }
                    //如果一切正常
                    sync();
                    // 如果被“中断”，直接退出
                    Thread.sleep(config.getSessionTimeout() / 3);
                }
            } catch (InterruptedException e) {
                log.error(">>>>>>>SyncHandler Exit...");
            }

        }
    }

}

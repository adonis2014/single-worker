package cn.timestack.singleWorker.object;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooDefs.Perms;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;

import cn.timestack.singleWorker.Utils;

/**
 * @author guanqing-liu
 * 2013年9月17日
 * zookeeper客户端的配置总览
 */
public class Config {

    private static Config instance;

    public static final String CHARSET = "UTF-8";

    private WorkerCaller caller;

    private Object waiter = new Object();

    public static final String PREFIX = "/workerSchedule";
    private String name;

    private String sid;

    private List<ACL> acls;

    private boolean reBalance;

    private String namespace;
    private Integer sessionTimeout;// zk-client sessionTimeout

    private String connectString;// zk connectString

    private String namespaceAuth;// 命名空间的授权信息,用户名:密码

    private String digest;// 经过digest计算之后的namespaceAuth

    //	/workerSchedule/${name}/servers
    private String serversPath;

    //	/workerSchedule/${name}/workers
    private String workersPath;

    //	/workerSchedule/${name}/schedule
    private String schedulePath;

    //用来标记当前实例zk服务的稳定时间,mtime表示与zk服务的最后一次链接失效时间戳
    //用来表示当前server的稳定度,mtime越小,表示当前实例越可靠
    private long mtime;

    private int period;

    private byte[] sidBytes;

    private Config(){}
    public String getName() {
        return name;
    }

    private String balancePath;
    public void setName(String name) {
        this.name = name;
        this.namespace = PREFIX + "/" + name;
        this.workersPath = namespace + "/workers";
        this.schedulePath = namespace + "/schedule";
        this.serversPath =  namespace + "/servers";
        this.balancePath = namespace + "/balance";
    }

    public String getNamespace() {
        return namespace;
    }

    public Integer getSessionTimeout() {
        return sessionTimeout;
    }

    public void setSessionTimeout(Integer sessionTimeout) {
        this.sessionTimeout = sessionTimeout;
    }

    public String getBalancePath() {
        return balancePath;
    }

    public String getConnectString() {
        return connectString;
    }

    public void setConnectString(String connectString) {
        this.connectString = connectString;
    }

    public String getNamespaceAuth() {
        return namespaceAuth;
    }

    public void setNamespaceAuth(String namespaceAuth) {
        this.namespaceAuth = namespaceAuth;
    }

    public String getDigest() {
        return digest;
    }

    public int getPeriod() {
        return period;
    }

    public void setPeriod(int period) {
        this.period = period;
    }

    public void setDigest(String digest) {
        this.digest = digest;
        // 构建ACL访问列表
        if(StringUtils.isBlank(digest) || StringUtils.isBlank(namespaceAuth)){
            acls = ZooDefs.Ids.OPEN_ACL_UNSAFE;
        }else{
            Id id = new Id("digest", digest);
            ACL acl = new ACL(Perms.ALL, id);
            acls = new ArrayList<ACL>(Collections.singletonList(acl));
        }
    }

    public boolean isReBalance() {
        return reBalance;
    }

    public void setReBalance(boolean reBalance) {
        this.reBalance = reBalance;
    }

    public List<ACL> getAcls() {
        return acls;
    }

    public String getSid() {
        return sid;
    }

    public void setSid(String sid) {
        this.sid = sid;
        try{
            sidBytes = sid.getBytes(CHARSET);
        }catch(Exception e){
            //
        }
    }

    public WorkerCaller getCaller() {
        return caller;
    }

    public void setCaller(WorkerCaller caller) {
        this.caller = caller;
    }

    public Object waiter(){
        return this.waiter;
    }

    public String getServersPath() {
        return serversPath;
    }

    public String getWorkersPath() {
        return workersPath;
    }

    public String getSchedulePath() {
        return schedulePath;
    }

    public byte[] getSidBytes(){
        return sidBytes;
    }

    public static Config getInstance(){
        return instance;
    }

    public long getMtime() {
        return mtime;
    }

    public void setMtime(long mtime) {
        this.mtime = mtime;
    }

    public synchronized static Config build(Properties properties) throws Exception{
        instance = new Config();

        if (properties == null || properties.isEmpty()) {
            throw new RuntimeException(">>>>>>>>>>WorkerScheduler,buildConfig,Please specify single-worker config..");
        }
        // connectString:适用于连接zk
        String connectString = properties.getProperty("singleWorker.zookeeper.connectString");
        if (StringUtils.isBlank(connectString)) {
            throw new RuntimeException(">>>>>>>>>WorkerScheduler,buildConfig,'singleWorker.zookeeper.connectString' should not be empty!!");
        }
        // name:适用于标记当前应用类型
        String name = properties.getProperty("singleWorker.name");
        if (StringUtils.isBlank(name)) {
            throw new RuntimeException(">>>>>>>>>>WorkerScheduler,buildConfig,'singleWorker.name' must not be null..");
        }

        if (!Pattern.matches(Utils.REGEX, name)) {
            throw new RuntimeException(">>>>>>>>>>WorkerScheduler,buildConfig,'zk.singleWorker.name' format error:a-Z,0-9,or '_''-' is accepted!");
        }
        String namespaceAuth = properties.getProperty("singleWorker.zookeeper.namespace.auth");
        String digest = DigestAuthenticationProvider.generateDigest(namespaceAuth);
        String timeout = properties.getProperty("singleWorker.zookeeper.sessionTimeout", "-1");
        Integer sessionTimeout = Math.abs(Integer.parseInt(timeout));
        String balance = properties.getProperty("singleWorker.cluster.rebalance","true");
        String balancePeriod = properties.getProperty("singleWorker.cluster.rebalance.period","24");
        //
        instance.setConnectString(connectString);
        instance.setName(name);
        instance.setNamespaceAuth(namespaceAuth);
        instance.setDigest(digest);
        instance.setSessionTimeout(sessionTimeout);
        instance.setReBalance(Boolean.valueOf(balance));
        instance.setMtime(System.currentTimeMillis());
        //一个config实例都将有一个sid,而且在整个生命周期中不会改变
        String id = RandomStringUtils.randomAlphabetic(6) + "-" + Utils.getLocalAddress();
        instance.setSid(id);
        instance.setPeriod(Integer.parseInt(balancePeriod));
        return instance;
    }

}
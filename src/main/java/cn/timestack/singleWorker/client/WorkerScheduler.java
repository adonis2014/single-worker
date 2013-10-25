package cn.timestack.singleWorker.client;

import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.quartz.JobDetail;
import org.quartz.Trigger;
import org.quartz.TriggerKey;
import org.springframework.beans.factory.InitializingBean;

import cn.timestack.singleWorker.Utils;
import cn.timestack.singleWorker.WorkerManager;
import cn.timestack.singleWorker.object.Config;
import cn.timestack.singleWorker.object.WorkerCaller;
import cn.timestack.singleWorker.object.WorkerJob;

public class WorkerScheduler implements InitializingBean{

	private WorkerCaller caller;//任务调度器

	private Properties config;//

    private boolean inited = false;

    private WorkerManager workerManager;


	public void setCaller(WorkerCaller caller) {
		this.caller = caller;
	}

    public void setConfig(Properties config) {
        this.config = config;
    }

    @Override
	public void afterPropertiesSet() throws Exception {
		if (inited) {
			return;
		}
		Config innerConfig = Config.build(config);
        innerConfig.setCaller(caller);
		workerManager = new WorkerManager();
        inited = true;
	}

	public void close(){
		workerManager.close();
	}

    /**
     * 提交任务,workerId为任务的关键词,workerId + cronExpression的方式需要和WorkerCaller一起配合使用
     * 当cron到达触发时机时,将会执行workerCaller.call(workerId)方法
     * @param clientWorkerId
     * @param cronExpression
     * @return 如果zk接受成功,则返回true,否则返回false
     */
	public boolean schedule(String clientWorkerId,String cronExpression) {
        if (StringUtils.isBlank(clientWorkerId) || StringUtils.isBlank(cronExpression)) {
            throw new NullPointerException(">>>>>>>>>>WorkerSchedule,schedule,workerId/cronExpression cant be null!!");
        }
        if (!Pattern.matches(Utils.REGEX, clientWorkerId)) {
            throw new RuntimeException(">>>>>>>>>>WorkerSchedule,schedule,workerId format error:a-Z,0-9,or '_''-' is accepted!");
        }
        return workerManager.schedule(clientWorkerId,cronExpression);
	}

    /**
     * 提交quartz-job类型的任务,如果任务接受成功,则返回true
     * @param jobDetail
     * @param trigger
     * @return
     */
    public boolean schedule(JobDetail jobDetail,Trigger trigger) {
        if (jobDetail == null || trigger == null) {
            throw new NullPointerException(">>>>>>>>>>WorkerSchedule,schedule,'jobDetail' org 'trigger' cant be null!!");
        }
        String workerId = WorkerJob.id(trigger.getKey());
        if (!Pattern.matches(Utils.REGEX, workerId)) {
            throw new RuntimeException(">>>>>>>>>>WorkerSchedule,schedule,workerId format error:a-Z,0-9,or '_''-' is accepted!");
        }
        return workerManager.schedule(jobDetail,trigger);
    }

    /**
     * 取消调度,
     * @param clientWorkerId
     * @return 如果zk数据操作正常,则返回true,否则返回false
     */
	public boolean unSchedule(String clientWorkerId) {
		if (StringUtils.isBlank(clientWorkerId)) {
			return false;
		}
		return workerManager.unSchedule(clientWorkerId);
	}

    /**
     * 取消quartz-job类型的任务
     * @param triggerKey
     * @return
     */
    public boolean unSchedule(TriggerKey triggerKey){
        if(triggerKey == null){
            return false;
        }
        return workerManager.unSchedule(triggerKey);
    }

}

package cn.timestack.singleWorker.object;

import org.quartz.JobDetail;
import org.quartz.Trigger;
import org.quartz.TriggerKey;

/**
 * keep jobDetail and trigger
 * User: guanqing-liu
 * Date: 13-10-14
 * Time: 下午2:01
 */
public class WorkerJob {
    private Trigger trigger;

    private JobDetail jobDetail;

    public WorkerJob(JobDetail jobDetail,Trigger trigger){
        this.jobDetail = jobDetail;
        this.trigger = trigger;
    }

    public Trigger getTrigger() {
        return trigger;
    }

    public void setTrigger(Trigger trigger) {
        this.trigger = trigger;
    }

    public JobDetail getJobDetail() {
        return jobDetail;
    }

    public void setJobDetail(JobDetail jobDetail) {
        this.jobDetail = jobDetail;
    }

    public String getWorkerId(){
        return id(trigger.getKey());
    }

    public static String id(TriggerKey tk){
        return tk.toString();
    }

    public static TriggerKey triggerKey(String workerId){
        int index = workerId.indexOf(".");
        if(index == -1){
             return new TriggerKey(workerId);
        }
        String group = workerId.substring(0,index);
        String name = workerId.substring(index + 1);
        return new TriggerKey(name,group);
    }

}

package cn.timestack.singleWorker.object;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import cn.timestack.singleWorker.Utils;

/**
 * @author guanqing-liu
 * 2013年9月17日
 * 内部执行类,为Worker与quartz job的桥梁
 */
public class CallerJobProxy implements Job {

	private static Logger log = Logger.getLogger(CallerJobProxy.class);
	
	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {
		
		WorkerCaller caller = (WorkerCaller)context.getJobDetail().getJobDataMap().get(Utils.WORKER_CALLER);
		if(caller == null){
            log.error("workerCaller is null,Worker cant be executed...");
			return;
		}
		String workerId = (String)context.getJobDetail().getJobDataMap().get(Utils.WORKER_ID);
		if(StringUtils.isBlank(workerId)){
			return;
		}
		try{
			log.info(">>>>>>>>>>CallerJobProxy begin:" + workerId);
			caller.call(workerId);
			log.info(">>>>>>>>>>CallerJobProxy success:" + workerId);
		}catch(Exception e){
			log.error(">>>>>>>>>>CallerJobProxy success:" + workerId,e);
		}

	}

}

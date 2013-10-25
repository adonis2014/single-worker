package cn.timestack.singleWorker.object;

/**
 * @author guanqing-liu
 * 2013年9月17日
 * 任务构造器,因为任务将会以分布式的方式分发到多太机器上,任务的workerId有zk保留
 * 但是任务的实际执行类实例,将通过Caller在本机构建.
 */
public interface WorkerCaller {

	public void call(String workerId);
	
}

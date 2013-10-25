package cn.timestack.singleWorker;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;

public class Utils {

    public static final String WORKER_CALLER = "_WORKER-CALLER_";

    public static final String WORKER_ID = "_WORKER-ID_";

    ////workerId,因为workerId作为了zk znodepath的一部分,所以,对workerId的格式有严格要求
    //"/"":"" "等均会被认为是非法的,此处强制要求workerId必须是readable简字符
    public static final String REGEX = "([A-z]|[0-9]|-|_)*";


    /**
     * 获取本机的本地IP:--siteLocalAddress
     *
     * @return
     * @throws Exception
     */
    public static String getLocalAddress() throws Exception {
        Enumeration<NetworkInterface> netInterfaces = NetworkInterface.getNetworkInterfaces();// 一个主机有多个网络接口
        while (netInterfaces.hasMoreElements()) {
            NetworkInterface netInterface = netInterfaces.nextElement();
            Enumeration<InetAddress> addresses = netInterface.getInetAddresses();// 每个网络接口,都会有多个"网络地址",比如一定会有lookback地址,会有siteLocal地址等.以及IPV4或者IPV6
            // .
            while (addresses.hasMoreElements()) {
                InetAddress address = addresses.nextElement();
                if (address.isSiteLocalAddress() && !address.isLoopbackAddress()) {
                    return address.getHostAddress();
                }
            }
        }
        return null;
    }

    /**
     * 获取当前JVM进程的ID
     *
     * @return
     */
    public static Integer getProcessId() {
        RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
        String process = runtime.getName();//10010@locahost
        if (process == null) {
            return -1;
        }
        return Integer.parseInt(process.substring(0, process.indexOf("@")));
    }

}

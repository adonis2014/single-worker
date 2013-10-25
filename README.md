single-worker
=============

singleWorker
在系统中,一个应用可能有多个cronJob在运行,但是由于分布式部署的原因,我们可能期望一个job只能在一台server上运行,那么此时singleWorker就能排上用场. 1) 任务单点运行 2) 任务在多个部署实例上均匀分布 3) 部署实例失效后,任务被其他实例自动接管.

singleWorker本身使用了zookeeper和quartz结合.能够满足大部分要求.支持spring环境下使用.

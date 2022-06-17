# distributed-system

# lab1 Mapreduce
## 主要框架
`Coordinator:`  
* Handle
  * Workder通过rpc调用Handle函数，分发Task
  * 负责Worker执行任务的状态切换
  * 返回Reply
 
`Worker`  
>通过**plugin**加载**mapf**和**reducef**
* MapTask
  * 执行mapf 
* RecudeTask
  * 执行reducef
 
`rpc`
* Reply
  - Type
  - Filename
  - Mapindex
  - Reduceindex
  - Stamp 分布式唯一标识符  
 
`命名格式`
* **stamp-mr-mapindex-rindex**

`Type状态机`
* map
* reduce
* wait
* exit

`具体执行流程`  
1. Worker循环发送RPC请求
2. Corrdinator分配Worker任务（Mapindex,Reduceindex,Type）
3. Worker执行结束
## data race
- coordinate.state 产生的数据竞争（上锁）  
- 部分测试失败!原因：输出的格式不对！  
## Result
![结果](https://github.com/MingweiGuo/DistributeSystem/blob/main/picture/lab1_test.png)

## Todo
**使用channel进行通信**  
**goroutine**

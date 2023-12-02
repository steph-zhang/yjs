# yjs
所有jar包可在对应module的target目录中获取。

## FilerWatcher
jar包，用于监听是否有新数据被爬取，将新数据输入Kafka的某个topic。
arg0:kafka topic名
arg1:监听路径

```java -jar FileWatcher-1.0-SNAPSHOT.jar arg0 arg1```

## FilerWriter
jar包，用于监听Kafka的某个topic是否有新消息，并将新消息写入文件。
arg0:kafka topic名
arg1:写入文件全路径名

```java -jar FileWriter-1.0-SNAPSHOT.jar arg0 arg1```

## Flink

依赖：

zookeeper3.4.13  配置在2181端口<br> 
kafka2.1.1 配置在9092端口<br>

Flink集群配置了三个节点master，worker1，worker2，每个节点中有一个slot。在启动集群后，浏览器打开 master:8081 进入flink dashboard提交任务。

任务jar包：<br>
ciyun-1.0-SNAPSHOT.jar<br>
    任务入口：
    
    # 计算北京地区Python岗位的描述关键词词频
    com.zmy.CiyunJob 

flink_python-1.0-SNAPSHOT.jar<br>
    任务入口：
    
    # 计算北京各地区Python岗位的数量
    com.zmy.PythonAreaJob 
    # 计算北京地区Python岗位的学历要求
    com.zmy.PythonDegreeJob 



salary-1.0-SNAPSHOT.jar<br>
    任务入口：
    
    # 计算北京地区Python岗位按地区分组计算平均工资
    com.zmy.SalaryJob 


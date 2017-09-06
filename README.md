## 实时报表

#### Compile: 
> #####  sbt clean assembly

#### Run:
> <pre> 
> path/to/spark/bin/spark-submit \
>   --executor-memory Xg \
>   --driver-memory Xg
>   --total-executor-cores X \
>   --executor-cores X \
>   --deploy-mode cluster \
>   --master &lt;master&gt; \
>   --class com.madhouse.ssp.ReportRT \
>   hdfs://XX/path/to/&lt;application&gt;.jar \
>   hdfs://XX/path/to/&lt;application&gt.conf
> </pre>

#### Config:
> <pre> 
> app {
>   spark {
>     master = "local[*]"
>     streaming {
>       starting_offsets = "earliest"
>       max_offsets_per_trigger = "10240"
>       trigger_processing_time_ms = 30000
>     }
>   }
> 
>   kafka {
>     bootstrap_servers = "10.10.16.25.27:9092,10.10.16.25.28:9092,10.10.16.25.29:9092"
>     topic_name = "topic_mediabid"
>   }
> 
>   mysql {
>     url = "jdbc:mysql://172.16.25.26:3306/premiummad_dev?useUnicode=true&characterEncoding=utf8&autoReconnect=true"
>     user = "root"
>     pwd = "tomcat2008"
> 
>     dest_table_name = "mad_report_media_mem"
>     batch_size = 16
>   }
> 
>   log_type = "MEDIABID"  // eg. MEDIABID, DSPBID, IMPRESSION, CLICK, WINNOTICE
> }
> </pre>
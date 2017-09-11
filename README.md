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
>   hdfs://XX/path/to/&lt;application&gt;.conf
> </pre>

#### Config:
> <pre> 
> app {
>   spark {
>     streaming {
>       starting_offsets = "@starting_offsets@"
>       max_offsets_per_trigger = "@max_offsets_per_trigger@"
>       trigger_processing_time_ms = @trigger_processing_time_ms@
>     }
>   }
> 
>   kafka {
>     bootstrap_servers = "@kafka.bootstrap_servers@"
>     topic_name = "@kafka.topic_name@"
>   }
> 
>   mysql {
>     url = "@mysql.url@"
>     user = "@mysql.user@"
>     pwd = "@mysql.pwd@"
> 
>     dest_table_name = "@mysql.dest_table_name@"
>     batch_size = @mysql.batch_size@
>   }
> 
>   log_type = "@log_type@"  // eg. MEDIABID, DSPBID, IMPRESSION, CLICK, WINNOTICE
> }
> </pre>
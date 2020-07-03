# checksumdb_mysql

1.适用于数据迁移后，数据级别的迁移比对  
注：比对时，2个库之间，不要发生数据变更，避免数据比对不准。   
注：根据实际host,port,user,password,db修改py文件
注：不要使用在主从同步环境中，会造成主从同步冲突，因为要创建相关中间表

![image](images/checkdb.jpg)

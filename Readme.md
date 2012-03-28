

#Subject
获取文章与股票和股票板块相关行的服务
##准备工作
###1. 需要安装git
###2. 安装scws
####下载地址：http://ftphp.com/scws/download.php
    $wget http://www.ftphp.com/scws/down/scws-1.1.9.tar.bz2
    $tar -jxvf scws-1.1.9.tar.bz2
    $cd scws-1.1.9
    $./configure --prefix=/usr/local/scws
    $make
    $su
    #make install
####检查安装是否成功
    $ /usr/local/scws/bin/scws -h
    scws (scws-cli/1.1.9)
####显示帮助文档即安装成功

##项目部署
###Clone code from github
    $git clone git://github.com/chenshijie/subject.git

###Install dependencies
    $cd subject
    $npm install -d
###Modify configuration file
    $cd etc
    $cp settings.original.json settings.json
    $vim settings.json
###settings.json 说明
    {
      "check_interval": 2000, //队列检查间隔,单位毫秒
      "debug": false, //是否已调试模式运行
      "log": {
        "file": "log/subject.log" //日志文件位置
      },
      "mysql": [
        {
          "database": "weibo", //数据库database
          "host": "127.0.0.1", //数据库IP
          "password": "spider", //数据库密码
          "port": "3306", //数据库端口
          "user": "spider"  //数据库用户名
        }
      ],
      "queue_server": { //队列服务
        "host": "127.0.0.1", //队列服务IP
        "port": 3000, //队列服务端口
        "queue_path": "queue" //队列服务路径
      },
      "scws": {                     //scws配置
        "cmd": "/usr/local/scws/bin/scws", //命令路径
        "dict": "/opt/stockradar/subject/data/stockdict.xdb", //字典文件路径
        "top": 200                 //分词显示数量
      },
      "subject_generate_queue": "article_similar",  //新任务入队，队列名称
      "subject_monitor_queue": "article_subject",   //监听队列名称
      "worker_count": 5         //worker数量
    }


##Start the spider server
    $node subject_server.js  

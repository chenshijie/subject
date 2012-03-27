var Worker = function(options) {};

/**
 * 根据任务URI从数据库中获取任务详情
 *
 * @param task
 * @param callback
 */
Worker.prototype.getTaskDetailFromDB = function(task, callback) {
  if (task.debug) {
    console.log('----------> getTaskDetailFromDB <-----------');
  }
  task.mysql.get_article_content(task.table, task.id, function(result) {
    if (result.length == 0 || result[0].url == '' || result[0].site == '' || result[0].type == '') {
      var error = {
        error: 'TASK_URL_NOT_FOUND',
        msg: ' can not read url info by id ' + task.id
      };
      callback(error, task);
    } else {
      task['articleInfo'] = result[0];
      callback(null, task);
    }
  });
};

var filtrateSegmentResult = function(data) {
    var result = [];
    var len = data.length;
    console.log(len);
    for (var i = 0; i < len; i++) {
      if ('BL' == data[i].attr || 'ST' == data[i].attr) {
        result.push(data[i]);
      }
    }
    return result;
  }

Worker.prototype.segmentTitle = function(task, callback) {
  if (task.debug) {
    console.log('----------> segmentTitle ' + task.articleInfo.id + '<-----------');
  }
  var segmentTask = {
    text: task.articleInfo.title
  };
  task.segment.doSegment(segmentTask, function(result) {
    task['title_keywords'] = filtrateSegmentResult(result);
    callback(null, task);
  });
};

Worker.prototype.segmentContent = function(task, callback) {
  if (task.debug) {
    console.log('----------> segmentContent ' + task.articleInfo.id + '<-----------');
  }
  var segmentTask = {
    text: task.articleInfo.content
  };
  task.segment.doSegment(segmentTask, function(result) {
    task['content_keywords'] = filtrateSegmentResult(result);
    callback(null, task);
  });
};

Worker.prototype.getArticleSubject = function(task, callback) {
  console.log(task.content_keywords);
  console.log(task.title_keywords);
  if (task.content_keywords.length == 0 && task.title_keywords.length) {
    var error = {
      error: 'ARTICLE_NO_KEY_WORD',
      msg: 'can not segment key words from article'
    };
    callback(error, task);
  }
  callback(null, task);
};


Worker.prototype.save2Database = function(task, callback) {
  //TODO 将主题保存到数据库
  callback(null, task);

};

exports.Worker = Worker;
exports.getWorker = function() {
  return new Worker();
};

var _ = require('underscore');
var utils = require('./utils');

var Worker = function() {

};

Worker.prototype.getTaskDetailFromDB = function(task, callback) {
  if (task.debug) {
    console.log('----------> getTaskDetailFromDB <-----------');
  }
  task.mysql.get_microblog_content(task.table, task.id, function(result) {
    if (result.length == 0 || result[0].article_id == '') {
      var error = {
        error: 'TASK_MICRO_BLOG_NOT_FOUND',
        msg: ' can not read micro_blog info by id ' + task.id
      };
      callback(error, task);
    } else {
      task['microBlogInfo'] = result[0];
      callback(null, task);
    }
  });
};

Worker.prototype.getTaskDetailFromDB2 = function(task, callback) {
  if (task.debug) {
    console.log('----------> getTaskDetailFromDB2 <-----------');
  }
  task.mysql.get_article_content(task.microBlogInfo.article_id, function(result) {
    if (result.length == 0 || result[0].url == '' || result[0].title == '' || result[0].content == '') {
      var error = {
        error: 'TASK_ARTICLE_CONTENT_NOT_FOUND',
        msg: ' can not read article_content by id ' + task.microBlogInfo.id
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
  for (var i = 0; i < len; i++) {
    if ('BL' == data[i].attr || 'ST' == data[i].attr) {
      result.push(data[i]);
    }
  }
  return result;
}

Worker.prototype.segmentTitle = function(task, callback) {
  if (task.debug) {
    console.log('----------> segmentTitle article_id: ' + task.articleInfo.id + '<-----------');
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
    console.log('----------> segmentContent article_id: ' + task.articleInfo.id + '<-----------');
  }
  var segmentTask = {
    text: task.articleInfo.content
  };
  task.segment.doSegment(segmentTask, function(result) {
    task['content_keywords'] = filtrateSegmentResult(result);
    callback(null, task);
  });
};

var calculateKeyWordsWeight = function(segmentWords, keyWords) {
  var stocks = [];
  var blocks = [];
  var words = [];
  var stocks_times = 0;
  var blocks_times = 0;
  //将板块和股票分离
  for (var i = 0, len = segmentWords.length; i < len; i++) {
    var word = segmentWords[i].word;
    if (segmentWords[i].attr == 'ST') {
      if (keyWords.stocks[word] != undefined) {
        var stock_code = keyWords.stocks[word];
        var block_id = keyWords.mapping[stock_code] == undefined ? 0: keyWords.mapping[stock_code];
        var obj = {
          code: stock_code,
          times: segmentWords[i].times,
          attr: segmentWords[i].attr,
          name: word,
          block_id: block_id
        }
        stocks_times += segmentWords[i].times;
        words.push(obj);
        stocks.push(obj);
      }
    }
    if (segmentWords[i].attr == 'BL') {
      if (keyWords.blocks[word] != undefined) {
        var obj = {
          code: keyWords.blocks[word],
          times: segmentWords[i].times,
          attr: segmentWords[i].attr,
          name: word
        }
        blocks_times += segmentWords[i].times;
        words.push(obj);
        blocks.push(obj);
      }
    }
  }
  var result = {
    stocks: stocks,
    blocks: blocks,
    words: words,
    stock_times: stocks_times,
    block_times: blocks_times,
    total_times: stocks_times + blocks_times
  };
  return result;
}

var addTimes4BlockTimes = function(collection, sub_key, element) {
  var hit = false;
  for (var i = 0; i < collection.length; i++) {
    if (collection[i][sub_key] == element[sub_key]) {
      collection[i].times += element.times;
      hit = true;
      break;
    }
  }
  if (false == hit) {
    collection.push(element);
  }
  return collection;
}

var getSameBlock4StockList = function(stocks, total_times, percent) {
  var block_times = [];
  for (var i = 0, len = stocks.length; i < len; i++) {
    var block_id = stocks[i].block_id;
    if (block_id != 0) {
      var element = {
        block_id: block_id,
        times: stocks[i].times
      };
      block_times = addTimes4BlockTimes(block_times, 'block_id', element);
    }
  }
  var max_times_block = _.max(block_times, function(ele) {
    return ele.times
  });
  if (max_times_block.times / total_times >= percent) {
    return max_times_block.block_id;
  } else {
    return 0;
  }
};

var keyWordFilter = function(words) {
  var result = [];
  for (var i = 0, len = words.length; i < len; i++) {
    if (words[i].attr != 'BL' && words[i].word != '银行') {
      result.push(words[i]);
    }
  }
  return result;
}

var stockNameFilter = function(stocks) {
  var result = [];
  for (var i = 0, len = stocks.length; i < len; i++) {
    var isFind = false;
    for (var j = 0, result_len = result.length; j < result_len; j++) {
      if (result[j].code == stocks[i].code) {
        result[j].times += stocks[i].times;
        isFind = true;
      }
    }
    if (!isFind) {
      result.push(stocks[i]);
    }
  }
  result = _.sortBy(result, function(stock) {
    return 0 - stock.times
  });
  return result;
}

Worker.prototype.getArticleSubject = function(task, callback) {
  task.title_keywords = keyWordFilter(task.title_keywords);
  task.content_keywords = keyWordFilter(task.content_keywords);
  if (task.debug) {
    console.log('----------> getArticleSubject article_id: ' + task.articleInfo.id + '<-----------');
    console.log(task.articleInfo.url);
    console.log(task.articleInfo.stock_code);
    console.log('title key word:')
    console.log(task.title_keywords);
    console.log('content key word:')
    console.log(task.content_keywords);
  }
  if (task.content_keywords.length == 0 && task.title_keywords.length == 0) {
    var error = {
      error: 'ARTICLE_NO_KEY_WORD',
      msg: 'can not segment key words from article'
    };
    callback(error, task);
  } else if (task.title_keywords.length > 0) { //标题中含有板块名称或者股票名称
    var temp = calculateKeyWordsWeight(task.title_keywords, task.keyWords);
    console.log(temp);
    var stock_key_word_length = temp.stocks.length;
    var block_key_word_length = temp.blocks.length;
    var words_length = temp.words.length;
    if (words_length == 1) { //只有一个关键字
      if (temp.words.length == 0) {
        var error = {
          error: 'calculateKeyWordsWeight_ERROR',
          msg: 'calculateKeyWordsWeight_ERROR'
        };
        callback(error, task);
      } else {
        console.log('title include one word: ' + temp.words[0].name);
        task['subject'] = {
          post: temp.words[0]
        };
        callback(null, task);
      }
    } else { //多个关键字，取出现次数多的
      if (temp.words.length == 0) {
        var error = {
          error: 'calculateKeyWordsWeight_ERROR',
          msg: 'calculateKeyWordsWeight_ERROR'
        };
        callback(error, task);
      } else {
        console.log('title include more than one words: ' + temp.words[0].name);
        task['subject'] = {
          post: temp.words[0]
        };
        callback(null, task);
      }
    }
  } else {
    var temp = calculateKeyWordsWeight(task.content_keywords, task.keyWords);
    temp.stocks = stockNameFilter(temp.stocks);
    console.log(temp);
    if (temp.blocks.length > 0) {
      console.log('有板块命中');
      if (temp.blocks.length > 1) {
        //舍弃
        console.log('板块超过1个舍弃');
        var error = {
          error: 'CONTENT_INCLUDE_MORE_THAN_ONE_BLOCK',
          msg: '文章正文提到超过1个板块,舍弃'
        };
        task.logger.info('-------------------------------------');
        task.logger.info('文章正文提到超过1个板块,舍弃');
        task.logger.info(task.articleInfo.url);
        task.logger.info('标题关键字:' + JSON.stringify(task.title_keywords));
        task.logger.info('正文关键字' + JSON.stringify(task.content_keywords));
        callback(error, task);
      } else {
        console.log('只有一个板块命中');
        var max_time_block_id = getSameBlock4StockList(temp.stocks, temp.stock_times, 0.8);
        if (max_time_block_id == temp.blocks[0].code) {
          console.log('成功匹配');
          task['subject'] = {
            post: temp.blocks[0]
          };
          callback(null, task);
        } else {
          var error = {
            error: 'CONTENT_BLOCK_NOT_COVER_STOCKS',
            msg: '文章正文提到的板块不能涵盖大部分股票,舍弃'
          };
          task.logger.info('-------------------------------------');
          task.logger.info('文章正文提到的板块不能涵盖大部分股票,舍弃');
          task.logger.info(task.articleInfo.url);
          task.logger.info('标题关键字:' + JSON.stringify(task.title_keywords));
          task.logger.info('正文关键字' + JSON.stringify(task.content_keywords));
          callback(error, task);
        }
      }
    } else {
      console.log('无板块命中');
      if (temp.stocks.length > 10) { //股票出现次数超过10个
        console.log('超过10之股票舍弃');
        var error = {
          error: 'CONTENT_INCLUDE_MORE_THEN_TEN_STOCKS',
          msg: '超过10之股票,舍弃'
        };
        task.logger.info('-------------------------------------');
        task.logger.info('文章正文超过10之股票,舍弃');
        task.logger.info(task.articleInfo.url);
        task.logger.info('标题关键字:' + JSON.stringify(task.title_keywords));
        task.logger.info('正文关键字' + JSON.stringify(task.content_keywords));
        callback(error, task);
      } else if (temp.stocks.length > 3) {
        var max_time_block_id = getSameBlock4StockList(temp.stocks, temp.stock_times, 0.8);
        if (max_time_block_id > 0) {
          console.log('有共同的板块 ID: ' + max_time_block_id);
          var block_temp_obj = {
            code: max_time_block_id,
            attr: 'BL'
          };
          task['subject'] = {
            post: block_temp_obj
          };
          callback(null, task);
        } else {
          console.log('无共同板块 舍弃');
          var error = {
            error: 'CONTENT_STOCKS_NO_SAME_BLOCK',
            msg: '文章正文提到股票无共同板块,舍弃'
          };
          task.logger.info('-------------------------------------');
          task.logger.info('文章正文提到股票无共同板块,舍弃');
          task.logger.info(task.articleInfo.url);
          task.logger.info('标题关键字:' + JSON.stringify(task.title_keywords));
          task.logger.info('正文关键字' + JSON.stringify(task.content_keywords));
          callback(error, task);
        }
      } else {
        if (temp.stocks[0].times / temp.stock_times > 0.6) {
          console.log('一只股票出现次数 超过 60%');
          task['subject'] = {
            post: temp.stocks[0]
          };
          callback(null, task);
        } else {
          console.log('无单只股票出现次数 超过 60%  舍弃');
          var error = {
            error: 'CONTENT_SINGLE_STOCK_WEIGHT_LESS_60',
            msg: '无单只股票出现次数 超过 60%  舍弃'
          };
          task.logger.info('-------------------------------------');
          task.logger.info('无单只股票出现次数 超过 60% ,舍弃');
          task.logger.info(task.articleInfo.url);
          task.logger.info('标题关键字:' + JSON.stringify(task.title_keywords));
          task.logger.info('正文关键字' + JSON.stringify(task.content_keywords));
          callback(error, task);
        }
      }
    }
  }
};

Worker.prototype.save2Database = function(task, callback) {
  //TODO 将主题保存到数据库
  if (task.debug) {
    console.log('#----------> save2Database article_id: ' + task.articleInfo.id + '<-----------');
  }
  //function(micro_blog_id, article_id, stock_code, block_id, send_type, in_time, cb) {
  var micro_blog_id = task.id;
  var article_id = task.microBlogInfo.article_id;
  var stock_code = task.subject.post.attr == 'ST' ? task.subject.post.code: '';
  var block_id = task.subject.post.attr == 'BL' ? task.subject.post.code: 0;
  var send_type = 'post';
  var in_time = utils.getTimestamp();

  task.mysql.save2ArticleSubject(micro_blog_id, article_id, stock_code, block_id, send_type, in_time, function(insert_id) {
    console.log(insert_id)
    if (insert_id > 0) {
      task['save2DBOK'] = true;
      task['new_task_id'] = insert_id;
      callback(null, task);
    } else {
      console.log('无单只股票出现次数 超过 60%  舍弃');
      var error = {
        error: 'SAVE_2_DB_ERROR',
        msg: '保存到数据库错误'
      };
      callback(error, task);
    }
  });
};

exports.Worker = Worker;
exports.getWorker = function() {
  return new Worker();
};


const PassThrough = require('stream').PassThrough;
const url         = require('url');
const miniget     = require('miniget');
const m3u8        = require('./m3u8-parser');
const Queue       = require('./queue');
const fs          = require('fs');


/**
 * @param {String} playlistURL
 * @param {Object} options
 * @return {stream.Readable}
 */
module.exports = function(playlistURL, options) {
  var stream = new PassThrough();
  options = options || {};
  var chunkReadahead = options.chunkReadahead || 3;
  var refreshInterval = options.refreshInterval || 600000; // 10 minutes
  var requestOptions = options.requestOptions;
  var onComplete = options.onComplete;
  var onError2 = options.onError;
  var outFile = options.outFile;
  var resumeLoad = !!options.resumeLoad;

  //save video
  if (outFile) {
    var start = 0, lastPosition = 0;
    if (resumeLoad) {
      var resumeFile = outFile + '.resume';
      if (fs.existsSync(resumeFile)) {
        var resumeContent = fs.readFileSync(resumeFile).toString().trim().split(',');
        var lastPathname = resumeContent[0];
        lastPosition = parseFloat(resumeContent[1]);
        start = lastPosition;
      }
    }
    if (!fs.existsSync(outFile))
      fs.writeFileSync(outFile, '');
    var outStream = fs.createWriteStream(outFile, { start: start, flags: 'r+' });
    outStream.on('error', onError);
    stream.pipe(outStream);
  }

  var latestSegment;
  var streamQueue = new Queue(function(segment, callback) {        
    latestSegment = segment;    
    //get stream length
    segment.length = 0;
    segment.on('data', function (chunk) {
      segment.length += chunk.length;      
    });
    segment.on('end', callback);    
    segment.pipe(stream, { end: false });
  }, { concurrency: 1 });

  var requestQueue = new Queue(function(segmentURL, callback) {
    var segment = miniget(url.resolve(playlistURL, segmentURL), requestOptions);    
    //get stream pathname    
    segment.pathname = url.parse(segmentURL).pathname;    
    segment.on('error', callback);    
    streamQueue.push(segment, callback);
  }, {
    concurrency: chunkReadahead,
    unique: function(segmentURL) { return segmentURL; },
  });

  function onError(err) {
    if (outFile)
      onError2 && onError2(err)
    else
      stream.emit('error', err);
    // Stop on any error.
    stream.end();
  }

  // When to look for items again.
  var refreshThreshold;
  var fetchingPlaylist = false;
  var destroyed = false;
  var ended = false;
  function onQueuedEnd(err) {        
    if (err) {
      onError(err);
    } else {
      if (resumeFile) {
        lastPosition += latestSegment.length;
        fs.writeFileSync(resumeFile, latestSegment.pathname + ',' + lastPosition);
      }      
      if (!fetchingPlaylist && !destroyed && !ended &&
        requestQueue.tasks.length + requestQueue.active === refreshThreshold) {
        refreshPlaylist();
      } else if (ended && !requestQueue.tasks.length && !requestQueue.active) {
        //download complete
        if (resumeFile && fs.existsSync(resumeFile))
          fs.unlinkSync(resumeFile);
        onComplete && onComplete();          
        stream.end();
      }
    } 
  }

  var tid;
  function refreshPlaylist() {
    clearTimeout(tid);
    fetchingPlaylist = true;
    var req = miniget(playlistURL, requestOptions);
    req.on('error', onError);
    var parser = req.pipe(new m3u8());
    parser.on('tag', function(tagName) {
      if (tagName === 'EXT-X-ENDLIST') {
        ended = true;
        req.unpipe();
        clearTimeout(tid);
      }
    });
    var totalItems = 0;
    var resume = !lastPathname;
    parser.on('item', function(item) {
      if (resume) {
        totalItems++;
        requestQueue.push(item, onQueuedEnd);
      } else if (lastPathname === url.parse(item).pathname) {
        resume = true;
      }
    });
    parser.on('end', function() {
      refreshThreshold = Math.ceil(totalItems * 0.01);
      tid = setTimeout(refreshPlaylist, refreshInterval);
      fetchingPlaylist = false;
    });
  }
  refreshPlaylist();

  stream.end = function() {
    destroyed = true;
    streamQueue.die();
    requestQueue.die();
    clearTimeout(tid);
    if (latestSegment) { latestSegment.unpipe(); }
    PassThrough.prototype.end.call(stream);
  };

  if (!outFile)
    return stream;
};
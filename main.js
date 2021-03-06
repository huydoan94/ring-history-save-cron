const cron = require('node-cron');
const express = require('express');
const fs = require('fs');
const path = require('path');
const nodeMachineId = require('node-machine-id');
const util = require('util');
const libaxios = require('axios');
const JSONBigInt = require('json-bigint');
const moment = require('moment');
const get = require('lodash/get');
const filter = require('lodash/filter');
const map = require('lodash/map');
const reduce = require('lodash/reduce');
const every = require('lodash/every');
const some = require('lodash/some');
const sortBy = require('lodash/sortBy');
const forEach = require('lodash/forEach');
const isNil = require('lodash/isNil');
const isEmpty = require('lodash/isEmpty');
const last = require('lodash/last');

const API_VERSION = 11;

async function axios(...params) {
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      reject(new Error('Timeout !!!'));
    }, 60000);
    libaxios(...params).then((res) => {
      clearTimeout(timeout);
      resolve(res);
    }).catch((err) => {
      clearTimeout(timeout);
      reject(err);
    });
  });
}

async function sleep(milliseconds) {
  return new Promise(resolve => setTimeout(resolve, milliseconds));
}

async function promiseFetchWithRetryMechanism(fetcher, ...params) {
  const retries = 5;
  const retryFunc = remain => fetcher(...params).catch((err) => {
    if (remain === 0 || get(err, 'response.status') === 401) {
      throw err;
    }
    return sleep(3000).then(() => retryFunc(remain - 1));
  });
  return retryFunc(retries - 1);
}

async function promiseAllWithLimit(callers, maxPromise = 5, stopOnError = true) {
  if (isEmpty(callers)) return [];
  return new Promise((resolve, reject) => {
    const endIndex = callers.length - 1;
    let currentIndex = -1;
    let currentInPool = 0;
    let isFailed = false;
    const results = [];
    const resolveResults = (data) => {
      if (every(data, r => r.data === null)) {
        reject(new Error('Promise failed !!!'));
      }
      const sorted = sortBy(data, r => r.index);
      const sortedResults = map(sorted, s => s.data);
      resolve(sortedResults);
    };
    const next = () => {
      if (isFailed) return;
      if (currentIndex >= endIndex && currentInPool <= 0) {
        resolveResults(results);
        return;
      }
      if (currentIndex >= endIndex) return;

      currentIndex += 1;
      currentInPool += 1;
      ((innerIndex) => {
        callers[innerIndex]().then((res) => {
          results.push({ index: innerIndex, data: res });
          currentInPool -= 1;
          next();
        }).catch((err) => {
          if (stopOnError) {
            isFailed = true;
            reject(err);
            return;
          }
          results.push({ index: innerIndex, data: null });
          currentInPool -= 1;
          next();
        });
      })(currentIndex);
    };

    for (let i = 0; i < maxPromise; i += 1) {
      next();
    }
  });
}

async function promiseMap(collection, iteratee) {
  if (isNil(collection)) return [];
  const iteratees = map(collection, (...params) => () => iteratee(...params));
  return promiseAllWithLimit(iteratees, 20);
}

class SaveHistoryJob {
  constructor(username, password) {
    this.username = username;
    this.password = password;
    this.authToken = null;
    this.sessionToken = null;
    this.hardwareId = null;
    this.metaPath = path.join(__dirname, '.meta.json');

    this.login.bind(this);
    this.getSession.bind(this);
  }

  // --------- HELPERS PART ---------- //

  logger(message) {
    console.log(`${moment().format('YYYY-MM-DD HH:mm:ss')}: ${message}`);
  }

  async getHardwareId() {
    if (!isEmpty(this.hardwareId)) {
      return this.hardwareId;
    }
    const meta = await this.readMeta();
    this.hardwareId = get(meta, 'hardwareId', nodeMachineId.machineIdSync());
    return this.hardwareId;
  }

  async fetcher(...params) {
    const [firstParam, ...others] = params;
    let url = get(firstParam, 'url', '');
    const attachVersionAndToken = get(firstParam, 'attachVersionAndToken', true);
    if (attachVersionAndToken && url.indexOf('api_version') === -1) {
      url = `${url}${url.indexOf('?') === -1 ? `?api_version=${API_VERSION}` : `&api_version=${API_VERSION}`}`;
    }
    if (attachVersionAndToken && url.indexOf('auth_token') === -1) {
      const sessionToken = await this.getSession();
      url = `${url}${url.indexOf('?') === -1 ? `?auth_token=${sessionToken}` : `&auth_token=${sessionToken}`}`;
    }
    let headers = get(firstParam, 'headers', {});
    if (attachVersionAndToken) {
      headers = { ...headers, hardware_id: await this.getHardwareId() };
    }
    const modParams = [{
      ...firstParam, url, headers, attachVersionAndToken: undefined,
    }, ...others];
    return promiseFetchWithRetryMechanism(axios, ...modParams).catch((err) => {
      if (get(err, 'response.status') === 401 && attachVersionAndToken) {
        return this.getSession(true).then(() => this.fetcher(...params));
      }
      throw err;
    });
  }

  async createDirs(dirs) {
    return reduce(dirs, (acc, d) => acc.then(() => new Promise((resolve, reject) => {
      fs.mkdir(d, '0777', (err) => {
        if (err) {
          if (err.code === 'EEXIST') resolve();
          else reject(err);
        } else resolve();
      });
    })), Promise.resolve());
  }

  // ---- SESSION AND TOKEN PART ----- //

  async login(skipCurrentToken = false) {
    if (!skipCurrentToken && !isEmpty(this.authToken)) {
      return this.authToken;
    }

    this.logger('Logging in');
    const reqBody = {
      username: this.username,
      password: this.password,
      grant_type: 'password',
      scope: 'client',
      client_id: 'ring_official_android',
    };

    return promiseFetchWithRetryMechanism(axios, {
      url: 'https://oauth.ring.com/oauth/token',
      method: 'POST',
      data: reqBody,
      headers: {
        'content-type': 'application/json',
        'content-length': JSON.stringify(reqBody).length,
      },
    }).then((res) => {
      this.logger('Log in sucessful');
      this.authToken = get(res, 'data.access_token');
      return this.authToken;
    }).catch((err) => {
      this.logger('Log in FAIL');
      throw err;
    });
  }

  async getSession(skipCurrentSession = false) {
    if (!skipCurrentSession && !isEmpty(this.sessionToken)) {
      return this.sessionToken;
    }

    this.logger('Getting session token');
    const reqBody = {
      device: {
        hardware_id: await this.getHardwareId(),
        metadata: {
          api_version: API_VERSION,
        },
        os: 'android',
      },
    };

    return promiseFetchWithRetryMechanism(axios, {
      url: `https://api.ring.com/clients_api/session?api_version=${API_VERSION}`,
      method: 'POST',
      data: reqBody,
      headers: {
        'content-type': 'application/json',
        'content-length': JSON.stringify(reqBody).length,
        Authorization: `Bearer ${await this.login()}`,
      },
    }).then((res) => {
      this.logger('Get session token sucessful');
      this.sessionToken = get(res, 'data.profile.authentication_token');
      return this.sessionToken;
    }).catch((err) => {
      if (get(err, 'response.status') === 401) {
        return this.login(true).then(() => this.getSession(true));
      }
      this.logger('Get session token FAIL');
      throw err;
    });
  }

  // --- VIDEO STREAM PROCESS PART ---- //

  async saveByteStreamVideo({
    id, createdAt, type,
    videoStreamByteUrl,
    dir, dirPath,
  }) {
    return new Promise((resolve, reject) => {
      const extension = videoStreamByteUrl.match(/\.[0-9a-z]+?(?=\?)/i)[0];
      const fileName = `${moment(createdAt).format('YYYY-MM-DD_HH-mm-ss')}_${type}${extension}`;
      const dest = path.join(dir, fileName);

      if (fs.existsSync(dest)) {
        this.logger(`${fileName} in ${dirPath} exist. Skipping ...`);
        resolve(2);
        return;
      }

      this.logger(`Saving event ${id} to file ${fileName} in ${dirPath}`);
      this.fetcher({
        url: videoStreamByteUrl,
        method: 'GET',
        responseType: 'stream',
        attachVersionAndToken: false,
      }).then((response) => {
        let timeout;
        const file = fs.createWriteStream(dest, { flag: 'w' });
        const refreshTimeout = () => {
          if (timeout !== undefined) clearTimeout(timeout);
          timeout = setTimeout(() => {
            fs.unlink(dest, () => (err2) => {
              if (!err2) {
                this.logger(`Deleted file ${fileName} in ${dirPath}`);
              }
            });
            reject(new Error(`Timeout on ${videoStreamByteUrl}`));
          }, 10000);
        };
        refreshTimeout();
        response.data.pipe(file);
        response.data.on('data', () => {
          refreshTimeout();
        });
        response.data.on('end', () => {
          clearTimeout(timeout);
          this.logger(`Save file ${fileName} to ${dirPath} SUCCESSFUL`);
          file.close(() => resolve(1));
        });
        response.data.on('error', (err) => {
          clearTimeout(timeout);
          fs.unlink(dest, (err2) => {
            if (!err2) {
              this.logger(`Deleted file ${fileName} in ${dirPath}`);
            }
          });
          this.logger(`Save file ${fileName} to ${dirPath} FAIL`);
          reject(err);
        });
      }).catch((err) => {
        this.logger(`Save file ${fileName} to ${dirPath} FAIL`);
        reject(err);
      });
    });
  }


  async getVideoStreamByteUrl(downloadUrl) {
    return this.fetcher({
      url: downloadUrl,
      method: 'GET',
    }).then(res => get(res, 'data.url')).catch((err) => {
      throw err;
    });
  }

  async updateDownloadPool(downloadPools, remain = 10) {
    return promiseMap(downloadPools, async (p) => {
      if (p.isFailed || p.isReady) return p;

      let isFailed = false;
      const videoStreamByteUrl = await this.getVideoStreamByteUrl(p.downloadUrl).catch(() => {
        isFailed = true;
      });
      return {
        ...p,
        videoStreamByteUrl,
        isReady: !isEmpty(videoStreamByteUrl) && !isFailed,
        isFailed,
      };
    }).then((res) => {
      if (every(res, r => r.isReady || r.isFailed)) {
        return res;
      }
      if (remain === 0) {
        return res.map((r) => {
          if (r.isReady || r.isFailed) return r;
          return { ...r, isFailed: true };
        });
      }
      return sleep(3000).then(() => this.updateDownloadPool(downloadPools, remain - 1));
    });
  }

  async triggerServerRender(id) {
    this.logger(`Triggering server render for ${id}`);
    return this.fetcher({
      url: `https://api.ring.com/clients_api/dings/${id}/share/download`,
      method: 'GET',
    }).then((res) => {
      this.logger(`Trigger server render for ${id} successful`);
      return get(res, 'data');
    }).catch((err) => {
      this.logger(`Trigger server render for ${id} FAIL`);
      throw err;
    });
  }

  async downloadHistoryVideos(history) {
    this.logger('Start downloading history videos');
    let downloadPool = await promiseMap(history, async (h) => {
      const downloadUrl = `https://api.ring.com/clients_api/dings/${h.id}/share/download_status`
        + '?disable_redirect=true';
      const videoStreamByteUrl = await this.getVideoStreamByteUrl(downloadUrl);
      const dirPath = get(h, 'doorbot.description', 'Unnamed Device');
      return {
        ...h,
        createdAt: h.created_at,
        type: h.kind,
        downloadUrl,
        videoStreamByteUrl,
        isReady: !isEmpty(videoStreamByteUrl),
        isFailed: false,
        isDownloaded: false,
        isSkipped: false,
        dir: path.join(__dirname, dirPath),
        dirPath,
      };
    });

    await promiseAllWithLimit(map(
      downloadPool,
      p => (p.isReady ? () => Promise.resolve() : () => this.triggerServerRender(p.id)),
    ));

    downloadPool = await this.updateDownloadPool(downloadPool);

    const dirs = reduce(downloadPool, (acc, d) => {
      if (acc.indexOf(d.dir) !== -1) return acc;
      return [d.dir, ...acc];
    }, []);
    await this.createDirs(dirs);

    await promiseAllWithLimit(
      map(downloadPool, d => () => this.saveByteStreamVideo(d)
        .then((res) => {
          // eslint-disable-next-line no-param-reassign
          if (res === 1 || res === 2) { d.isDownloaded = true; }
          // eslint-disable-next-line no-param-reassign
          if (res === 2) { d.isSkipped = true; }
          return res;
        })
        .catch((err) => {
          // eslint-disable-next-line no-param-reassign
          d.isFailed = true;
          throw err;
        })),
      5, false,
    );

    this.logger('Download history videos done');
    return downloadPool;
  }

  // --------- HISTORY PART ---------- //

  async getLimitHistory(earliestEventId, limit = 50, remain = 5) {
    return this.fetcher({
      url: 'https://api.ring.com/clients_api/doorbots/history'
        + `?limit=${limit}${isNil(earliestEventId) ? '' : `&older_than=${earliestEventId}`}`,
      method: 'GET',
      transformResponse: [data => JSONBigInt.parse(data)],
    }).then((res) => {
      const data = get(res, 'data', []);
      if (remain === 0) {
        return data;
      }
      if (some(data, d => get(d, 'recording.status') !== 'ready')) {
        return sleep(5000).then(() => this.getLimitHistory(earliestEventId, limit, remain - 1));
      }
      return data;
    }).catch((err) => {
      throw err;
    });
  }

  async getHistory(from, to) {
    this.logger(`Geting history from ${moment(from).format('l LT')} `
      + `to ${moment(to).format('l LT')}`);
    if (isEmpty(from)) return [];
    if (moment(from).isAfter(moment(to))) return [];
    let earliestEventId = 0;
    let totalEvents = [];
    while (
      earliestEventId === 0
      || moment(last(totalEvents).created_at).isAfter(moment(from))
    ) {
      const historyEvents = await this.getLimitHistory(earliestEventId);
      if (isEmpty(historyEvents)) break;
      if (earliestEventId.toString() === last(historyEvents).id.toString()) break;
      if (moment(last(historyEvents).created_at).isBefore(moment(from))) {
        const evts = filter(historyEvents, e => moment(e.created_at).isAfter(moment(from)));
        totalEvents = totalEvents.concat(evts);
        break;
      }
      totalEvents = totalEvents.concat(historyEvents);

      const earliestEvent = last(totalEvents);
      earliestEventId = earliestEvent.id;
    }
    this.logger(`Get history from ${moment(from).format('l LT')} `
      + `to ${moment(to).format('l LT')} sucessful`);
    return filter(totalEvents, evt => moment(evt.created_at).isBefore(moment(to)));
  }

  // -------- NORMAL RUN PART -------- //

  async run(from, to) {
    this.logger(`Running at ${moment().format('l LT')}`);
    const parsedFrom = isNil(from) || !moment(from).isValid() ? undefined : moment(from);
    const parsedTo = isNil(to) || !moment(to).isValid() ? undefined : moment(to).endOf('day').format();
    try {
      await this.login();
      await this.getSession();
      const processedEvents = await this.downloadHistoryVideos(
        await this.getHistory(parsedFrom, moment(parsedTo)),
      );
      await this.writeMeta(this.createMetaData(await this.readMeta(), processedEvents));

      let ok = 0, fail = 0, skip = 0;
      forEach(processedEvents, (pe) => {
        if (pe.isSkipped) {
          skip += 1;
        } else if (pe.isDownloaded) {
          ok += 1;
        } else {
          fail += 1;
        }
      });
      // eslint-disable-next-line max-len
      this.logger(`Result:\n\tTotal: ${processedEvents.length}\n\tDownloaded: ${ok}\n\tSkipped: ${skip}\n\tFailed: ${fail}`);
      this.logger(`Finished at ${moment().format('l LT')}`);
      return processedEvents;
    } catch (err) {
      this.logger(`Run FAILED at ${moment().format('l LT')} --- ${err.stack}`);
      return [];
    }
  }

  // ----------- CRON PART ----------- //

  async readMeta() {
    return new Promise((resolve, reject) => {
      fs.readFile(this.metaPath, 'utf-8', (err, data) => {
        if (err) {
          if (err.code === 'ENOENT') {
            resolve({});
          }
          reject(err);
          return;
        }
        let metadata;
        try {
          metadata = JSONBigInt.parse(data);
        } catch (e) {
          metadata = {};
        }
        resolve(metadata);
      });
    });
  }

  async writeMeta(data) {
    return new Promise((resolve, reject) => {
      const stringified = JSONBigInt.stringify(data, null, 2)
        .replace(
          /("traversedEventIds": \[)([^\]]+)/,
          (_, a, b) => `${a}\n\t\t${b.replace(/\s+/g, ' ').replace(/((?:\S+\s){3}\S+)\s/g, '$1\n\t\t').trim()}\n\t`,
        );
      fs.writeFile(this.metaPath, stringified, (err) => {
        if (err) {
          reject(err);
          return;
        }
        resolve();
      });
    });
  }

  createMetaData(oldMeta, downloadPool) {
    const sorted = downloadPool.sort((a, b) => {
      if (moment(a.created_at).isAfter(moment(b.created_at))) return -1;
      if (moment(a.created_at).isBefore(moment(b.created_at))) return 1;
      return 0;
    });
    const oldFailedEvents = get(oldMeta, 'failedEvents', []);
    const traversedEventIDs = get(oldMeta, 'traversedEventIds', []);

    const failedEvents = filter(sorted, e => e.isFailed);
    const downloadedEvent = filter(sorted, e => e.isDownloaded);

    failedEvents.forEach((f) => {
      if (oldFailedEvents.findIndex(o => o.id.toString() === f.id.toString()) === -1) {
        oldFailedEvents.push(f);
      }
    });
    const newFailedEvents = filter(
      oldFailedEvents,
      o => downloadedEvent.findIndex(d => d.id.toString() === o.id.toString()) === -1,
    );

    downloadPool.forEach((d) => {
      if (traversedEventIDs.findIndex(id => id.toString() === d.id.toString()) === -1) {
        traversedEventIDs.push(d.id);
      }
    });
    let lastestEvent = isEmpty(oldMeta.lastestEvent) ? {} : oldMeta.lastestEvent;
    if (!isEmpty(sorted)) {
      if (isEmpty(oldMeta.lastestEventTime)) {
        lastestEvent = sorted[0];
      } else if (moment(oldMeta.lastestEventTime).isBefore(moment(sorted[0].created_at))) {
        lastestEvent = sorted[0];
      }
    }
    return {
      hardwareId: this.hardwareId,
      lastestEvent,
      lastestEventTime: lastestEvent.created_at,
      failedEvents: newFailedEvents,
      traversedEventIds: traversedEventIDs,
    };
  }

  async runCron() {
    let isCronRunning = false;
    let isFirstRun = true;
    const cronJob = async () => {
      if (isCronRunning) {
        return;
      }
      if (isFirstRun) {
        isFirstRun = false;
      }
      this.logger(`Running job at ${moment().format('l LT')}`);
      isCronRunning = true;
      try {
        let processedEvents = [];
        let meta = await this.readMeta();
        if (isEmpty(meta) || isEmpty(meta.lastestEventTime)) {
          const result = await this.run(moment().startOf('day'));
          processedEvents = result;
          meta = this.createMetaData(meta, result);
        } else {
          let retryFailedEvents = [];
          if (!isEmpty(meta.failedEvents)) {
            retryFailedEvents = await this.downloadHistoryVideos(meta.failedEvents);
          }

          let newEvents = [];
          const lastestEvent = (await this.getLimitHistory(undefined, 1))[0];
          if (moment(lastestEvent.created_at).isAfter(meta.lastestEventTime)) {
            newEvents = await this.downloadHistoryVideos(
              await this.getHistory(meta.lastestEventTime),
            );
          }

          processedEvents = [...retryFailedEvents, ...newEvents];
          meta = this.createMetaData(meta, processedEvents);
        }

        await this.writeMeta(meta);
        let ok = 0, fail = 0, skip = 0;
        forEach(processedEvents, (pe) => {
          if (pe.isSkipped) {
            skip += 1;
          } else if (pe.isDownloaded) {
            ok += 1;
          } else {
            fail += 1;
          }
        });
        // eslint-disable-next-line max-len
        this.logger(`Job result:\n\tTotal: ${processedEvents.length}\n\tDownloaded: ${ok}\n\tSkipped: ${skip}\n\tFailed: ${fail}`);
        this.logger(`Job run SUCCESS at ${moment().format('l LT')}`);
        isCronRunning = false;
      } catch (e) {
        this.logger(`Job run FAIL at ${moment().format('l LT')} --- ${e}`);
        isCronRunning = false;
      }
    };

    if (isFirstRun) cronJob();
    return cron.schedule('0 * * * * *', cronJob);
  }
}

(async () => {
  const logFilePath = path.join(__dirname, 'log.txt');
  const usernameParam = process.argv.indexOf('--username');
  const passwordParam = process.argv.indexOf('--password');
  if (usernameParam === -1 || passwordParam === -1) {
    console.log('Please input --username and/or --password');
    process.exit(1);
    return;
  }
  const username = process.argv[usernameParam + 1];
  const password = process.argv[passwordParam + 1];

  const logFile = fs.createWriteStream(logFilePath, { flags: 'a' });
  const procStdOut = process.stdout;
  console.log = (message) => {
    logFile.write(`${util.format(message)}\n`);
    procStdOut.write(`${util.format(message)}\n`);
  };

  const job = new SaveHistoryJob(username, password);
  const isCron = process.argv.indexOf('--cron') !== -1;
  if (isCron) {
    job.runCron();
    return;
  }

  const fromParam = process.argv.indexOf('--from');
  const toParam = process.argv.indexOf('--to');
  const from = fromParam !== -1 ? process.argv[fromParam + 1] : undefined;
  const to = toParam !== -1 ? process.argv[toParam + 1] : undefined;
  await job.run(from, to);
  process.exit();
})();

express().listen(0);

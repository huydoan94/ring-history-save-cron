/* eslint-disable no-console */
/* eslint-disable no-await-in-loop */
const cron = require('node-cron');
const express = require('express');
const fs = require('fs');
const crypto = require('crypto');
const util = require('util');
const axios = require('axios');
const JSONBigInt = require('json-bigint');
const moment = require('moment');
const get = require('lodash/get');
const filter = require('lodash/filter');
const map = require('lodash/map');
const reduce = require('lodash/reduce');
const every = require('lodash/every');
const forEach = require('lodash/forEach');
const isNil = require('lodash/isNil');
const isEmpty = require('lodash/isEmpty');

let username;
let password;

const API_VERSION = 11;

async function sleep(milliseconds) {
  return new Promise(resolve => setTimeout(resolve, milliseconds));
}

async function withRetryMechanism(caller, ...params) {
  const retries = 5;
  const retryFunc = remain => caller(...params).catch((err) => {
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
    let currentIndex = 0;
    let currentInPool = 0;
    let isFailed = false;
    const results = [];
    const next = () => {
      if (isFailed) return;
      if (currentIndex >= endIndex && currentInPool <= 0) {
        resolve(results);
        return;
      }
      if (currentIndex >= endIndex) return;

      currentIndex += 1;
      currentInPool += 1;
      callers[currentIndex]().then((res) => {
        results.concat(res);
        currentInPool -= 1;
        next();
      }).catch((err) => {
        if (stopOnError) {
          isFailed = true;
          reject(err);
          return;
        }
        currentInPool -= 1;
        next();
      });
    };

    for (let i = 0; i < maxPromise; i += 1) {
      next();
    }
  });
}

async function promiseMap(origArr, it) {
  if (isNil(origArr) || origArr.length === 0) return [];
  return new Promise((resolve, reject) => {
    let count = 0;
    const results = [];
    forEach(origArr, async (...params) => {
      results.push(await it(...params).catch(err => reject(err)));
      count += 1;
      if (count === origArr.length) {
        resolve(results);
      }
    });
  });
}

class SaveHistoryJob {
  constructor() {
    this.authToken = null;
    this.sessionToken = null;

    this.login.bind(this);
    this.getSession.bind(this);
  }

  logger(message) {
    console.log(`${moment().format()}: ${message}`);
  }

  async login(skipCurrentToken = false) {
    if (!skipCurrentToken && !isEmpty(this.authToken)) {
      return this.authToken;
    }

    this.logger('Logging in');
    const reqBody = {
      username,
      password,
      grant_type: 'password',
      scope: 'client',
      client_id: 'ring_official_android',
    };

    return withRetryMechanism(axios, {
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
        hardware_id: crypto.randomBytes(16).toString('hex'),
        metadata: {
          api_version: API_VERSION,
        },
        os: 'android',
      },
    };

    return withRetryMechanism(axios, {
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

  async fetcher(...params) {
    return withRetryMechanism(axios, ...params).catch((err) => {
      if (get(err, 'response.status') === 401) {
        return this.getSession(true);
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

  async getLimitHistory(earliestEventId, limit = 50) {
    return this.fetcher({
      url: 'https://api.ring.com/clients_api/doorbots/history'
        + `?api_version=${API_VERSION}&auth_token=${await this.getSession()}`
        + `&limit=${limit}${isNil(earliestEventId) ? '' : `&older_than=${earliestEventId}`}`,
      method: 'GET',
      transformResponse: [data => JSONBigInt.parse(data)],
    }).then(res => get(res, 'data', [])).catch((err) => {
      throw err;
    });
  }

  async getHistory(from, to) {
    this.logger(`Geting history from ${moment(from).format('l LT')} `
      + `to ${moment(to).format('l LT')}`);
    if (isEmpty(from)) return [];
    if (moment(from).isAfter(moment(to))) return [];
    let earliestEventId;
    let totalEvents = [];
    while (
      earliestEventId === undefined
      || moment(totalEvents[totalEvents.length - 1].created_at).isAfter(moment(from))
    ) {
      const historyEvents = await this.getLimitHistory(earliestEventId);
      if (isEmpty(historyEvents)) break;
      if (earliestEventId === historyEvents[historyEvents.length - 1].id) break;
      if (moment(historyEvents[historyEvents.length - 1].created_at).isBefore(moment(from))) {
        const evts = filter(historyEvents, e => moment(e.created_at).isAfter(moment(from)));
        totalEvents = totalEvents.concat(evts);
        break;
      }
      totalEvents = totalEvents.concat(historyEvents);

      const earliestEvent = totalEvents[totalEvents.length - 1];
      earliestEventId = earliestEvent.id;
    }
    this.logger(`Get history from ${moment(from).format('l LT')} `
      + `to ${moment(to).format('l LT')} sucessful`);
    return filter(totalEvents, evt => moment(evt.created_at).isBefore(moment(to)));
  }

  async triggerServerRender(id) {
    this.logger(`Triggering server render for ${id}`);
    return this.fetcher({
      url: `https://api.ring.com/clients_api/dings/${id}/share/download`
        + `?api_version=${API_VERSION}&auth_token=${await this.getSession()}`,
      method: 'GET',
    }).then((res) => {
      this.logger(`Trigger server render for ${id} successful`);
      return get(res, 'data');
    }).catch((err) => {
      this.logger(`Trigger server render for ${id} FAIL`);
      throw err;
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

  async completeDownloadPool(downloadPools, remain = 10) {
    this.logger('Updating download pool');
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
      return sleep(3000).then(() => this.completeDownloadPool(downloadPools, remain - 1));
    });
  }

  async saveByteStreamVideo({
    id, createdAt, type, dir, videoStreamByteUrl,
  }) {
    return new Promise((resolve, reject) => {
      const extension = videoStreamByteUrl.match(/\.[0-9a-z]+?(?=\?)/i)[0];
      const fileName = `${moment(createdAt).format('YYYY-MM-DD_HH-mm-ss')}_${type}${extension}`;
      const dest = `${dir}/${fileName}`;

      if (fs.existsSync(dest)) {
        this.logger(`${fileName} in ${dir} exist. Skipping ...`);
        resolve();
        return;
      }

      this.logger(`Saving event ${id} to file ${fileName} in ${dir}`);
      this.fetcher({
        url: videoStreamByteUrl,
        method: 'GET',
        responseType: 'stream',
      }).then((response) => {
        let timeout;
        const file = fs.createWriteStream(dest, { flag: 'w' });
        const refreshTimeout = () => {
          if (timeout !== undefined) clearTimeout(timeout);
          timeout = setTimeout(() => {
            fs.unlink(dest, () => {});
            reject(new Error(`Timeout on ${videoStreamByteUrl}`));
          }, 5000);
        };
        refreshTimeout();
        response.data.pipe(file);
        response.data.on('data', () => {
          refreshTimeout();
        });
        response.data.on('end', () => {
          this.logger(`Save file ${fileName} SUCCESSFUL`);
          file.close(resolve);
        });
        response.data.on('error', (err) => {
          fs.unlink(dest, () => {});
          this.logger(`Save file ${fileName} FAIL`);
          reject(err);
        });
      }).catch((err) => {
        this.logger(`Save file ${fileName} FAIL`);
        reject(err);
      });
    });
  }

  async downloadHistoryVideos(history) {
    this.logger('Start downloading history videos');
    let downloadPool = await promiseMap(history, async (h) => {
      const downloadUrl = `https://api.ring.com/clients_api/dings/${h.id}/share/download_status`
      + '?disable_redirect=true'
      + `&api_version=${API_VERSION}&auth_token=${await this.getSession()}`;
      const videoStreamByteUrl = await this.getVideoStreamByteUrl(downloadUrl);
      return {
        ...h,
        id: h.id,
        createdAt: h.created_at,
        type: h.kind,
        downloadUrl,
        videoStreamByteUrl,
        isReady: !isEmpty(videoStreamByteUrl),
        isFailed: false,
        isDownloaded: false,
        dir: `./${get(h, 'doorbot.description', 'Unnamed Device')}`,
      };
    });

    await promiseAllWithLimit(map(
      downloadPool,
      p => (p.isReady ? () => Promise.resolve() : () => this.triggerServerRender(p.id)),
    ));

    downloadPool = await this.completeDownloadPool(downloadPool);

    const dirs = reduce(downloadPool, (acc, d) => {
      if (acc.indexOf(d.dir) !== -1) return acc;
      return [d.dir, ...acc];
    }, []);
    await this.createDirs(dirs);

    await promiseAllWithLimit(
      map(downloadPool, d => () => this.saveByteStreamVideo(d)
        // eslint-disable-next-line no-param-reassign
        .then((res) => { d.isDownloaded = true; return res; })
        // eslint-disable-next-line no-param-reassign
        .catch((err) => { d.isFailed = true; throw err; })),
      5, false,
    );

    this.logger('Download history videos done');
    return downloadPool;
  }

  async run(from, to) {
    this.logger(`Running at ${moment().format('l LT')}`);
    const parsedFrom = isNil(from) || !moment(from).isValid() ? null : moment(from);
    try {
      await this.login();
      await this.getSession();
      const result = await this.downloadHistoryVideos(
        await this.getHistory(parsedFrom, moment(to)),
      );
      this.logger(`Finished at ${moment().format('l LT')}`);
      return result;
    } catch (err) {
      this.logger(`Run FAILED at ${moment().format('l LT')} --- ${err}`);
      return [];
    }
  }

  async readMeta() {
    return new Promise((resolve, reject) => {
      fs.readFile('./.meta', 'utf-8', (err, data) => {
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
      fs.writeFile('./.meta', JSONBigInt.stringify(data, null, 2), (err) => {
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
      if (oldFailedEvents.findIndex(o => o.id === f.id) === -1) {
        oldFailedEvents.push(f);
      }
    });
    const newFailedEvents = filter(oldFailedEvents, o => downloadedEvent.findIndex(d => d.id === o.id) === -1);

    downloadPool.forEach((d) => {
      if (traversedEventIDs.findIndex(id => id === d.id) === -1) {
        traversedEventIDs.push(d.id);
      }
    });
    let lastestEvent = {};
    if (!isEmpty(sorted)) {
      lastestEvent = moment(oldMeta.lastestEventTime).isBefore(moment(sorted[0].created_at))
        ? sorted[0]
        : oldMeta.lastestEvent;
    }
    return {
      lastestEvent,
      lastestEventTime: lastestEvent.created_at,
      failedEvents: newFailedEvents,
      traversedEventIds: traversedEventIDs,
    };
  }

  async runCron() {
    let isCronRunning = false;
    return cron.schedule('0 * * * * *', async () => {
      if (isCronRunning) {
        this.logger(`Skip running job at ${moment().format('l LT')}`);
        return;
      }
      this.logger(`Running job at ${moment().format('l LT')}`);
      isCronRunning = true;
      try {
        let meta = await this.readMeta();
        if (isEmpty(meta) || isEmpty(meta.lastestEventTime)) {
          const result = await this.run(moment().startOf('day'));
          meta = this.createMetaData(meta, result);
        } else {
          let retryFailedEvents = [];
          if (!isEmpty(meta.failedEvents)) {
            retryFailedEvents = await this.downloadHistoryVideos(meta.failedEvents);
          }

          let newEvents = [];
          const lastestEvent = (await this.getLimitHistory(undefined, 1))[0];
          if (lastestEvent.created_at !== meta.lastestEventTime) {
            newEvents = await this.downloadHistoryVideos(
              await this.getHistory(meta.lastestEventTime),
            );
          }

          meta = this.createMetaData(meta, [...retryFailedEvents, ...newEvents]);
        }
        await this.writeMeta(meta);
        this.logger(`Job run SUCCESS at ${moment().format('l LT')}`);
        isCronRunning = false;
      } catch (e) {
        this.logger(`Job run FAIL at ${moment().format('l LT')} --- ${e}`);
        isCronRunning = false;
      }
    });
  }
}

(async () => {
  const usernameParam = process.argv.indexOf('--username');
  const passwordParam = process.argv.indexOf('--password');
  if (usernameParam === -1 || passwordParam === -1) {
    process.exit(1);
    return;
  }
  username = process.argv[usernameParam + 1];
  password = process.argv[passwordParam + 1];

  const logFile = fs.createWriteStream('./output.log', { flags: 'a' });
  const procStdOut = process.stdout;
  console.log = (message) => {
    logFile.write(`${util.format(message)}\n`);
    procStdOut.write(`${util.format(message)}\n`);
  };

  const job = new SaveHistoryJob();
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

express().listen(65520);

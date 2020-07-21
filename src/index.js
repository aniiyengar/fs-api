
import express from 'express';
import auth0 from 'auth0';
import bodyParser from 'body-parser';
import aws from 'aws-sdk';
import twit from 'twit';
import cors from 'cors';
import moment from 'moment';
import LRU from 'lru-cache';
import {
  getUser,
  createUser,
  deleteUser,
  updateUser,
  scanUsers,
} from './users';

const tokenCache = new LRU(1000);

import {
  symmetricEncrypt, symmetricDecrypt,
} from './crypto';
import { MANAGEMENT_CLIENT_ID, MANAGEMENT_CLIENT_SECRET, SYMMETRIC_KEY, TWITTER_API_KEY, TWITTER_API_SECRET } from './config';
import { runLoop, scheduleTask, initializeIndex, deleteIndex, searchIndex, getAllIds, runIndexTask } from './queue';

aws.config.update({ region: 'us-east-1' });

const app = express();

app.use(cors());

const management = new auth0.ManagementClient({
  domain: 'aniiyengar.auth0.com',
  clientId: MANAGEMENT_CLIENT_ID,
  clientSecret: MANAGEMENT_CLIENT_SECRET,
  scope: 'read:users update:users',
});

const authentication = new auth0.AuthenticationClient({
  domain: 'aniiyengar.auth0.com',
  clientId: MANAGEMENT_CLIENT_ID,
});

// const tokens = new auth0.TokenManager({
//   baseUrl: 'aniiyengar.auth0.com',
//   clientId: MANAGEMENT_CLIENT_ID,
// });

app.use(bodyParser.json());

app.get('/ping', (req, res) => { res.send('pong'); });

const auth = async (req, res, next) => {
  const token = req.get('authorization').match(/\S+/g)[1];
  try {
    let user;
    if (tokenCache.has(token)) {
      user = JSON.parse(tokenCache.get(token));
    } else {
      user = await authentication.getProfile(token);
      tokenCache.set(token, JSON.stringify(user));
    }
    req.user = user;
    next();
  } catch (e) {
    console.log(e);
    return res.setStatus(401);
  }
};

const initializeUser = async (id) => {
  const mUser = await management.getUser({ id });
  const accessToken = mUser.identities[0].access_token;
  const accessTokenSecret = mUser.identities[0].access_token_secret;

  /* Get list of allowed users */
  const params = {
    Bucket: 'faveindex',
    Key: 'allowed_users',
  };
  const s3 = new aws.S3();
  const data = await s3.getObject(params).promise();

  const allowedScreenNames = data.Body.toString().trim().match(/\S+/g);

  if (!accessTokenSecret || !accessToken) {
    throw new Error('Could not authenticate user.');
  }

  if (!allowedScreenNames.includes(mUser.screen_name)) {
    throw new Error('User not allowed.');
  }

  await createUser(id, {
    indexedEntries: 0,
    screen_name: mUser.screen_name,
    encryptedToken: symmetricEncrypt(accessToken),
    encryptedTokenSecret: symmetricEncrypt(accessTokenSecret),
  });
};

app.post('/login', auth, async (req, res) => {
  let user = await getUser(req.user.sub);
  if (!user && req.body.create) {
    console.log('Created new user.');
    /* Create new user. */
    try {
      await initializeUser(req.user.sub);
      await initializeIndex(req.user.sub);

      /* Start indexing tweets. Schedule 10 rounds. */
      // await updateUser(req.user.sub, {
      //   indexingRoundsLeft: 15,
      // });

      // scheduleTask({ id: req.user.sub });
      // await runIndexTask(req.user.sub, 15, 1);
      await scheduleTask({ id: req.user.sub, amount: 15 });
    } catch (e) {
      return res.status(409).json({
        message: e.message,
      });
    }
  }

  user = await getUser(req.user.sub);

  if (req.body.create) {
    return res.json({
      screenName: user.screen_name,
      indexed: user.indexedEntries,
      id: user.id,
      lastIndexTime: user.lastIndexTime,
    });
  } else {
    return res.json({
      id: req.user.sub,
    });
  }
});

app.post('/delete', auth, async (req, res) => {
  await deleteUser(req.user.sub);
  await deleteIndex(req.user.sub);
  return res.sendStatus(204);
});

app.post('/reindex', async (req, res) => {
  const token = req.get('authorization');
  if (token.trim() !== SYMMETRIC_KEY) {
    return res.sendStatus(403);
  }

  const users = await scanUsers();
  users.forEach(async (user) => {
    await updateUser(user.id, {
      indexingRoundsLeft: 2,
    });

    scheduleTask({ id: user.id, amount: 2 });
  });

  return res.sendStatus(204);
});

app.post('/deleteindex', async (req, res) => {
  const token = req.get('authorization');
  if (token.trim() !== SYMMETRIC_KEY) {
    return res.sendStatus(403);
  }

  const users = await scanUsers();
  users.forEach(async (user) => {
    console.log('deleting index ' + user.id);
    await deleteIndex(user.id);
    await deleteUser(user.id);
    const s3 = new aws.S3();
    try {
      await s3.deleteObject({
        Key: user.id.split('|')[1],
        Bucket: 'faveindex-userdata',
      }).promise();
    } catch {
      console.log('blah');
    }
  });

  return res.sendStatus(204);
});

app.get('/search', auth, async (req, res) => {
  if (!req.query.q) {
    return res.status(200).json({
      total: 0,
      results: [],
    });
  }
  const data = await searchIndex(req.user.sub, req.query.q, req.query.from);
  res.status(200).json({
    total: data.total,
    results: data.results.sort((a, b) => moment(b.created_at, 'ddd MMM D HH:mm:ss ZZ YYYY') - moment(a.created_at, 'ddd MMM D HH:mm:ss ZZ YYYY'))
  });
});

app.listen(9090, () => {
  console.log('Listening on 9090');
});

runLoop();

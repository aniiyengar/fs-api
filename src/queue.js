
import twit from 'twit';
import aws from 'aws-sdk';
import { getUser, updateUser, deleteUser } from './users';
import { symmetricDecrypt } from './crypto';
import { TWITTER_API_KEY, TWITTER_API_SECRET } from './config';
import aws4 from 'aws4';
import axios from 'axios';
import moment from 'moment';

aws.config.update({ region: 'us-east-1' });

const sqs = new aws.SQS({ apiVersion: '2012-11-05' });

/* 1 if x is larger, -1 if y is larger */
const compare = (x, y) => x.padStart(30).localeCompare(y.padStart(30));

const sendSignedRequest = async (options) => {
  const chain = new aws.CredentialProviderChain();
  chain.providers.push(new aws.SharedIniFileCredentials({ profile: 'faveindex' }));
  chain.providers.push(new aws.EC2MetadataCredentials());

  const credentials = await chain.resolvePromise();
  aws4.sign(options, credentials);

  options.url = 'https://' + options.host + options.path;
  options.data = options.body;

  let result;
  try {
    result =await axios(options);
  } catch (e) {
    console.log(e);
    console.log(e.response.data);
  }

  return result;
}

const url = 'search-faveindex-teusqwasx7if6k6px32gu5e7om.us-east-1.es.amazonaws.com';

const trimHydratedTweet = (tweet) => {
  const result = {
    created_at: tweet.created_at,
    id_str: tweet.id_str,
    text: tweet.text,
    entities: tweet.entities,
    user: {
      id_str: tweet.user.id_str,
      name: tweet.user.name,
      screen_name: tweet.user.screen_name,
      url: tweet.user.url,
      profile_image_url_https: tweet.user.profile_image_url_https,
    },
    is_quote_status: tweet.is_quote_status,
  };

  if (tweet.is_quote_status && tweet.quoted_status) {
    result.quoted_status = trimHydratedTweet(tweet.quoted_status);
  }

  return result;
}

export const getAllIds = async (userId) => {
  const id = userId.split('|')[1];
  const esOptions = {
    query: {
      match_all: {},
    },
    stored_fields: [],
  };

  const result = [];
  let total = 0;
  let scrollId;
  let reachTotal = 1;

  let opts = {
    host: url,
    path: '/tweets_' + id + '/_search?size=100&scroll=5s',
    service: 'es',
    region: 'us-east-1',
    method: 'GET',
    body: JSON.stringify(esOptions),
    headers: {
      host: url,
      'Content-Type': 'application/json',
    },
  }
  let r = await sendSignedRequest(opts);
  reachTotal = r.data.hits.total.value;
  total += r.data.hits.hits.length;
  scrollId = r.data._scroll_id;
  result.push(...r.data.hits.hits.map(i => i._id));

  while (total < reachTotal) {
    opts = {
      host: url,
      path: '/_search/scroll',
      service: 'es',
      region: 'us-east-1',
      method: 'GET',
      body: JSON.stringify({
        scroll: '5s',
        scroll_id: scrollId,
      }),
      headers: {
        host: url,
        'Content-Type': 'application/json',
      },
    };

    r = await sendSignedRequest(opts);
    scrollId = r.data._scroll_id;
    total += r.data.hits.hits.length;
    result.push(...r.data.hits.hits.map(i => i._id));
    console.log(result);
  }

  return result;
}

export const initializeIndex = async (userId) => {
  const id = userId.split('|')[1];

  const textualFields = [
    'text',
    'quoted_text',
    'user_name',
    'user_handle',
    'quoted_user_handle',
    'quoted_user_name',
  ];

  const esOptions = {
    mappings: {
      properties: {
        raw: {
          enabled: false,
        },
      },
    },
  };

  for (let i = 0; i < textualFields.length; i += 1) {
    esOptions.mappings.properties[textualFields[i]] = {
      type: 'text',
      analyzer: 'english',
    };
  }

  const opts = {
    host: url,
    path: '/tweets_' + id,
    service: 'es',
    region: 'us-east-1',
    method: 'PUT',
    body: JSON.stringify(esOptions),
    headers: {
      host: url,
      'Content-Type': 'application/json',
    },
  };

  await sendSignedRequest(opts);
  console.log('Successfully initialized ES index for tweets_' + id);
};

export const deleteIndex = async (userId) => {
  const id = userId.split('|')[1];
  const opts = {
    host: url,
    path: '/tweets_' + id,
    service: 'es',
    region: 'us-east-1',
    method: 'DELETE',
    headers: {
      host: url,
    },
  };

  await sendSignedRequest(opts);
};

export const searchIndex = async (userId, query, from) => {
  const id = userId.split('|')[1];
  const fields = [
    'text',
    'quoted_text',
    'user_name',
    'user_handle',
    'quoted_user_handle',
    'quoted_user_name',
  ];
  const opts = {
    host: url,
    path: '/tweets_' + id + '/_search',
    body: {
      size: 10,
      query: {
        multi_match: {
          query,
          fields,
          operator: 'and',
          type: 'cross_fields',
        },
      },
      sort: [
        { ts: { order: 'desc' } },
      ],
    },
    service: 'es',
    region: 'us-east-1',
    method: 'GET',
    headers: {
      host: url,
      'Content-Type': 'application/json',
    },
  };

  if (from) {
    opts.body.from = from;
  }

  opts.body = JSON.stringify(opts.body);

  const response = await sendSignedRequest(opts);
  console.log(JSON.stringify(response.data));
  if (response.data && response.data.hits) {
    // return {
    //   total: response.data.hits.total.value,
    //   results: response.data.hits.hits.map(hit => JSON.parse(hit._source.raw))
    // };
    const user = await getUser(userId);
    const token = symmetricDecrypt(user.encryptedToken);
    const tokenSecret = symmetricDecrypt(user.encryptedTokenSecret);
    const T = new twit({
      consumer_key: TWITTER_API_KEY,
      consumer_secret: TWITTER_API_SECRET,
      access_token: token,
      access_token_secret: tokenSecret,
    });
    const httpResult = await T.post(
      'statuses/lookup',
      {
        id: response.data.hits.hits.map(hit => hit._id).join(','),
        include_entities: true,
      },
    );
    return {
      total: response.data.hits.total.value,
      results: httpResult.data,
    };
  }
  return {
    total: 0,
    results: [],
  };
};

const sleep = t => new Promise(r => setTimeout(r, t));

const getUserData = async (userId) => {
  const s3 = new aws.S3();
  let result = null;

  try {
    const text = await s3.getObject({
      Bucket: 'faveindex-userdata',
      Key: userId.split('|')[1],
    }).promise();
    result = JSON.parse(text.Body.toString());
  } catch (e) {
    if (e.message === 'Access Denied') {
      result = {
        ids: [],
      };
      await s3.putObject({
        Bucket: 'faveindex-userdata',
        Key: userId.split('|')[1],
        Body: Buffer.from(JSON.stringify(result), 'utf8'),
      }).promise();
      console.log('Successfully put new data file.');
    }
  }

  return result;
}

const uniqueElements = array => array.filter((value, index, self)=>self.indexOf(value)===index);

export const runIndexTask = async (userId, rounds, sleepLength = 12) => {
  const user = await getUser(userId);
  const id = user.id.split('|')[1];
  const s3 = new aws.S3();
  if (!user) {
    throw new Error('User does not exist');
  }
  const token = symmetricDecrypt(user.encryptedToken);
  const tokenSecret = symmetricDecrypt(user.encryptedTokenSecret);

  /* Lock user */
  await updateUser(userId, {
    lock: 1,
  });

  /* First download all the tweet IDs */
  const userData = await getUserData(userId);
  if (!userData) {
    throw new Error('No user data');
  }
  const currIds = userData.ids;

  const T = new twit({
    consumer_key: TWITTER_API_KEY,
    consumer_secret: TWITTER_API_SECRET,
    access_token: token,
    access_token_secret: tokenSecret,
  });

  /* Then do a few rounds of tweets. */
  let next;
  const allTweetIds = [];
  for (let i = 0; i < rounds; i += 1) {
    const opts = { count: 200 };
    if (next) opts.max_id = next;
    const tweets = await T.get(
      'favorites/list', opts,
    );
    const tweetIds = tweets.data.map(t => t.id_str);
    tweetIds.sort(compare);
    next = tweetIds[0];
    allTweetIds.push(...tweetIds);

    console.log('Retrieved another ' + tweetIds.length + ' tweets');

    await sleep(sleepLength * 1000);
  }

  /* Filter out tweet IDs that we've already indexed */
  const filtered = uniqueElements(allTweetIds.filter(tweetId => !currIds.includes(tweetId)));
  currIds.push(...filtered);

  /* Hydrate all the new tweets (up to 100 at a time) */
  const total = filtered.length;
  const hydrateRounds = Math.round(total / 100) + 1;
  const fullTweets = [];
  for (let i = 0; i < hydrateRounds; i += 1) {
    const response = await T.post(
      'statuses/lookup',
      {
        id: filtered.slice(i * 100, (i + 1) * 100).join(','),
        include_entities: true,
      },
    );
    fullTweets.push(...response.data);
    console.log('Hydrated another ' + response.data.length + ' tweets');
    await sleep(1000);
  }

  /* Now that we have hydrated tweets, index them in bulk to ES */
  const numIndexRounds = Math.ceil(fullTweets.length / 200);

  for (let j = 0; j < numIndexRounds; j += 1) {
    let requestBody = '';
    const hydrated = fullTweets.slice(j * 200, (j + 1) * 200);
    for (let i = 0; i < hydrated.length; i += 1) {
      const indexObj = {
        id: hydrated[i].id_str,
        ts: moment(hydrated[i].created_at, 'ddd MMM D HH:mm:ss ZZ YYYY').unix(),
        text: hydrated[i].text,
        user_name: hydrated[i].user.name,
        user_handle: hydrated[i].user.screen_name,
      };
      if (hydrated[i].is_quote_status && hydrated[i].quoted_status) {
        Object.assign(indexObj, {
          quoted_text: hydrated[i].quoted_status.text,
          quoted_user_name: hydrated[i].quoted_status.user.name,
          quoted_user_handle: hydrated[i].quoted_status.user.screen_name,
        });
      }

      // TODO: Find new ways to extract info from tweets

      const line = JSON.stringify(indexObj);

      requestBody += JSON.stringify({
        index: {
          _index: 'tweets_' + id,
          _type: '_doc',
          _id: hydrated[i].id_str,
        },
      }) + '\n';
      requestBody += line + '\n';
    }

    if (hydrated.length) {
      /* Send bulk indexing request */
      const opts = {
        host: url,
        path: '/tweets_' + id + '/_bulk',
        service: 'es',
        region: 'us-east-1',
        method: 'POST',
        body: requestBody,
        headers: {
          host: url,
          'Content-Type': 'application/x-ndjson',
        },
        maxBodyLength: Infinity,
        maxContentLength: Infinity,
      };

      try {
        await sendSignedRequest(opts);
      } catch (e) {
        console.log(e.response.data);
      }

      console.log('Indexed ' + hydrated.length + ' tweets');
    }
  }

  /* Update user object. */
  await updateUser(userId, {
    indexedEntries: currIds.length,
    lastIndexTime: moment().format(),
    lock: 0,
  });

  /* Update userdata */
  await s3.putObject({
    Bucket: 'faveindex-userdata',
    Key: id,
    Body: Buffer.from(JSON.stringify({ ids: currIds }), 'utf8'),
  }).promise();

  console.log('== FINISHED INDEXING ==');

  return;
}

const UID_CHARS = 'QWERTYUIOPASDFGHJKLZXCVBNMqwertyuiopasdfghjklzxcvbnm1234567890';

export const generateUID = (length, chars = UID_CHARS) => {
  let str = '';
  for (let i = 0; i < length; i += 1) {
    const choice = Math.floor(Math.random() * chars.length);
    str = str + chars[choice];
  }
  return str;
};

const deleteMessage = async (handle) => {
  console.log('deleting message with handle ' + handle);
  try {
    await sqs.deleteMessage({
      QueueUrl: 'https://sqs.us-east-1.amazonaws.com/804334343084/FaveIndex',
      ReceiptHandle: handle,
    }).promise();
  } catch (e) {
    console.error(e);
  }
}

const sendMessage = async (data) => {
  await sqs.sendMessage({
    DelaySeconds: 0,
    MessageBody: JSON.stringify(data),
    QueueUrl: 'https://sqs.us-east-1.amazonaws.com/804334343084/FaveIndex',
  }).promise();
};

const receiveMessage = async () => {
  let data;
  try {
    data = await sqs.receiveMessage({
      AttributeNames: ['All'],
      MaxNumberOfMessages: 10,
      QueueUrl: 'https://sqs.us-east-1.amazonaws.com/804334343084/FaveIndex',
      VisibilityTimeout: 300,
    }).promise();
  } catch (e) {
    console.error(e);
    return [];
  }

  let result = [];

  if (data.Messages && data.Messages.length) {
    const unique = [];
    for (let i = 0; i < data.Messages.length; i += 1) {
      if (unique.every(x => JSON.parse(data.Messages[i].Body).id !== x.id)) {
        unique.push({
          ...JSON.parse(data.Messages[i].Body),
          handle: data.Messages[i].ReceiptHandle,
        });
      }
    }

    result = unique;
  } else {
    result = [];
  }

  return result;
}

export const scheduleTask = async (data) => {
  sendMessage(data);
};

export const runLoop = () => {
  setInterval(
    async () => {
      const newTasks = [];
      for (let i = 0; i < 3; i += 1) {
        const received = await receiveMessage();
        for (let j = 0; j < received.length; j += 1) {
          if (newTasks.every(task => task.id !== received[j].id)) {
            newTasks.push(received[j]);
          }
        }
      }

      if (newTasks.length) {
        newTasks.forEach(async (newTask) => {
          const { lock, ...user } = await getUser(newTask.id);

          if (user.lock) {
            return;
          }

          await deleteMessage(newTask.handle);
          await runIndexTask(newTask.id, newTask.amount);
        });
      } else {
        console.error('Did not get any message');
      }
    },
    12000,
  );
};

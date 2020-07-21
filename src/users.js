
import aws from 'aws-sdk';

aws.config.update({ region: 'us-east-1' });

const dynamo = new aws.DynamoDB({
  apiVersion: '2012-08-10',
});

const objToTypes = (data) => {
  const keys = Object.keys(data);
  const result = {};

  for (let i = 0; i < keys.length; i += 1) {
    if (typeof data[keys[i]] === 'string') {
      result[keys[i]] = { S: data[keys[i]] };
    } else if (typeof data[keys[i]] === 'number') {
      result[keys[i]] = { N: `${data[keys[i]]}` };
    }
  }

  return result;
};

const typesToObj = (data) => {
  const user = {};
  const keys = Object.keys(data);
  for (let i = 0; i < keys.length; i += 1) {
    const type = Object.keys(data[keys[i]])[0];
    if (type === 'N') {
      user[keys[i]] = parseFloat(data[keys[i]][type]);
    } else if (type === 'S') {
      user[keys[i]] = data[keys[i]][type];
    }
  }
  return user;
}

export const createUser = async (id, data) => {
  const params = {
    Item: {
      id: {
        S: id,
      },
    },
    TableName: 'FaveIndexUsers',
  };

  Object.assign(params.Item, objToTypes(data));
  return dynamo.putItem(params).promise();
};

export const getUser = async (id) => {
  const params = {
    Key: {
      id: {
        S: id,
      },
    },
    TableName: 'FaveIndexUsers',
  };

  const data = await dynamo.getItem(params).promise();
  if (!data.Item) {
    return null;
  }
  const user = {};
  const keys = Object.keys(data.Item);
  for (let i = 0; i < keys.length; i += 1) {
    const type = Object.keys(data.Item[keys[i]])[0];
    if (type === 'N') {
      user[keys[i]] = parseFloat(data.Item[keys[i]][type]);
    } else if (type === 'S') {
      user[keys[i]] = data.Item[keys[i]][type];
    }
  }
  return user;
};

export const updateUser = async (id, updateKeys) => {
  const keys = Object.keys(updateKeys);
  const ean = {};
  const eav = {};
  const exprTerms = [];
  for (let i = 0; i < keys.length; i += 1) {
    ean[`#${i + 1}`] = keys[i];
    eav[`:${i + 1}`] = updateKeys[keys[i]];
    exprTerms.push(`#${i + 1} = :${i + 1}`);
  }

  const params = {
    TableName: 'FaveIndexUsers',
    Key: {
      id: {
        S: id,
      },
    },
    ExpressionAttributeNames: ean,
    ExpressionAttributeValues: objToTypes(eav),
    UpdateExpression: `SET ${exprTerms.join(', ')}`,
  };

  return dynamo.updateItem(params).promise();
};

export const deleteUser = async (id) => {
  const params = {
    Key: {
      id: {
        S: id,
      },
    },
    TableName: 'FaveIndexUsers',
  };

  return dynamo.deleteItem(params).promise();
};

export const scanUsers = async () => {
  const users = await dynamo.scan({ TableName: 'FaveIndexUsers' }).promise();
  return users.Items.map(typesToObj);
}

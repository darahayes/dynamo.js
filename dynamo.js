'use strict';
const AWS = require('aws-sdk');
const debug = require('debug')('dynamo.js');

module.exports = function(awsConfig) {
  const ddb = new AWS.DynamoDB(awsConfig);
  const dynamo = new AWS.DynamoDB.DocumentClient({service: ddb});

  function create(obj, hashKey, table, callback) {
    debug(JSON.stringify(getCreateParams(obj, hashKey, table)))
    dynamo.put(getCreateParams(obj, hashKey, table), (err, data) => {
      if (err) {
        return callback(err);
      }
      debug('object created', obj);
      callback(null, obj);
    });
  }

  function updateItem(tableName, hashKey, toUpdate, callback) {
    if (Object.keys(hashKey).length == 1) {
      let params = getUpdateParams(tableName, hashKey, toUpdate);
      debug(params);
      dynamo.update(params, (err, updated) => {
        if (err) {
          return callback(err);
        }
        return callback(null, updated.Attributes);
      });
    }
    else {
      return callback(new Error('Invalid HashKey', JSON.stringify(hashKey)));
    }
  }

  function deleteItem(hashKey, tableName, callback) {
    if (Object.keys(hashKey).length == 1) {
      let params = {
        TableName : tableName,
        Key: hashKey
      };
      debug(params);
      dynamo.delete(params, (err, result) => {
        if (err) {
          debug(err);
          return callback(err);
        }
        debug('deleted: ', result);
        return callback(null, result);
      });
    }
    else {
      return callback(new Error('Invalid HashKey', JSON.stringify(hashKey)))
    }
  }

  function findByHashKey(hashKey, tableName, cb) {
    //ensure a valid hashKey was supplied
    if (Object.keys(hashKey).length == 1) {
      let params = {
        TableName : tableName,
        Key: hashKey
      };
      debug(params);
      dynamo.get(params, (err, result) => {
        if (err) {
          return cb(err);
        }
        return cb(null, result.Item);
      });
    }
    else {
      return cb(new Error('Invalid HashKey', JSON.stringify(hashKey)))
    }
  }

  function findBy(attributeName, attribute, tableName, cb) {
    let params = buildScanParams(attributeName, attribute, tableName);
    debug(JSON.stringify(params, null, 2))
    dynamo.scan(params, cb);
  }

  //see http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB/DocumentClient.html#update-property
  function getUpdateParams(tableName, hashKey, toUpdate) {
    let params = {
      TableName: tableName,
      Key: hashKey,
      AttributeUpdates: {},
      ReturnValues: 'ALL_NEW'
    };
    Object.keys(toUpdate).forEach((key) => {
      params.AttributeUpdates[key] = {
        Action: 'PUT',
        Value: toUpdate[key]
      }
    })
    return params;
  }

  // http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB/DocumentClient.html#put-property
  function getCreateParams(obj, hashKey, table) {
    return {
      Item: obj,
      TableName: table,
      ConditionExpression: 'attribute_not_exists(' + hashKey + ')' //ensure uniqueness
    }
  }

  //see http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB/DocumentClient.html#scan-property
  function buildScanParams(attributeName, attribute, tableName) {
    let params = {ExpressionAttributeValues: {}};
    params.TableName = tableName;
    params.FilterExpression = attributeName + ' = :' + attributeName;
    params.ExpressionAttributeValues[':'+ attributeName] = attribute;
    return params;
  }

  return {
    create: create,
    updateItem: updateItem,
    findBy: findBy,
    findByHashKey: findByHashKey,
    deleteItem: deleteItem
  }

}
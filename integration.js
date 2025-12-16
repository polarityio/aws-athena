'use strict';

const async = require('async');
const path = require('path');
const { 
  AthenaClient, 
  StartQueryExecutionCommand, 
  GetQueryExecutionCommand, 
  GetQueryResultsCommand,
  CreatePreparedStatementCommand,
  UpdatePreparedStatementCommand,
  GetPreparedStatementCommand
} = require("@aws-sdk/client-athena");
const { get } = require('lodash');
const { DateTime } = require('luxon');



let Logger;
let originalOptions = {};
let athenaClient = null;
let currentPreparedStatement = null;
let currentQuery = null;

const origWarning = process.emitWarning;
process.emitWarning = function (...args) {
  if (Array.isArray(args) && args.length > 0 && args[0].startsWith('The AWS SDK for JavaScript (v3) will')) {
    // Log the deprecation in our integration logs but don't bubble it up on stderr
    Logger.warn({ args }, 'Node12 Deprecation Warning');
  } else {
    // pass any other warnings through normally
    return origWarning.apply(process, args);
  }
};

function startup(logger) {
  Logger = logger;
}

/**
 * Used to escape single quotes in entities and remove any newlines
 * For parameterized queries with EXECUTE/USING, we still need basic escaping for the parameter value
 * @param entityValue
 * @returns {*}
 */
function escapeEntityValue(entityValue) {
  const escapedValue = entityValue
    .replace(/(\r\n|\n|\r)/gm, '')
    .replace(/\\/g, '\\\\')
    .replace(/'/g, "\\'");
  Logger.trace({ entityValue, escapedValue }, 'Escaped Entity Value');
  return escapedValue;
}

function parseTypeHints(query) {
  // Parse ?:<type> syntax and extract type information
  const typeHints = [];
  let parameterIndex = 0;
  
  // Match both ? and ?:<type> patterns
  const modifiedQuery = query.replace(/\?(?::(\w+))?/g, (match, typeHint) => {
    typeHints.push({
      index: parameterIndex,
      type: typeHint || 'string' // Default to string if no type specified
    });
    parameterIndex++;
    return '?'; // Replace with plain ? for the prepared statement
  });
  
  return {
    query: modifiedQuery,
    typeHints: typeHints
  };
}

async function ensurePreparedStatement(query, options) {
  // Get the directory name to make the statement name unique
  // Replace spaces with underscores to ensure valid statement name
  const directoryName = path.basename(__dirname).replace(/\s+/g, '_');
  const statementName = `polarity_prepared_statement_${directoryName}`;
  
  // Parse type hints from the query (converts ?:<type> to ? and extracts type info)
  const { query: processedQuery, typeHints } = parseTypeHints(query);
  
  // Store type hints for parameter creation
  options._typeHints = typeHints;
  
  // Check if we need to create or update the prepared statement
  if (currentQuery !== processedQuery || currentPreparedStatement === null) {
    
    // Always check if the prepared statement exists first
    const getCommand = new GetPreparedStatementCommand({
      StatementName: statementName,
      WorkGroup: options.workGroup || 'primary'
    });
    
    let statementExists;
    try {
      await athenaClient.send(getCommand);
      statementExists = true;
      Logger.trace({ statementName }, 'Prepared statement exists');
    } catch (error) {
      // Statement doesn't exist (ResourceNotFoundException or similar)
      statementExists = false;
      Logger.trace({ statementName }, 'Prepared statement does not exist');
    }
    
    if (statementExists) {
      // Statement exists, update it with the new query
      Logger.trace({ statementName, originalQuery: query, processedQuery, typeHints }, 'Updating existing prepared statement');
      
      const updateCommand = new UpdatePreparedStatementCommand({
        StatementName: statementName,
        QueryStatement: processedQuery,
        WorkGroup: options.workGroup || 'primary'
      });
      
      await athenaClient.send(updateCommand);
      Logger.trace({ statementName }, 'Successfully updated prepared statement');
    } else {
      // Statement doesn't exist, create it
      Logger.trace({ statementName, originalQuery: query, processedQuery, typeHints }, 'Creating new prepared statement');
      
      const createCommand = new CreatePreparedStatementCommand({
        StatementName: statementName,
        QueryStatement: processedQuery,
        WorkGroup: options.workGroup || 'primary'
      });
      
      await athenaClient.send(createCommand);
      Logger.trace({ statementName }, 'Successfully created prepared statement');
    }
    
    // Update our cached values
    currentPreparedStatement = statementName;
    currentQuery = processedQuery;
  }
  
  return currentPreparedStatement;
}

function createParameterValue(entityValue, type = 'string') {
  const escapedValue = escapeEntityValue(entityValue);
  
  switch (type.toLowerCase()) {
    case 'integer':
    case 'int':
    case 'bigint':
      const intValue = parseInt(entityValue, 10);
      if (isNaN(intValue)) {
        throw new Error(`Cannot convert "${entityValue}" to integer type`);
      }
      return String(intValue);
      
    case 'decimal':
    case 'double':
    case 'float':
    case 'real':
      const floatValue = parseFloat(entityValue);
      if (isNaN(floatValue)) {
        throw new Error(`Cannot convert "${entityValue}" to numeric type`);
      }
      return String(floatValue);
      
    case 'boolean':
    case 'bool':
      const lowerValue = entityValue.toLowerCase();
      if (lowerValue === 'true' || lowerValue === '1') {
        return 'true';
      } else if (lowerValue === 'false' || lowerValue === '0') {
        return 'false';
      } else {
        throw new Error(`Cannot convert "${entityValue}" to boolean type`);
      }
      
    case 'string':
    case 'varchar':
    case 'char':
    default:
      return `'${escapedValue}'`;
  }
}

async function createQuery(entity, options) {
  // Always use prepared statements, but handle both parameterized and non-parameterized queries
  const preparedStatementName = await ensurePreparedStatement(options.query, options);
  
  let queryString;
  if (options.query.includes('?')) {
    // Get type hints that were extracted during prepared statement creation
    const typeHints = options._typeHints || [];
    const parameterCount = typeHints.length;
    
    // Create parameter list using type hints
    const parameters = [];
    for (let i = 0; i < parameterCount; i++) {
      const typeHint = typeHints[i];
      try {
        const parameterValue = createParameterValue(entity.value, typeHint.type);
        parameters.push(parameterValue);
      } catch (error) {
        Logger.error({ 
          error: error.message, 
          entityValue: entity.value, 
          expectedType: typeHint.type,
          parameterIndex: i 
        }, 'Failed to convert entity value to expected type');
        throw new Error(`Parameter ${i + 1}: ${error.message}`);
      }
    }
    
    const parametersString = parameters.join(', ');
    
    // Query has parameters, use EXECUTE with USING
    queryString = `EXECUTE ${preparedStatementName} USING ${parametersString}`;
    
    Logger.trace({ 
      parameterCount, 
      parameters: parametersString, 
      entityValue: entity.value, 
      typeHints,
      query: options.query 
    }, 'Created type-hinted parameterized query execution');
  } else {
    // Query has no parameters, use EXECUTE without USING
    queryString = `EXECUTE ${preparedStatementName}`;
  }
  
  const queryParams = {
    QueryString: queryString,
    WorkGroup: options.workGroup || 'primary'
  };
  
  // Only specify ResultConfiguration if OutputLocation is provided
  // If WorkGroup has default ResultConfiguration, it will be used automatically
  if (options.outputLocation && options.outputLocation.trim().length > 0) {
    queryParams.ResultConfiguration = {
      OutputLocation: options.outputLocation
    };
  }
  
  return queryParams;
}

async function executeAthenaQuery(athenaClient, queryParams, options) {
  // Start query execution
  const startCommand = new StartQueryExecutionCommand(queryParams);
  let startResult;
  
  try {
    startResult = await athenaClient.send(startCommand);
  } catch (error) {
    // Handle specific S3 bucket errors with better messaging
    if (error.name === 'InvalidRequestException' && error.message && error.message.includes('output bucket')) {
      const bucketMatch = error.message.match(/bucket ([^\s]+)/);
      const bucketName = bucketMatch ? bucketMatch[1] : 'unknown';
      
      throw new Error(`S3 output bucket error: ${error.message}. Please ensure the bucket '${bucketName}' exists and the IAM user has s3:PutObject and s3:GetBucketLocation permissions, or configure a WorkGroup with a default output location.`);
    }
    
    // For other errors, create a clean error object without circular references
    throw new Error(`Failed to start Athena query: ${error.message || error.name || 'Unknown error'}`);
  }
  
  const queryExecutionId = startResult.QueryExecutionId;
  Logger.trace({ queryExecutionId }, 'Started Athena query execution');
  
  // Wait for query to complete with 45 second timeout
  let queryStatus = 'RUNNING';
  let attempts = 0;
  let statusResult = null;
  const maxAttempts = 15; // 45 seconds / 3 second intervals = 15 attempts
  const waitInterval = 3000; // 3 second intervals
  
  while (queryStatus === 'RUNNING' || queryStatus === 'QUEUED') {
    if (attempts >= maxAttempts) {
      Logger.trace({ queryExecutionId, attempts }, 'Query execution timeout reached, returning queryExecutionId for later polling');
      return {
        results: undefined,
        complete: false,
        queryExecutionId: queryExecutionId
      };
    }
    
    await new Promise(resolve => setTimeout(resolve, waitInterval));
    
    const statusCommand = new GetQueryExecutionCommand({ QueryExecutionId: queryExecutionId });
    statusResult = await athenaClient.send(statusCommand);
    queryStatus = statusResult.QueryExecution.Status.State;
    
    Logger.trace({ queryStatus, attempts, queryExecutionId }, 'Athena query status check');
    attempts++;
  }
  
  if (queryStatus === 'FAILED' || queryStatus === 'CANCELLED') {
    const errorMsg = queryStatus === 'FAILED' 
      ? `Query failed: ${statusResult.QueryExecution.Status.StateChangeReason}`
      : 'Query was cancelled';
    throw new Error(errorMsg);
  }
  
  // Get query results
  const resultsCommand = new GetQueryResultsCommand({ 
    QueryExecutionId: queryExecutionId,
    MaxResults: options.limit || 100
  });
  const resultsResponse = await athenaClient.send(resultsCommand);
  
  Logger.trace({ resultSetMetadata: resultsResponse.ResultSet.ResultSetMetadata }, 'Athena query results metadata');
  
  // Convert Athena results to JSON format
  const rows = resultsResponse.ResultSet.Rows;
  if (!rows || rows.length === 0) {
    return {
      results: [],
      complete: true,
      queryExecutionId: queryExecutionId
    };
  }
  
  // First row contains column headers
  const headers = rows[0].Data.map(col => col.VarCharValue);
  
  // Convert remaining rows to objects
  const results = rows.slice(1).map(row => {
    const obj = {};
    row.Data.forEach((col, index) => {
      obj[headers[index]] = col.VarCharValue;
    });
    return obj;
  });
  
  return {
    results: results,
    complete: true,
    queryExecutionId: queryExecutionId
  };
}

function optionsHaveChanged(options) {
  if (
    originalOptions.region !== options.region ||
    originalOptions.endpoint !== options.endpoint ||
    originalOptions.accessKeyId !== options.accessKeyId ||
    originalOptions.secretAccessKey !== options.secretAccessKey
  ) {
    originalOptions = options;
    return true;
  }
  return false;
}

function summaryTagsOptionHasChanged(options) {
  if (originalOptions.summaryTags !== options.summaryTags) {
    originalOptions = options;
    return true;
  }

  return false;
}

function errorToPojo(err, detail) {
  if (err instanceof Error) {
    // Create a clean error object without circular references
    const cleanError = {
      name: err.name,
      message: err.message,
      detail: detail || err.message || 'Unexpected error encountered'
    };
    
    // Only include stack trace if it exists and doesn't contain circular references
    if (err.stack && typeof err.stack === 'string') {
      cleanError.stack = err.stack;
    }
    
    // Handle AWS SDK specific error properties safely
    if (err.$metadata) {
      cleanError.requestId = err.$metadata.requestId;
      cleanError.httpStatusCode = err.$metadata.httpStatusCode;
    }
    
    return cleanError;
  }
  
  // If it's not an Error object, return it as-is (but this shouldn't happen)
  return err;
}

function parseAttribute(attribute, parser) {
  // Handle null/undefined attributes
  if (attribute === null || attribute === undefined) {
    return '';
  }

  try {
    switch (parser) {
      case null:
      case undefined:
        return attribute;
      case 'date-iso':
        const isoDate = DateTime.fromISO(attribute);
        return isoDate.isValid ? isoDate.toLocaleString(DateTime.DATETIME_SHORT) : attribute;
      case 'date-http':
        const httpDate = DateTime.fromHTTP(attribute);
        return httpDate.isValid ? httpDate.toLocaleString(DateTime.DATETIME_SHORT) : attribute;
      case 'date-rfc2822':
        const rfcDate = DateTime.fromRFC2822(attribute);
        return rfcDate.isValid ? rfcDate.toLocaleString(DateTime.DATETIME_SHORT) : attribute;
      case 'date-sql':
        const sqlDate = DateTime.fromSQL(attribute);
        return sqlDate.isValid ? sqlDate.toLocaleString(DateTime.DATETIME_SHORT) : attribute;
      case 'date-seconds':
        const secondsDate = DateTime.fromSeconds(+attribute);
        return secondsDate.isValid ? secondsDate.toLocaleString(DateTime.DATETIME_SHORT) : attribute;
      case 'date-millis':
        // Fixed: Use fromMillis instead of fromSeconds for milliseconds
        const millisDate = DateTime.fromMillis(+attribute);
        return millisDate.isValid ? millisDate.toLocaleString(DateTime.DATETIME_SHORT) : attribute;
      default:
        return attribute;
    }
  } catch (error) {
    Logger.trace({ attribute, parser, error }, 'Error parsing attribute, returning original value');
    return attribute;
  }
}

/**
 * Takes the attribute option string which is a comma delimited list of attributes to display and converts
 * it into an object of the format:
 * ```
 * {
 *     label: <attribute label>,
 *     attribute: <attribute name>,
 *     parser: <attribute parser>
 * }
 * ```
 * The label is optional and is the display label to be used for the attribute.  If no label is provided then the
 * attribute name is used
 *
 * The attribute is the "name" of the attribute
 *
 * The parser is the parser to use if specified for converting the attribute value (currently supports various
 * date parsers for Athena timestamp and date fields).
 * then
 * @param attributeOption
 * @returns {*}
 */
function processAttributeOption(attributeOption) {
  const fields = attributeOption.split(',').map((column) => {
    const tokens = column.split(':');
    if (tokens.length === 1) {
      return {
        label: tokens[0].trim(),
        attribute: tokens[0].trim(),
        parser: null
      };
    } else if (tokens.length === 2) {
      return {
        label: tokens[0].trim(),
        attribute: tokens[1].trim(),
        parser: null
      };
    } else if (tokens.length === 3) {
      return {
        label: tokens[0].trim(),
        attribute: tokens[2].trim(),
        parser: tokens[1].trim().toLowerCase()
      };
    }
  });

  return fields;
}

function getDocumentTitle(result, options) {
  if (!options.documentTitleAttribute || typeof options.documentTitleAttribute !== 'string' || options.documentTitleAttribute.trim().length === 0) {
    return null;
  }

  const titleAttributes = processAttributeOption(options.documentTitleAttribute);

  if (titleAttributes[0]) {
    const attributeObj = titleAttributes[0];
    const attributeValue = get(result, attributeObj.attribute);
    const parsedValue = parseAttribute(attributeValue, attributeObj.parser);
    if (attributeObj.label) {
      return `${attributeObj.label}: ${parsedValue}`;
    } else {
      return parsedValue;
    }
  }

  return null;
}

function getDetails(results, options) {
  // If no detail attributes are specified then we just display the
  // whatever Athena returns using the JSON viewer
  if (!options.detailAttributes || typeof options.detailAttributes !== 'string' || options.detailAttributes.trim().length === 0) {
    return {
      showAsJson: true,
      results
    };
  }

  const details = [];
  const detailAttributes = processAttributeOption(options.detailAttributes);

  results.forEach((result) => {
    const document = [];
    detailAttributes.forEach((attributeObj) => {
      const attributeValue = get(result, attributeObj.attribute);
      if (attributeValue !== undefined && attributeValue !== null && attributeValue !== '') {
        document.push({
          key: attributeObj.label,
          value: parseAttribute(attributeValue, attributeObj.parser)
        });
      }
    });

    if (document.length > 0) {
      // Create resultAsString by concatenating all attribute values with spaces
      // Handle nested JSON values by converting objects/arrays to searchable strings
      const resultAsString = document.map(attr => {
        const value = attr.value;
        if (typeof value === 'object' && value !== null) {
          // For objects/arrays, convert to JSON string for filtering
          return JSON.stringify(value).toLowerCase();
        }
        // For primitive values, return as-is
        return String(value).toLowerCase();
      }).join(' ');
      
      details.push({
        title: getDocumentTitle(result, options),
        attributes: document,
        resultAsString: resultAsString
      });
    }
  });

  return {
    showAsJson: false,
    results: details
  };
}

function getSummaryTags(results, options) {
  const tags = [];
  
  // Check if summaryAttributes is defined and not empty
  if (!options.summaryAttributes || typeof options.summaryAttributes !== 'string' || options.summaryAttributes.trim().length === 0) {
    // No summary attributes configured, just return result count
    tags.push(`${results.length} ${results.length === 1 ? 'result' : 'results'}`);
    return tags;
  }

  const summaryAttributes = processAttributeOption(options.summaryAttributes);

  summaryAttributes.forEach((attributeObj) => {
    for (let i = 0; i < options.maxSummaryDocuments && i < results.length; i++) {
      const result = results[i];
      const tag = get(result, attributeObj.attribute);
      if (tag !== undefined && tag !== null && tag !== '') {
        if (attributeObj.label) {
          tags.push(`${attributeObj.label}: ${parseAttribute(tag, attributeObj.parser)}`);
        } else {
          tags.push(parseAttribute(tag, attributeObj.parser));
        }
      }
    }
  });

  if (tags.length === 0 || results.length > options.maxSummaryDocuments) {
    tags.push(`${results.length} ${results.length === 1 ? 'result' : 'results'}`);
  }

  return tags;
}

async function doLookup(entities, options, cb) {
  let lookupResults;

  if (optionsHaveChanged(options) || athenaClient === null) {
    const clientOptions = {
      region: options.region.value
    };

    if (options.accessKeyId.length > 0 || options.secretAccessKey.length > 0) {
      clientOptions.credentials = {
        accessKeyId: options.accessKeyId,
        secretAccessKey: options.secretAccessKey
      };
    }

    Logger.trace({ clientOptions }, 'Creating new Athena client');
    athenaClient = new AthenaClient(clientOptions);
    
    // Reset prepared statement variables when options change
    // The existing prepared statement will remain in Athena and can be reused
    currentPreparedStatement = null;
    currentQuery = null;
  }

  const searchTasks = entities.map((entity) => {
    return async () => {
      const queryParams = await createQuery(entity, options);
      Logger.trace({ queryParams }, 'Athena SQL query parameters');
      
      const queryResult = await executeAthenaQuery(athenaClient, queryParams, options);
      Logger.trace({ queryResult }, 'Athena Query Result Object');
      
      if (!queryResult.complete) {
        // Query is still running, return queryExecutionId for later polling
        Logger.trace({ queryExecutionId: queryResult.queryExecutionId }, 'Query still running, returning execution ID');
        return {
          entity,
          data: {
            summary: ['Query Running'],
            details: {
              showAsJson: false,
              results: [{
                title: 'Query Status',
                attributes: [
                  { key: 'Status', value: 'Running' },
                  { key: 'Query Execution ID', value: queryResult.queryExecutionId }
                ]
              }]
            }
          }
        };
      }
      
      if (!Array.isArray(queryResult.results) || queryResult.results.length === 0) {
        return {
          entity,
          data: null
        };
      } else {
        Logger.trace({ results: queryResult.results }, 'JSON Results from Athena');
        return {
          entity,
          data: {
            summary: getSummaryTags(queryResult.results, options),
            details: getDetails(queryResult.results, options)
          }
        };
      }
    };
  });

  try {
    lookupResults = await async.parallelLimit(searchTasks, 10);
  } catch (lookupError) {
    Logger.error(lookupError, 'doLookup error');
    return cb(errorToPojo(lookupError, 'Error running Athena SQL query'));
  }

  Logger.trace({ lookupResults }, 'lookup results');

  cb(null, lookupResults);
}

function validateOptions(userOptions, cb) {
  let errors = [];
  
  // Validate required accessKeyId field
  if (
    typeof userOptions.accessKeyId !== 'string' ||
    (typeof userOptions.accessKeyId === 'string' && userOptions.accessKeyId.trim().length === 0)
  ) {
    errors.push({
      key: 'accessKeyId',
      message: 'You must provide a valid AWS Access Key ID'
    });
  }

  // Validate required secretAccessKey field
  if (
    typeof userOptions.secretAccessKey !== 'string' ||
    (typeof userOptions.secretAccessKey === 'string' && userOptions.secretAccessKey.trim().length === 0)
  ) {
    errors.push({
      key: 'secretAccessKey',
      message: 'You must provide a valid AWS Secret Access Key'
    });
  }

  // Validate required query field
  if (
    typeof userOptions.query.value !== 'string' ||
    (typeof userOptions.query.value === 'string' && userOptions.query.value.length === 0)
  ) {
    errors.push({
      key: 'query',
      message: 'You must provide a valid SQL Query'
    });
  }

  cb(null, errors);
}

module.exports = {
  doLookup,
  startup,
  validateOptions
};

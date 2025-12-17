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
} = require('@aws-sdk/client-athena');
const { get } = require('lodash');
const { DateTime } = require('luxon');

// Athena query execution configuration constants
const MAX_QUERY_STATUS_POLLING_ATTEMPTS = 10; // 10 attempts * 3 second interval = 30 seconds wait time 
const POLLING_WAIT_INTERVAL = 3000; // 3 second intervals

let Logger;
let originalOptions = null;
let athenaClient = null;
let currentPreparedStatement = null;
let currentQuery = null;

// Cached processed attributes to avoid recomputing on every lookup
let cachedDocumentTitleAttributes = null;
let cachedDetailAttributes = null;
let cachedSummaryAttributes = null;
let lastDocumentTitleAttributeOption = null;
let lastDetailAttributesOption = null;
let lastSummaryAttributesOption = null;

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
      Logger.trace(
        {
          statementName,
          originalQuery: query,
          processedQuery,
          typeHints
        },
        'Updating existing prepared statement'
      );

      const updateCommand = new UpdatePreparedStatementCommand({
        StatementName: statementName,
        QueryStatement: processedQuery,
        WorkGroup: options.workGroup || 'primary'
      });

      await athenaClient.send(updateCommand);
      Logger.trace({ statementName }, 'Successfully updated prepared statement');
    } else {
      // Statement doesn't exist, create it
      Logger.trace(
        {
          statementName,
          originalQuery: query,
          processedQuery,
          typeHints
        },
        'Creating new prepared statement'
      );

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

async function createQuery(entity, options, preparedStatementName = null) {
  let queryString;

  if (options.query.includes('?')) {
    // Use the prepared statement name passed in (already ensured in doLookup)
    if (!preparedStatementName) {
      throw new Error('Prepared statement name required for parameterized queries');
    }

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
        Logger.error(
          {
            error: error.message,
            entityValue: entity.value,
            expectedType: typeHint.type,
            parameterIndex: i
          },
          'Failed to convert entity value to expected type'
        );
        throw new Error(`Parameter ${i + 1}: ${error.message}`);
      }
    }

    const parametersString = parameters.join(', ');

    // Query has parameters, use EXECUTE with USING
    queryString = `EXECUTE ${preparedStatementName} USING ${parametersString}`;

    Logger.trace(
      {
        parameterCount,
        parameters: parametersString,
        entityValue: entity.value,
        typeHints,
        query: options.query
      },
      'Created type-hinted parameterized query execution'
    );
  } else {
    // Query has no parameters, execute the raw query directly
    queryString = options.query;
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

      throw new Error(
        `S3 output bucket error: ${error.message}. Please ensure the bucket '${bucketName}' exists and the IAM user has s3:PutObject and s3:GetBucketLocation permissions, or configure a WorkGroup with a default output location.`
      );
    }

    // For other errors, create a clean error object without circular references
    throw new Error(`Failed to start Athena query: ${error.message || error.name || 'Unknown error'}`);
  }

  const queryExecutionId = startResult.QueryExecutionId;
  Logger.trace({ queryExecutionId }, 'Started Athena query execution');

  // Wait for query to complete with timeout
  let queryStatus = 'RUNNING';
  let attempts = 0;
  let statusResult = null;

  while (queryStatus === 'RUNNING' || queryStatus === 'QUEUED') {
    if (attempts >= MAX_QUERY_STATUS_POLLING_ATTEMPTS) {
      Logger.trace(
        {
          queryExecutionId,
          attempts
        },
        'Query execution timeout reached, returning queryExecutionId for later polling'
      );

      // Extract partial execution stats for running queries
      const partialStats = statusResult?.QueryExecution?.Statistics || {};
      const queryExecution = statusResult?.QueryExecution || {};
      const submissionTime = queryExecution.Status?.SubmissionDateTime;
      
      // Use helper function to calculate elapsed time
      const { elapsedMs, elapsedSeconds } = calculateElapsedTime(submissionTime);

      const incompleteStats = {
        runtimeMs: null, // Not available until completion
        runtimeSeconds: null,
        elapsedMs: elapsedMs,
        elapsedSeconds: elapsedSeconds,
        dataScannedBytes: partialStats.DataScannedInBytes || null,
        status: 'RUNNING'
      };

      return {
        results: undefined,
        complete: false,
        queryExecutionId: queryExecutionId,
        executionStats: incompleteStats
      };
    }

    await new Promise((resolve) => setTimeout(resolve, POLLING_WAIT_INTERVAL));

    const statusCommand = new GetQueryExecutionCommand({ QueryExecutionId: queryExecutionId });
    statusResult = await athenaClient.send(statusCommand);
    queryStatus = statusResult.QueryExecution.Status.State;

    Logger.trace({ queryStatus, attempts, queryExecutionId }, 'Athena query status check');
    attempts++;
  }

  if (queryStatus === 'FAILED' || queryStatus === 'CANCELLED') {
    const errorMsg =
      queryStatus === 'FAILED'
        ? `Query failed: ${statusResult.QueryExecution.Status.StateChangeReason}`
        : 'Query was cancelled';
    throw new Error(errorMsg);
  }

  // Extract execution statistics from Athena
  const executionStats = statusResult.QueryExecution.Statistics || {};
  const queryExecution = statusResult.QueryExecution || {};

  // Calculate runtime information
  const submissionTime = queryExecution.Status?.SubmissionDateTime;
  const completionTime = queryExecution.Status?.CompletionDateTime;
  let runtimeMs = null;
  let runtimeSeconds = null;

  if (submissionTime && completionTime) {
    runtimeMs = new Date(completionTime) - new Date(submissionTime);
    runtimeSeconds = (runtimeMs / 1000).toFixed(3);
  }

  // Athena provides execution time in milliseconds in Statistics.EngineExecutionTimeInMillis
  const athenaRuntimeMs = executionStats.EngineExecutionTimeInMillis || null;
  const athenaRuntimeSeconds = athenaRuntimeMs ? (athenaRuntimeMs / 1000).toFixed(3) : null;

  const queryStats = {
    runtimeMs: athenaRuntimeMs || runtimeMs,
    runtimeSeconds: athenaRuntimeSeconds || runtimeSeconds,
    dataScannedBytes: executionStats.DataScannedInBytes || null,
    dataProcessedBytes: executionStats.DataProcessedInBytes || null,
    queryQueueTimeMs: executionStats.QueryQueueTimeInMillis || null,
    queryPlanningTimeMs: executionStats.QueryPlanningTimeInMillis || null,
    serviceProcessingTimeMs: executionStats.ServiceProcessingTimeInMillis || null
  };

  Logger.trace({ queryStats, executionStats }, 'Athena query execution statistics');

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
      queryExecutionId: queryExecutionId,
      executionStats: queryStats
    };
  }

  // First row contains column headers
  const headers = rows[0].Data.map((col) => col.VarCharValue);

  // Convert remaining rows to objects
  const results = rows.slice(1).map((row) => {
    const obj = {};
    row.Data.forEach((col, index) => {
      obj[headers[index]] = col.VarCharValue;
    });
    return obj;
  });

  return {
    results: results,
    complete: true,
    queryExecutionId: queryExecutionId,
    executionStats: queryStats
  };
}

function optionsHaveChanged(options) {
  if (
    originalOptions === null ||
    originalOptions.region.value !== options.region.value ||
    originalOptions.endpoint !== options.endpoint ||
    originalOptions.accessKeyId !== options.accessKeyId ||
    originalOptions.secretAccessKey !== options.secretAccessKey
  ) {
    Logger.trace({ originalOptions, options }, 'Options have changed');
    originalOptions = { ...options };
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

function getDocumentTitle(result) {
  // Use cached document title attributes if available
  if (!cachedDocumentTitleAttributes || cachedDocumentTitleAttributes.length === 0) {
    return null;
  }

  const attributeObj = cachedDocumentTitleAttributes[0];
  const attributeValue = get(result, attributeObj.attribute);
  const parsedValue = parseAttribute(attributeValue, attributeObj.parser);
  if (attributeObj.label) {
    return `${attributeObj.label}: ${parsedValue}`;
  } else {
    return parsedValue;
  }
}

function getDetails(results, options, complete = true, queryExecutionId = null, executionStats = null) {
  // If no detail attributes are cached then we just display the
  // whatever Athena returns using the JSON viewer
  if (!cachedDetailAttributes || cachedDetailAttributes.length === 0) {
    return {
      showAsJson: true,
      results,
      complete: complete,
      queryExecutionId: queryExecutionId,
      executionStats: executionStats
    };
  }

  const details = [];
  // Use cached detail attributes instead of processing them again
  const detailAttributes = cachedDetailAttributes;

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
      const resultAsString = document
        .map((attr) => {
          const value = attr.value;
          if (typeof value === 'object' && value !== null) {
            // For objects/arrays, convert to JSON string for filtering
            return JSON.stringify(value).toLowerCase();
          }
          // For primitive values, return as-is
          return String(value).toLowerCase();
        })
        .join(' ');

      details.push({
        title: getDocumentTitle(result),
        attributes: document,
        resultAsString: resultAsString
      });
    }
  });

  return {
    showAsJson: false,
    results: details,
    complete: complete,
    queryExecutionId: queryExecutionId,
    executionStats: executionStats
  };
}

function getSummaryTags(results, options) {
  const tags = [];

  // Check if cached summary attributes are available
  if (!cachedSummaryAttributes || cachedSummaryAttributes.length === 0) {
    // No summary attributes configured, just return result count
    tags.push(`${results.length} ${results.length === 1 ? 'result' : 'results'}`);
    return tags;
  }

  // Use cached summary attributes instead of processing them again
  const summaryAttributes = cachedSummaryAttributes;

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

  // Remove duplicate tags as final cleanup step
  return [...new Set(tags)];
}

async function doLookup(entities, options, cb) {
  Logger.trace({ entities, options }, 'doLookup');
  let lookupResults;

  if (originalOptions === null) {
    originalOptions = { ...options };
  }

  if (optionsHaveChanged(options) || athenaClient === null) {
    athenaClient = initializeAthenaClient(options);

    // Reset prepared statement variables when options change
    // The existing prepared statement will remain in Athena and can be reused
    currentPreparedStatement = null;
    currentQuery = null;
  }

  // Process and cache attributes once at the beginning of doLookup
  setCachedDisplayAttributes(options);

  // Ensure prepared statement exists once per doLookup call, not per entity
  let preparedStatementName = null;
  if (options.query.includes('?')) {
    preparedStatementName = await ensurePreparedStatement(options.query, options);
  }

  const searchTasks = entities.map((entity) => {
    return async () => {
      const queryParams = await createQuery(entity, options, preparedStatementName);
      Logger.trace({ queryParams }, 'Athena SQL query parameters');

      const queryResult = await executeAthenaQuery(athenaClient, queryParams, options);
      Logger.trace({ queryResult }, 'Athena Query Result Object');

      if (!queryResult.complete) {
        // Query is still running, return queryExecutionId for later polling
        Logger.trace({ queryExecutionId: queryResult.queryExecutionId }, 'Query still running, returning execution ID');

        // Use the same formatting logic as onMessage
        const formattedResult = formatQueryResult(queryResult, options);

        return {
          entity,
          data: formattedResult
        };
      }

      if (!Array.isArray(queryResult.results) || queryResult.results.length === 0) {
        return {
          entity,
          data: null
        };
      } else {
        Logger.trace({ results: queryResult.results }, 'JSON Results from Athena');

        // Use the same formatting logic as onMessage
        const formattedResult = formatQueryResult(queryResult, options);

        return {
          entity,
          data: formattedResult
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

async function onMessage(message, options, cb) {
  Logger.trace({ message }, 'Received message');

  if (message.action === 'CHECK_QUERY_STATUS') {
    try {
      const queryExecutionId = message.queryExecutionId;

      if (!queryExecutionId) {
        return cb({
          error: 'Missing queryExecutionId in message'
        });
      }

      Logger.trace({ queryExecutionId }, 'Checking query status');

      // Initialize Athena client if needed - check if we need a new client first
      if (optionsHaveChanged(options) || athenaClient === null) {
        athenaClient = initializeAthenaClient(options);
      }

      // Process and cache attributes for result formatting
      setCachedDisplayAttributes(options);

      // Check query status and get results - reuse helper function
      const queryResult = await checkQueryStatusAndGetResults(queryExecutionId, options);

      // Format results using the same logic as doLookup - reuse helper function
      const responseData = formatQueryResult(queryResult, options);

      Logger.trace({ responseData }, 'Returning query status check results');
      cb(null, responseData);
    } catch (error) {
      Logger.error({ error }, 'Error checking query status');
      cb(errorToPojo(error, 'Error checking Athena query status'));
    }
  } else {
    cb({ error: `Unknown action: ${message.action}` });
  }
}

function validateOptions(userOptions, cb) {
  let errors = [];

  // Validate required accessKeyId field
  if (
    typeof userOptions.accessKeyId.value !== 'string' ||
    (typeof userOptions.accessKeyId.value === 'string' && userOptions.accessKeyId.value.trim().length === 0)
  ) {
    errors.push({
      key: 'accessKeyId',
      message: 'You must provide a valid AWS Access Key ID'
    });
  }

  // Validate required secretAccessKey field
  if (
    typeof userOptions.secretAccessKey.value !== 'string' ||
    (typeof userOptions.secretAccessKey.value === 'string' && userOptions.secretAccessKey.value.trim().length === 0)
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

function initializeAthenaClient(options) {
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
  return athenaClient;
}

async function checkQueryStatusAndGetResults(queryExecutionId, options) {
  // Check query status
  const statusCommand = new GetQueryExecutionCommand({ QueryExecutionId: queryExecutionId });
  const statusResult = await athenaClient.send(statusCommand);
  const queryStatus = statusResult.QueryExecution.Status.State;

  Logger.trace({ queryStatus, queryExecutionId }, 'Retrieved query status');

  if (queryStatus === 'SUCCEEDED') {
    // Query completed successfully, get results
    const executionStats = statusResult.QueryExecution.Statistics || {};
    const queryExecution = statusResult.QueryExecution || {};

    // Calculate runtime information
    const submissionTime = queryExecution.Status?.SubmissionDateTime;
    const completionTime = queryExecution.Status?.CompletionDateTime;
    let runtimeMs = null;
    let runtimeSeconds = null;

    if (submissionTime && completionTime) {
      runtimeMs = new Date(completionTime) - new Date(submissionTime);
      runtimeSeconds = (runtimeMs / 1000).toFixed(3);
    }

    const athenaRuntimeMs = executionStats.EngineExecutionTimeInMillis || null;
    const athenaRuntimeSeconds = athenaRuntimeMs ? (athenaRuntimeMs / 1000).toFixed(3) : null;

    const queryStats = {
      runtimeMs: athenaRuntimeMs || runtimeMs,
      runtimeSeconds: athenaRuntimeSeconds || runtimeSeconds,
      dataScannedBytes: executionStats.DataScannedInBytes || null,
      dataProcessedBytes: executionStats.DataProcessedInBytes || null,
      queryQueueTimeMs: executionStats.QueryQueueTimeInMillis || null,
      queryPlanningTimeMs: executionStats.QueryPlanningTimeInMillis || null,
      serviceProcessingTimeMs: executionStats.ServiceProcessingTimeInMillis || null
    };

    // Get query results
    const resultsCommand = new GetQueryResultsCommand({
      QueryExecutionId: queryExecutionId,
      MaxResults: options.limit || 100
    });
    const resultsResponse = await athenaClient.send(resultsCommand);

    // Convert Athena results to JSON format
    const rows = resultsResponse.ResultSet.Rows;
    let results = [];

    if (rows && rows.length > 0) {
      // First row contains column headers
      const headers = rows[0].Data.map((col) => col.VarCharValue);

      // Convert remaining rows to objects
      results = rows.slice(1).map((row) => {
        const obj = {};
        row.Data.forEach((col, index) => {
          obj[headers[index]] = col.VarCharValue;
        });
        return obj;
      });
    }

    return {
      results: results,
      complete: true,
      queryExecutionId: queryExecutionId,
      executionStats: queryStats
    };
  } else if (queryStatus === 'RUNNING' || queryStatus === 'QUEUED') {
    // Query still running
    const partialStats = statusResult.QueryExecution?.Statistics || {};
    const queryExecution = statusResult.QueryExecution || {};
    const submissionTime = queryExecution.Status?.SubmissionDateTime;
    
    // Use helper function to calculate elapsed time
    const { elapsedMs, elapsedSeconds } = calculateElapsedTime(submissionTime);

    const incompleteStats = {
      runtimeMs: null,
      runtimeSeconds: null,
      elapsedMs: elapsedMs,
      elapsedSeconds: elapsedSeconds,
      dataScannedBytes: partialStats.DataScannedInBytes || null,
      status: queryStatus
    };

    return {
      results: [],
      complete: false,
      queryExecutionId: queryExecutionId,
      executionStats: incompleteStats
    };
  } else if (queryStatus === 'FAILED' || queryStatus === 'CANCELLED') {
    // Query failed or was cancelled
    const errorReason = statusResult.QueryExecution.Status.StateChangeReason || 'Unknown error';
    throw new Error(`Query ${queryStatus.toLowerCase()}: ${errorReason}`);
  } else {
    // Unknown status
    throw new Error(`Unknown query status: ${queryStatus}`);
  }
}

function formatQueryResult(queryResult, options) {
  // Format results using the same logic as doLookup
  let summary;
  let details;

  if (!queryResult.complete) {
    // Query still running - same logic as doLookup
    summary = ['Query Running'];

    const statusAttributes = [
      { key: 'Status', value: queryResult.executionStats?.status || 'Running' },
      { key: 'Query Execution ID', value: queryResult.queryExecutionId }
    ];

    if (queryResult.executionStats) {
      if (queryResult.executionStats.elapsedSeconds) {
        statusAttributes.push({ key: 'Elapsed Time', value: `${queryResult.executionStats.elapsedSeconds}s` });
      }
      if (queryResult.executionStats.dataScannedBytes) {
        const scannedMB = (queryResult.executionStats.dataScannedBytes / 1024 / 1024).toFixed(2);
        statusAttributes.push({ key: 'Data Scanned', value: `${scannedMB} MB` });
      }
    }

    details = {
      showAsJson: false,
      results: [
        {
          title: 'Query Status',
          attributes: statusAttributes
        }
      ],
      complete: false,
      queryExecutionId: queryResult.queryExecutionId,
      executionStats: queryResult.executionStats
    };
  } else {
    // Query completed - reuse existing formatting functions
    if (!Array.isArray(queryResult.results) || queryResult.results.length === 0) {
      // No results
      summary = ['No results'];
      details = {
        showAsJson: false,
        results: [],
        complete: true,
        queryExecutionId: queryResult.queryExecutionId,
        executionStats: queryResult.executionStats
      };
    } else {
      // Has results - reuse existing functions
      summary = getSummaryTags(queryResult.results, options);
      details = getDetails(
        queryResult.results,
        options,
        queryResult.complete,
        queryResult.queryExecutionId,
        queryResult.executionStats
      );
    }
  }

  return {
    summary: summary,
    details: details
  };
}

function setCachedDisplayAttributes(options) {
  // Check and update document title attributes cache
  if (lastDocumentTitleAttributeOption !== options.documentTitleAttribute) {
    if (options.documentTitleAttribute && typeof options.documentTitleAttribute === 'string' && options.documentTitleAttribute.trim().length > 0) {
      cachedDocumentTitleAttributes = processAttributeOption(options.documentTitleAttribute);
    } else {
      cachedDocumentTitleAttributes = null;
    }
    lastDocumentTitleAttributeOption = options.documentTitleAttribute;
  }

  // Check and update detail attributes cache
  if (lastDetailAttributesOption !== options.detailAttributes) {
    if (options.detailAttributes && typeof options.detailAttributes === 'string' && options.detailAttributes.trim().length > 0) {
      cachedDetailAttributes = processAttributeOption(options.detailAttributes);
    } else {
      cachedDetailAttributes = null;
    }
    lastDetailAttributesOption = options.detailAttributes;
  }

  // Check and update summary attributes cache
  if (lastSummaryAttributesOption !== options.summaryAttributes) {
    if (options.summaryAttributes && typeof options.summaryAttributes === 'string' && options.summaryAttributes.trim().length > 0) {
      cachedSummaryAttributes = processAttributeOption(options.summaryAttributes);
    } else {
      cachedSummaryAttributes = null;
    }
    lastSummaryAttributesOption = options.summaryAttributes;
  }

  return {
    documentTitleAttributes: cachedDocumentTitleAttributes,
    detailAttributes: cachedDetailAttributes,
    summaryAttributes: cachedSummaryAttributes
  };
}

function calculateElapsedTime(submissionTime) {
  if (!submissionTime) {
    return { elapsedMs: null, elapsedSeconds: null };
  }

  const submissionDate = new Date(submissionTime);
  // Validate the date and ensure non-negative elapsed time
  if (!isNaN(submissionDate.getTime())) {
    const elapsedMs = Math.max(0, Date.now() - submissionDate.getTime());
    const elapsedSeconds = (elapsedMs / 1000).toFixed(3);
    return { elapsedMs, elapsedSeconds };
  } else {
    Logger.warn({ submissionTime }, 'Invalid submission time format received from Athena');
    return { elapsedMs: null, elapsedSeconds: null };
  }
}

module.exports = {
  doLookup,
  startup,
  validateOptions,
  onMessage
};

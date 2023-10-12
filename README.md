# Hasura DynamoDB Native Data Connector
This connector allows for connecting to DynamoDB tables in your AWS account, allowing for an instant GraphQL API on top of your DynamoDB data.
It uses the [TypeScript Data Connector SDK](https://github.com/hasura/ndc-sdk-typescript/) and implements the [Native Data Connector specification](https://github.com/hasura/ndc-spec/).

> [!WARNING]
> This connector is still is development and as such is very unfinished.

# Development Requirements
* NodeJS 18

## Build & Run
```
> npm install

# Start the configuration server
> npm start -- configuration serve

# Start the connector with a particular configuration file
> npm start -- serve --configuration your-configuration.json
```

## Docker Build & Run
```
> docker build . -t ndc-dynamodb:latest

# Start the configuration server
> docker run -it --rm -p 8100 -p 9100 ndc-dynamodb:latest configuration serve

# Start the connector with a particular configuration file mounted via a volume
> docker run -it --rm --name "ndc-dynamodb" -p 8100 -p 9100 \
    -v ./configuration.json:/tmp/configuration.json ndc-dynamodb:latest \
    serve --configuration /tmp/configuration.json
```

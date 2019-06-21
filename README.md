## sdmx-express
A NodeJS Express app that will publish SDMX as a Hosted Feature Service in ArcGIS Online.

## Requirements
### Setting up the Redis Cache
This provider utilizes a Redis cache. At the moment, no other types of caches are supported. Using Redis deployed on Microsoft Azure has been a very easy setup and is highly recommended. However, you need only supply the following environmnet variables in order for the provider to function.
- REDIS_HOST
    - ex: `azure-redis-resource.redis.cache.windows.net`
- REDIS_PORT
    - ex: `6380`
- REDIS_AUTH
    - ex: `superSecretAuthenticationGoesHere`

If you are using Microsoft Azure, you can set these variables in the `Application Settings` section of your App Service web app

## Getting Started
- `git clone` this repo
- `cd` into the directory and run `npm install`
- start the server with `npm start`
- your server is now available at `http://localhost:3000` and the publish endpoint that applications can call is `http://localhost:3000/publishSDMX`
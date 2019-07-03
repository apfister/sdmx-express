const express = require('express');
const sdmxService = require('../sdmxService');
const router = express.Router();
const fs = require('fs');

const Queue = require('bull');

const REDIS_CONFIG = {
  port: process.env.REDIS_PORT,
  host: process.env.REDIS_HOST,
  password: process.env.REDIS_AUTH,
  tls: { servername: process.env.REDIS_HOST }
};

const fsQueue = new Queue('fsQueue', {
  redis: REDIS_CONFIG
});

fsQueue.clean(1000);

fsQueue.client.on('error', err => {
  console.log('error connecting', err);
});

fsQueue.process(async (job, done) => {
  console.log(`processing job :: ${job.id}`);

  const params = job.data.params;
  const files = job.data.files;

  const token = params && params.token ? params.token : null;
  const userContentUrl = params && params.userContentUrl ? params.userContentUrl : null;
  const title = params && params.title ? params.title : `fromSDMX_${new Date().getTime()}`;
  const returnJson = params && params.sdmxApiFormat === 'json' ? true : false;

  // only return fields
  if (params && params.returnFieldsOnly === 'true') {
    const response = sdmxService.getFeatureServiceFields(params);
    return done(null, { success: true, fields: response.fields });
  }

  job.progress(10);
  let sdmxResponse = null;
  try {
    if (files && files.sdmxFile) {
      sdmxResponse = sdmxService.loadAndParseFile(files.sdmxFile.tempFilePath);
    } else if (params.sdmxApi) {
      sdmxResponse = await sdmxService.querySDMXEndpoint(params.sdmxApi, returnJson);
    } else {
      return done(new Error('unable to process request. no body or query params in request.'));
    }
  } catch (err) {
    return done(new Error(`Unable to get SDMX data. ${err.message})`));
  }

  job.progress(20);
  let sdmxAsGeoJson = null;
  try {
    sdmxAsGeoJson = sdmxService.sdmxToGeoJson(sdmxResponse, title, returnJson);
  } catch (err) {
    return done(new Error(`Unable to convert SDMX data to GeoJSON. ${err.message})`));
  }

  job.progress(30);
  // load geographies
  if (params.joinToGeographies === 'true') {
    job.progress(35);
    if (!params.sdmxField || !params.geoField) {
      return done(new Error('both sdmxField and geoField must be specified when joinToGeographies is set to true.'));
    }

    let geographiesResponse = null;
    if (files && eq.files.geoFile) {
      geographiesResponse = sdmxService.loadAndParseFile(files.sdmxFile.tempFilePath);
    } else {
      job.progress(35);
      geographiesResponse = await sdmxService.queryFeatureServiceForGeographies(
        params.geographiesFeatureServiceUrl,
        null
      );
    }

    job.progress(40);
    sdmxAsGeoJson = sdmxService.joinSDMXToGeoJson(
      sdmxAsGeoJson,
      geographiesResponse,
      params.sdmxField,
      params.geoField
    );
  }

  job.progress(50);
  let addResponse = null;
  try {
    addResponse = await sdmxService.addGeoJsonItem(sdmxAsGeoJson, title, token, userContentUrl);
  } catch (error) {
    return done(new Error(addResponse.error));
  }

  if (addResponse.error) {
    return done(new Error(addResponse.error.message));
  }

  job.progress(60);
  // publish geojson as feature service
  let publishResponse = null;
  try {
    publishResponse = await sdmxService.publishGeoJsonItem(addResponse.id, title, token, userContentUrl);
  } catch (error) {
    return done(new Error(error));
  }

  job.progress(100);
  if (!publishResponse.data.services[0]) {
    return done(new Error(`Unable to Publish Layer. No serviceItemId in response`));
  } else if (publishResponse.data.services[0].success && publishResponse.data.services[0].success === false) {
    return done(new Error(`Unable to Publish Layer '${title}'. A service with that name may already exist.`));
  }

  done(null, { itemId: publishResponse.data.services[0].serviceItemId });

  // clean up
  for (var file in files) {
    try {
      fs.unlinkSync(files[file].tempFilePath);
      console.log(`deleted ${files[file].name} at ${files[file].tempFilePath}`);
    } catch (error) {
      console.log(error);
    }
  }
});

fsQueue.on('progress', (job, progress) => {
  console.log(`processing :: ${progress}`);
});

fsQueue.on('completed', function(job, result) {
  console.log('fsQueue completed');
});

fsQueue.on('error', function(job, result) {
  console.log('fsQueue error');
});

fsQueue.on('failed', function(job, result) {
  console.log('fsQueue failed');
});

router.get('/status/:jobId', async (req, res, next) => {
  const jobId = req.params.jobId;
  let job = null;
  try {
    job = await fsQueue.getJob(jobId);
    const progress = await job.progress();
    const isCompleted = await job.isCompleted();
    const isFailed = await job.isFailed();
    res.json({ data: job.returnvalue, progress, isCompleted, isFailed, failedReason: job.failedReason });
  } catch (error) {
    return res.json({ error: error });
  }
});

router.post('/', async (req, res, next) => {
  let added = null;
  try {
    added = await fsQueue.add({ params: req.query, files: req.files });
    return res.json({
      jobId: added.id,
      checkJobStatusUrl: `${req.protocol}://${req.get('host')}/publishSDMX/status/${added.id}`
    });
  } catch (error) {
    console.log(error);
    return res.json({ error: err.message });
  }
});

module.exports = router;

const express = require('express');
const sdmxService = require('../sdmxService');
const router = express.Router();
const fs = require('fs');

const kue = require('kue');
const queue = kue.createQueue({
  redis: {
    port: process.env.REDIS_PORT,
    host: process.env.REDIS_HOST,
    auth: process.env.REDIS_AUTH,
    options: {
      tls: { servername: process.env.REDIS_HOST }
    }
  }
});

router.get('/', async (req, res, next) => {
  queue.process('featureservice', (job, done) => {
    console.log('job data', job.data);
    job.progress(50, 100);
    done();
  });

  const job = queue.create('featureservice', { hello: 'world' }).save(err => {
    if (!err) {
      return res.json({
        jobId: job.id,
        checkJobStatusUrl: `${req.protocol}://${req.get('host')}/kuemon/job/${job.id}`
      });
    } else {
      return res.json({ error: err });
    }
  });

  job.on('progress', (progress, data) => {
    console.log(progress, data);
  });
});

router.post('/', async (req, res, next) => {
  queue.process('featureservice', async (job, done) => {
    // console.log('job data', job.data);
    const params = job.data.params;
    const files = job.data.files;

    const token = params && params.token ? params.token : null;
    const userContentUrl = params && params.userContentUrl ? params.userContentUrl : null;
    const title = params && params.title ? params.title : `fromSDMX_${new Date().getTime()}`;

    // only return fields
    if (params && params.returnFieldsOnly === 'true') {
      const response = sdmxService.getFeatureServiceFields(params);
      return done(null, { success: true, fields: response.fields });
    }

    job.progress(20, 100, { message: 'Parsing SDMX file' });
    let sdmxResponse = null;
    try {
      if (files && files.sdmxFile) {
        sdmxResponse = sdmxService.loadAndParseFile(files.sdmxFile.tempFilePath);
      } else if (params.sdmxApi) {
        sdmxResponse = await sdmxService.querySDMXEndpoint(params.sdmxApi);
      } else {
        return done(new Error('unable to process request. no body or query params in request.'));
      }
    } catch (err) {
      return done(new Error(`Unable to get SDMX data. ${err.message})`));
    }

    job.progress(20, 100, { message: 'Converting SDMX to GeoJson' });
    let sdmxAsGeoJson = null;
    try {
      sdmxAsGeoJson = sdmxService.sdmxToGeoJson(sdmxResponse);
    } catch (err) {
      return done(new Error(`Unable to convert SDMX data to GeoJSON. ${err.message})`));
    }

    job.progress(30, 100);
    // load geographies
    if (params.joinToGeographies === 'true') {
      job.progress(35, 100, { message: 'Loading GeoJson file' });
      if (!params.sdmxField || !params.geoField) {
        return done(new Error('both sdmxField and geoField must be specified when joinToGeographies is set to true.'));
      }

      let geographiesResponse = null;
      if (files && eq.files.geoFile) {
        geographiesResponse = sdmxService.loadAndParseFile(files.sdmxFile.tempFilePath);
      } else {
        job.progress(35, 100, { message: 'Querying Feature Service for GeoJson' });
        geographiesResponse = await sdmxService.queryFeatureServiceForGeographies(
          params.geographiesFeatureServiceUrl,
          null
        );
      }

      job.progress(40, 100, { message: 'Joining SDMX to GeoJson' });
      sdmxAsGeoJson = sdmxService.joinSDMXToGeoJson(
        sdmxAsGeoJson,
        geographiesResponse,
        params.sdmxField,
        params.geoField
      );
    }

    job.progress(50, 100, { message: 'Adding GeoJson to ArcGIS Online' });
    const addResponse = await sdmxService.addGeoJsonItem(sdmxAsGeoJson, title, token, userContentUrl);

    if (addResponse.error) {
      return done(new Error(addResponse.error));
    }

    job.progress(60, 100, { message: 'Publishing GeoJson as ArcGIS Online Hosted Feature Service' });
    // publish geojson as feature service
    let publishResponse = null;
    try {
      publishResponse = await sdmxService.publishGeoJsonItem(addResponse.id, title, token, userContentUrl);
    } catch (error) {
      return done(new Error(error));
    }

    job.progress(99, 100, { message: 'Done!' });
    if (publishResponse.services[0].success && publishResponse.services[0].success === false) {
      return res.json({
        success: false,
        error: `Unable to Publish Layer '${title}'. A service with that name may already exist.`
      });
    }

    // clean up
    for (var file in files) {
      try {
        fs.unlinkSync(files[file].tempFilePath);
        console.log(`deleted ${files[file].name} at ${files[file].tempFilePath}`);
      } catch (error) {
        console.log(error);
      }
    }

    done(null, { success: true, publishResponse: publishResponse });
  });

  const job = queue.create('featureservice', { params: req.query, files: req.files }).save(err => {
    if (!err) {
      return res.json({
        jobId: job.id,
        checkJobStatusUrl: `${req.protocol}://${req.get('host')}/kuemon/job/${job.id}`
      });
    } else {
      return res.json({ error: err.message });
    }
  });

  // job.on('progress', (progress, data) => {
  //   console.log(progress, data);
  // });

  // job.on('complete', result => {
  //   res.json({ success: true, data: result });
  // });
});

module.exports = router;

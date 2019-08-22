const express = require('express');
const sdmxService = require('../sdmxService');
const router = express.Router();

router.post('/', async (req, res, next) => {
  const params = req.query;
  const files = req.files;

  const token = params && params.token ? params.token : null;
  const userContentUrl = params && params.userContentUrl ? params.userContentUrl : null;
  const title = params && params.title ? params.title : `fromSDMX_${new Date().getTime()}`;
  const returnJson = params && params.sdmxApiFormat === 'json' && params.isSDMXUploadCsv !== 'true' ? true : false;
  const isSDMXUploadCsv = params && params.isSDMXUploadCsv === 'true' ? true : false;

  // only return fields
  if (params && params.returnFieldsOnly === 'true') {
    const response = sdmxService.getFeatureServiceFields(params);
    return res.json({ success: true, fields: response.fields });
  }

  let sdmxResponse = null;
  try {
    if (files && files.sdmxFile) {
      sdmxResponse = await sdmxService.loadAndParseFile(files.sdmxFile.tempFilePath, isSDMXUploadCsv);
    } else if (params.sdmxApi) {
      sdmxResponse = await sdmxService.querySDMXEndpoint(params.sdmxApi, returnJson);
    } else {
      return res.json({
        success: false,
        message: new Error('unable to process request. no body or query params in request.')
      });
    }
  } catch (err) {
    return res.json({
      success: false,
      message: new Error(`Unable to get SDMX data. ${err.message})`)
    });
  }

  let sdmxAsGeoJson = null;
  try {
    sdmxAsGeoJson = sdmxService.sdmxToGeoJson(sdmxResponse, title, returnJson, isSDMXUploadCsv);
  } catch (err) {
    return res.json({
      success: false,
      message: new Error(`Unable to convert SDMX data to GeoJSON. ${err.message}`)
    });
  }

  // load geographies
  if (params.joinToGeographies === 'true') {
    if (!params.sdmxField || !params.geoField) {
      return res.json({
        success: false,
        message: new Error('both sdmxField and geoField must be specified when joinToGeographies is set to true.')
      });
    }

    let geographiesResponse = null;
    if (files && req.files.geoFile) {
      geographiesResponse = sdmxService.loadAndParseFile(files.sdmxFile.tempFilePath);
    } else {
      geographiesResponse = await sdmxService.queryFeatureServiceForGeographies(
        params.geographiesFeatureServiceUrl,
        null,
        [params.geoField]
      );
    }

    sdmxAsGeoJson = sdmxService.joinSDMXToGeoJson(
      sdmxAsGeoJson,
      geographiesResponse,
      params.sdmxField,
      params.geoField
    );
  }

  let addResponse = null;
  try {
    addResponse = await sdmxService.addGeoJsonItem(sdmxAsGeoJson, title, token, userContentUrl);
  } catch (error) {
    return res.json({
      success: false,
      message: new Error(addResponse.error)
    });
  }

  if (addResponse.error) {
    return res.json({
      success: false,
      message: new Error(addResponse.error.message)
    });
  }

  // publish geojson as feature service
  let publishResponse = null;
  try {
    publishResponse = await sdmxService.publishGeoJsonItem(addResponse.id, title, token, userContentUrl);
  } catch (error) {
    return res.json({
      success: false,
      message: new Error(error)
    });
  }

  if (!publishResponse.data.services[0]) {
    return res.json({
      success: false,
      message: new Error(`Unable to Publish Layer. No serviceItemId in response`)
    });
  } else if (publishResponse.data.services[0].success && publishResponse.data.services[0].success === false) {
    return res.json({
      success: false,
      message: new Error(`Unable to Publish Layer '${title}'. A service with that name may already exist.`)
    });
  }

  res.json({ itemId: publishResponse.data.services[0].serviceItemId });
});

module.exports = router;

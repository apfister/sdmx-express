const fs = require('fs');
const moment = require('moment');
const rp = require('request-promise');
const axios = require('axios');
const xmlParser = require('fast-xml-parser');
const Papa = require('papaparse');

const SDMX_ACCEPT_HEADER = 'application/vnd.sdmx.data+json;version=1.0.0-wd';

require('isomorphic-fetch');
require('isomorphic-form-data');
const { queryFeatures } = require('@esri/arcgis-rest-feature-layer');

const buffer = require('buffer');
buffer.constants.MAX_STRING_LENGTH = Infinity;

// const debug = require('debug')('sdmx-express:server');

/**
 * Create feature collection from Xml string
 * @param json JSON object of SDMX response
 * @param title The name of the output layer
 */
function createFeatureCollection(json, title) {
  let fc = {
    type: 'FeatureCollection',
    features: []
  };

  fc.metadata = {
    name: 'from sdmx',
    idField: 'counterField',
    fields: [
      {
        name: 'counterField',
        alias: 'counterField',
        type: 'Integer'
      }
    ]
  };

  const dimensionProps = json.data.structure.dimensions.observation;
  const attributeProps = json.data.structure.attributes.observation;
  let fields = parseFieldsAndLookups(dimensionProps, attributeProps);
  fields.push({
    name: 'OBS_VALUE',
    alias: 'Observation Value',
    type: 'Double'
  });

  fc.metadata.fields = fc.metadata.fields.concat(fields);

  const observations = json.data.dataSets[0].observations;

  const features = createFeatures(observations, dimensionProps, attributeProps);
  fc.features = features;

  // const layerName = json.data.structure.name.en || json.data.structure.name;

  fc.metadata.name = title;

  return fc;
}

/**
 * Create features in geojson format from SDMX formatted response
 * @param observations The actual data values for the observations
 * @param dimensionProps Dimension properties
 * @param attributeProps Attribute properties
 */
function createFeatures(observations, dimensionProps, attributeProps) {
  let features = [];
  let idCounter = 1;
  for (const obs in observations) {
    // let feature = { type: 'Feature', properties: {}, geometry: { type: 'Point', coordinates: [] } };
    let feature = { type: 'Feature', properties: {}, geometry: {} };

    const dimSplits = obs.split(':');
    const attributes = observations[obs];

    for (var i = 0; i < dimSplits.length; i++) {
      const currentKeyInt = parseInt(dimSplits[i]);
      const foundDim = dimensionProps.filter(dim => dim.keyPosition === i)[0];
      if (foundDim) {
        if (foundDim.id === 'TIME_PERIOD') {
          const tv = moment(foundDim.values[currentKeyInt].name.en, 'YYYY-MM');
          // feature.properties[`${foundDim.id}_CODE`] = foundDim.values[currentKeyInt].id;
          feature.properties[`${foundDim.id}_CODE`] = tv.format('YYYY-MM').toString();
          feature.properties[foundDim.name.en.toUpperCase().replace(' ', '_')] = tv.format('YYYY-MM').toString();
        } else {
          feature.properties[`${foundDim.id}_CODE`] = foundDim.values[currentKeyInt].id;
          feature.properties[foundDim.name.en.toUpperCase().replace(' ', '_')] = foundDim.values[currentKeyInt].name.en;
        }
      }
    }

    const obsValue = attributes[0];
    feature.properties['OBS_VALUE'] = obsValue;

    attributes.shift();

    for (var j = 0; j < attributes.length; j++) {
      const attValue = attributes[j];
      const foundAtt = attributeProps[j];
      if (attValue === null) {
        feature.properties[`${foundAtt.id}_CODE`] = null;
        feature.properties[foundAtt.name.en.toUpperCase().replace(' ', '_')] = null;
      } else {
        feature.properties[`${foundAtt.id}_CODE`] = foundAtt.values[attValue].id;
        feature.properties[foundAtt.name.en.toUpperCase().replace(' ', '_')] = foundAtt.values[attValue].name.en;
      }
    }

    feature.properties['counterField'] = idCounter++;

    features.push(feature);
  }

  return features;
}

/**
 * Helper to parse fields from SDMX formatted response
 * @param dimensionProps Dimension properties
 * @param attributeProps Attribute properties
 */
function parseFieldsAndLookups(dimensionProps, attributeProps) {
  let fields = [];

  dimensionProps.forEach(obs => {
    fields.push({
      name: `${obs.id}_CODE`,
      alias: `${obs.id}_CODE`,
      type: 'String'
    });

    if (!obs.name.en) {
      obs.name = { en: obs.name };
    }
    fields.push({
      name: obs.name.en.toUpperCase().replace(' ', '_'),
      alias: obs.name.en,
      type: 'String'
    });
  });

  attributeProps.forEach(obs => {
    fields.push({
      name: `${obs.id}_CODE`,
      alias: `${obs.id}_CODE`,
      type: 'String'
    });

    if (!obs.name.en) {
      obs.name = { en: obs.name };
    }

    fields.push({
      name: obs.name.en.toUpperCase().replace(' ', '_'),
      alias: obs.name.en,
      type: 'String'
    });
  });

  return fields;
}

/**
 * Create feature collection from Xml string
 * @param csvFile The XML formatted string
 * @param title The name of the output layer
 */
function createFeatureCollectionFromCsv(csvFile, title) {
  let fc = {
    type: 'FeatureCollection',
    features: []
  };

  fc.metadata = {
    name: 'from sdmx',
    idField: 'counterField',
    fields: [
      {
        name: 'counterField',
        alias: 'counterField',
        type: 'Integer'
      }
    ]
  };

  let fields = Object.keys(csvFile[0]).map(field => ({ name: field, alias: field, type: 'String' }));

  fc.metadata.fields = [...fc.metadata.fields, ...fields];

  const features = createFeaturesFromCsv(csvFile);
  fc.features = features;

  fc.metadata.name = title;

  return fc;
}

function createFeaturesFromCsv(csvFile) {
  let features = csvFile.map((feat, i) => {
    let feature = {
      type: 'Feature',
      properties: feat,
      geometry: { type: 'Point', coordinates: [] }
    };

    feature.properties['counterField'] = i++;

    return feature;
  });
  return features;
}

/**
 * Create feature collection from Xml string
 * @param xmlString The XML formatted string
 * @param title The name of the output layer
 */
function createFeatureCollectionFromXml(xmlString, title) {
  let fc = {
    type: 'FeatureCollection',
    features: []
  };

  fc.metadata = {
    name: 'from sdmx',
    idField: 'counterField',
    fields: [
      {
        name: 'counterField',
        alias: 'counterField',
        type: 'Integer'
      }
    ]
  };

  const parsedXml = xmlParser.parse(xmlString, {
    ignoreAttributes: false,
    ignoreNameSpace: true
  });

  let fields = parseFieldsAndLookupsFromXml(parsedXml);
  fields.push({
    name: 'OBS_VALUE',
    alias: 'Observation Value',
    type: 'Double'
  });

  fc.metadata.fields = [...fc.metadata.fields, ...fields];

  const observations = parsedXml.GenericData.DataSet.Obs;

  const features = createFeaturesFromXml(observations);
  fc.features = features;

  fc.metadata.name = title;

  return fc;
}

/**
 * Get Fields from SDMX XML response
 * @param parsedXml The XML as JSON object
 */
function parseFieldsAndLookupsFromXml(parsedXml) {
  const keyFields = parsedXml.GenericData.DataSet.Obs[0].ObsKey.Value.map(rec => rec['@_id']);
  const attFields = parsedXml.GenericData.DataSet.Obs[0].Attributes.Value.map(rec => rec['@_id']);
  return [...keyFields, ...attFields];
}

/**
 * Create GeoJson features from XML
 * @param observations The SDMX observations
 */
function createFeaturesFromXml(observations) {
  let features = observations.map((obs, i) => {
    let feature = { type: 'Feature', properties: {}, geometry: { type: 'Point', coordinates: [] } };
    const featureKeys = obs.ObsKey.Value;
    featureKeys.forEach(fk => {
      feature.properties[fk['@_id']] = fk['@_value'];
    });

    const attributes = obs.Attributes.Value;
    attributes.forEach(att => {
      feature.properties[att['@_id']] = att['@_value'];
    });

    feature.properties['OBS_VALUE'] = obs.ObsValue['@_value'];

    feature.properties['counterField'] = i++;

    return feature;
  });
  return features;
}

module.exports = {
  getFeatureServiceFields: () => {
    return {};
  },

  loadAndParseFile: async (tempFilePath, isSDMXUploadCsv) => {
    return new Promise((resolve, reject) => {
      if (isSDMXUploadCsv) {
        const file = fs.createReadStream(tempFilePath);
        const outCsv = Papa.parse(file, {
          header: true,
          complete: (results, file) => {
            resolve(results.data);
          },
          error: (error, file) => {
            reject(error);
          }
        });
      } else {
        const outJson = JSON.parse(fs.readFileSync(tempFilePath));
        resolve(outJson);
      }
    });
  },

  querySDMXEndpoint: async (sdmxApi, returnJson) => {
    let options = {
      url: sdmxApi
    };

    if (returnJson) {
      options.json = true;
      options.headers = { accept: SDMX_ACCEPT_HEADER };
    }

    let response = await rp(options);
    if (returnJson && !response.data) {
      response = { data: response };
    }
    return response;
  },

  sdmxToGeoJson: (dataSet, title, isJson, isSDMXUploadCsv) => {
    let fc = null;
    if (isJson) {
      fc = createFeatureCollection(dataSet, title);
    } else if (isSDMXUploadCsv) {
      fc = createFeatureCollectionFromCsv(dataSet, title);
    } else {
      fc = createFeatureCollectionFromXml(dataSet, title);
    }
    return fc;
  },

  getUniqueGeoValues: (sdmxAsGeoJson, sdmxField) => {
    return [...new Set(sdmxAsGeoJson.features.map(feature => feature.properties[sdmxField]))];
  },

  queryFeatureServiceForGeographies: async (url, whereClause, outFields) => {
    return queryFeatures({
      url: `${url}/query`,
      where: whereClause || '1=1',
      outFields: outFields || '*',
      outSR: 4326,
      params: { f: 'geojson' }
    });
  },

  joinSDMXToGeoJson: (sdmxAsGeoJson, geographiesResponse, sdmxField, geoField) => {
    let tempCache = {};
    let foundGeom = null;

    sdmxAsGeoJson.features.forEach(feature => {
      if (tempCache[feature.properties[sdmxField]]) {
        feature.geometry = tempCache[feature.properties[sdmxField]];
      } else {
        foundGeom = null;
        foundGeom = geographiesResponse.features.filter(gjFeature => {
          const gjValue = gjFeature.properties[geoField];
          const sdmxValue = feature.properties[sdmxField];
          return gjValue === sdmxValue;
        })[0];
        if (foundGeom && foundGeom.geometry) {
          tempCache[feature.properties[sdmxField]] = foundGeom.geometry;
          feature.geometry = foundGeom.geometry;
        }
      }
    });

    tempCache = {};

    return sdmxAsGeoJson;
  },

  addGeoJsonItem: async (geojson, title, token, userContentUrl) => {
    // can't figure out how to use axios lib to POST with the geojson as a file
    // .. sticking with request-promise (rp)
    const options = {
      url: `${userContentUrl}/addItem`,
      method: 'POST',
      json: true,
      headers: {
        'Cache-control': 'no cache'
      },
      formData: {
        title: title,
        type: 'GeoJson',
        f: 'json',
        token: token,
        file: {
          value: Buffer.from(JSON.stringify(geojson)),
          options: {
            filename: `${title}.geojson`,
            contentType: 'application/json'
          }
        }
      }
    };
    const addResponse = await rp(options);

    const updateOptions = {
      url: `${userContentUrl}/items/${addResponse.id}/update`,
      method: 'post',
      repsonseType: 'json',
      params: {
        f: 'json',
        token: token,
        tags: 'SDMX',
        typeKeywords: `SDMX`
      }
    };
    const updateResponse = await axios(updateOptions);

    return { id: addResponse.id };
  },

  publishGeoJsonItem: async (itemId, title, token, userContentUrl, sdgMetadata) => {
    const options = {
      url: `${userContentUrl}/publish`,
      method: 'post',
      responseType: 'json',
      params: {
        itemId: itemId,
        overwrite: false,
        filetype: 'geojson',
        f: 'json',
        tags: 'SDMX',
        typeKeywords: 'SDMX',
        token: token,
        publishParameters: {
          hasStaticData: true,
          name: title,
          maxRecordCount: 10000,
          layerInfo: {
            capabilities: 'Query'
          }
        }
      }
    };

    const response = await axios(options);

    const updateOptions = {
      url: `${userContentUrl}/items/${response.data.services[0].serviceItemId}/update`,
      method: 'post',
      repsonseType: 'json',
      params: {
        f: 'json',
        token: token,
        tags: 'SDMX',
        typeKeywords: `SDMX`
      }
    };

    if (sdgMetadata) {
      if (sdgMetadata.seriesTags && sdgMetadata.seriesTags.length > 0) {
        updateOptions.params.tags = [...sdgMetadata.seriesTags, 'SDMX'].join(',');
      }
      let description = [];

      // Build Goal Info
      if (sdgMetadata.goalCode && sdgMetadata.goal) {
        description.push(`Goal ${sdgMetadata.goalCode} - ${sdgMetadata.goal}`);
      } else if (sdgMetadata.goalCode) {
        description.push(`Goal ${sdgMetadata.goalCode}`);
      } else if (sdgMetadata.goal) {
        description.push(`${sdgMetadata.goalCode}`);
      }

      // Build Target Info
      if (sdgMetadata.targetCode && sdgMetadata.target) {
        description.push(`Target ${sdgMetadata.targetCode} - ${sdgMetadata.target}`);
      } else if (sdgMetadata.targetCode) {
        description.push(`Target ${sdgMetadata.targetCode}`);
      } else if (sdgMetadata.target) {
        description.push(`${sdgMetadata.targetCode}`);
      }

      // Build Indicator Info
      if (sdgMetadata.indicatorCode && sdgMetadata.indicator) {
        description.push(`Indicator ${sdgMetadata.indicatorCode} - ${sdgMetadata.indicator}`);
      } else if (sdgMetadata.indicatorCode) {
        description.push(`Indicator ${sdgMetadata.indicatorCode}`);
      } else if (sdgMetadata.indicator) {
        description.push(`${sdgMetadata.indicatorCode}`);
      }

      // Build Series Info
      if (sdgMetadata.seriesCode && sdgMetadata.series) {
        description.push(`Series ${sdgMetadata.seriesCode} - ${sdgMetadata.series}`);
      } else if (sdgMetadata.seriesCode) {
        description.push(`Series ${sdgMetadata.seriesCode}`);
      } else if (sdgMetadata.series) {
        description.push(`${sdgMetadata.seriesCode}`);
      }

      if (description.length > 0) {
        updateOptions.params.description = description.join('<br>');
      }
    }

    const updateResponse = await axios(updateOptions);

    return response;
  }
};

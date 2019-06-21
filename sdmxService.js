const fs = require('fs');
const moment = require('moment');
const rp = require('request-promise');
const SDMX_ACCEPT_HEADER = 'application/vnd.sdmx.data+json;version=1.0.0-wd';

require('isomorphic-fetch');
require('isomorphic-form-data');
const { queryFeatures } = require('@esri/arcgis-rest-feature-layer');
const { request } = require('@esri/arcgis-rest-request');

const buffer = require('buffer');
buffer.constants.MAX_STRING_LENGTH = Infinity;

function createFeatureCollection(json) {
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

  const layerName = json.data.structure.name.en || json.data.structure.name;

  fc.metadata.name = layerName;

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
    let feature = { type: 'Feature', properties: {}, geometry: { type: 'Point', coordinates: [] } };

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

module.exports = {
  getFeatureServiceFields: () => {
    return {};
  },

  loadAndParseFile: tempFilePath => {
    const outJson = JSON.parse(fs.readFileSync(tempFilePath));
    return outJson;
  },

  querySDMXEndpoint: async sdmxApi => {
    const options = {
      url: sdmxApi,
      json: true,
      headers: { accept: SDMX_ACCEPT_HEADER }
    };
    let response = await rp(options);
    if (!response.data) {
      response = { data: response };
    }
    return response;
  },

  sdmxToGeoJson: dataSet => {
    const fc = createFeatureCollection(dataSet);
    return fc;
  },

  queryFeatureServiceForGeographies: async (url, whereClause) => {
    const where = whereClause || '1=1';

    return queryFeatures({
      url: `${url}/query`,
      where: where,
      outFields: '*',
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
    const options = {
      url: `${userContentUrl}/addItem`,
      method: 'POST',
      json: true,
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
    const response = await rp(options);

    // const addStatus = await testItOut(response.id, token, userContentUrl);

    return response;
  },

  publishGeoJsonItem: async (itemId, title, token, userContentUrl) => {
    const options = {
      httpMethod: 'POST',
      params: {
        itemId: itemId,
        f: 'json',
        token: token,
        filetype: 'geojson',
        overwrite: false,
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

    const response = await request(`${userContentUrl}/publish`, options);
    return response;
  }
};

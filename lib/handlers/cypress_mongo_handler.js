const IndividualResultSchema = require('cqm-models').IndividualResultSchema;
const _ = require('lodash');
const mongoose = require('mongoose');
const QDMPatientSchema = require('cqm-models').PatientSchema;
const PatientSource = require('../models/patient_source');
const MeasureSource = require('../models/measure_source');

module.exports = class Handler {
  /* Initializes cumulative results structure for storage at finish time, and initializes connection to MongoDB
    */
  constructor() {
    this.finished = true;

    /* Individual patient results, hashed by measure id and patient id */
    this.individualResultsByMeasureId = {};

    this.correlationId = null;
    this.effectiveDate = null;
    this.patientinfo = {};
  }

  start(options) {
    // this.aggregateResultsByMeasureId = {};
    if (options) {
      this.correlationId = options.correlation_id;
      this.effectiveDate = options.effective_date;
    }
    this.finished = false;
    return true;
  }

  async queryCollection(connection){
    this.QDMPatient = connection.model('QDM_Patient', QDMPatientSchema);
    
    this.ptntids = [];
    this.ptntinfo = {};
    
    await Promise.all(Object.keys(this.individualResultsByMeasureId).map((measureId) => {
        return Promise.all(Object.keys(this.individualResultsByMeasureId[measureId]).map((patientId) => {
          this.ptntids.push(patientId);
        }));
      // TODO: Save aggregate results
      }));

    const p = await this.findPatients(this.ptntids);
    p.forEach((ptn) => {
      this.ptntinfo['given']= ptn.givenNames;
      this.ptntinfo['family'] = ptn.familyName;
      this.ptntinfo['birthday']=ptn.birthDatetime;
      this.ptntinfo['gender'] = this.findGender(ptn);
      this.ptntinfo['medical_record_number'] = ptn.extendedData.medical_record_number;
      this.patientinfo[ptn._id] = this.ptntinfo;
      this.ptntinfo = {};
    });
  }

  findGender(patient, callback = null){
    const self = this;
    this.gender = null;

    patient.dataElements.forEach((de) => {
      if(de._type === 'QDM::PatientCharacteristicSex'){
        this.gender = de.dataElementCodes[0].code;
      }
    });
    if (callback != null) {
        callback(self);
      }
    return this.gender;
  }
async getId(connection, idkey){
    this.measureSource = new MeasureSource(connection);
    this.measureId = Object.keys(this.individualResultsByMeasureId)[0]
    this.measure = await this.measureSource.findMeasure(this.measureId);
    return this.measure.get(idkey);
}

  async findPatients(patientIds, callback = null) {
    const self = this;
    this.index = 0;
    this.patients = [];

    const patientIdsList = Array.isArray(patientIds) ? patientIds : [patientIds];

    return this.QDMPatient.find({
      // Need to transform the input array using mongoose.Types.ObjectId()
      _id: { $in: _.map(patientIdsList, mongoose.Types.ObjectId) },
    }, (err, patients) => {
      if (err) return Error(err);

      if (patients === null) return TypeError('patients not found');

      self.patients = _.map(patients, p => self.QDMPatient(p.toObject()));

      if (callback != null) {
        callback(self);
      }
      return patients;
    });
  }
  /* Takes in the most recent measure calculation results for a single patient and records/aggregates them
    */
  handleResult(measure, resultsByPatientId) {
    this.individualResultsByMeasureId[measure._id] = resultsByPatientId;
  }

  /* Wraps up individual and aggregate results and saves to the database */
  async finish(connection) {
    const IndividualResult = connection.model('qdm_individual_result', IndividualResultSchema);
    const hqmfid = await this.getId(connection, 'hqmf_id');
    const sid = await this.getId(connection,'sub_id');
    if (this.finished) {
      throw new Error('Handler cannot be finished until it is started.');
    } else {
      await Promise.all(Object.keys(this.individualResultsByMeasureId).map((measureId) => {
        return Promise.all(Object.keys(this.individualResultsByMeasureId[measureId]).map((patientId) => {
          // IndividualResult data gets reinstantiated in an object with a MongoDB connection
          
          //this.sub_id = this.measure.get('sub_id');
          const patientResultMongo =
            IndividualResult(this.individualResultsByMeasureId[measureId][patientId]
              .toObject());
          this.individualResultsByMeasureId[measureId][patientId] = patientResultMongo;
          if (patientResultMongo.state === 'running') {
            patientResultMongo.state = 'complete';
          }
          // Add necessary Cypress data to the extended_data tab
          if (!patientResultMongo.extendedData) {
            patientResultMongo.extendedData = {};
          }

          if (this.correlationId) {
            patientResultMongo.extendedData.correlation_id = this.correlationId;
          }
          if (this.effectiveDate) {
            patientResultMongo.extendedData.effective_date = this.effectiveDate;
          }
          if (this.patientinfo[patientId]){
            patientResultMongo.extendedData.first = this.patientinfo[patientId].given;
            patientResultMongo.extendedData.last = this.patientinfo[patientId].family;
            patientResultMongo.extendedData.DOB = this.patientinfo[patientId].birthday;
            patientResultMongo.extendedData.gender = this.patientinfo[patientId].gender;
            patientResultMongo.extendedData.medical_record_number = this.patientinfo[patientId].medical_record_number;
          }
          if(hqmfid){
            patientResultMongo.extendedData.hqmf_id = hqmfid;
          }
          if(sid){
            patientResultMongo.extendedData.sub_id = sid;
          }

          return patientResultMongo.save((err) => {
            if (err) throw Error(err);
          });
        }));
      // TODO: Save aggregate results
      }));
    }
    this.finished = true;
    // TODO: Return something needed specifically by Cypress
    return {
      Individual: this.individualResultsByMeasureId,
    };
  }
};

const { ValueSetSchema } = require('cqm-models');
const mongoose = require('mongoose');

module.exports = class MongoDBValueSetSource {
  /* Insert documentation here
    */

  constructor(connectionInfo) {
    mongoose.connect(connectionInfo);

    this.ValueSet = mongoose.model('ValueSet', ValueSetSchema);

    this.valueSetsByOid = {};
  }

  /*
    TODO: Full documentation
    This callback's signature is callback(self)
  */
  async findValueSetsByOidForMeasures(measures, callback = null) {
    const self = this;
    this.index = 0;
    this.valueSetsByOid = {};

    const measureList = Array.isArray(measures) ? measures : [measures];

    let valueSetOidList = [];
    measureList.forEach((mes) => {
      valueSetOidList = valueSetOidList.concat(mes.get('value_sets'));
    });

    return this.ValueSet.find({
      // Need to transform the input array using mongoose.Types.ObjectId()
      _id: { $in: valueSetOidList },
    }, (err, valueSets) => {
      if (err) return Error(err);

      const valueSetsList = Array.isArray(valueSets) ? valueSets : [valueSets];

      valueSetsList.forEach((valueSet) => {
        self.valueSetsByOid[valueSet.get('oid')] = valueSet;
      });

      if (callback != null) {
        callback(self);
      }
      return valueSetsList;
    });
  }

  reset() {
    this.index = 0;
  }

  getLength() {
    return this.patients.length();
  }

  currentPatient() {
    return this.patients[this.index];
  }

  nextPatient() {
    if (this.index >= this.patients.length) {
      return null;
    }
    const nextPatient = this.currentPatient();
    this.index += 1;
    return nextPatient;
  }
};
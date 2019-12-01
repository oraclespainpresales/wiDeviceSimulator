'use strict'

const fs = require('fs')
    , log = require('npmlog-ts')
    , async = require('async')
    , commandLineArgs = require('command-line-args')
    , getUsage = require('command-line-usage')
    , mqtt = require('mqtt')
    , _ = require('lodash')
;

// Main constants
const PROCESSNAME = "WEDO Industry 4.0 Demo - Device Simulator"
    , VERSION = "v1.0"
    , AUTHOR  = "Carlos Casares <carlos.casares@oracle.com>"
    , PROCESS = 'PROCESS'
    , CONFIG  = 'CONFIG'
    , MQTT    = "MQTT"
;

// Initialize input arguments
const optionDefinitions = [
  { name: 'mqttbroker', alias: 'm', type: String },
  { name: 'logfile', alias: 'f', type: String },
  { name: 'help', alias: 'h', type: Boolean },
  { name: 'verbose', alias: 'v', type: Boolean, defaultOption: false }
];

const sections = [
  {
    header: PROCESSNAME + " " + VERSION,
    content: 'Device simulator for WEDO Industry events'
  },
  {
    header: 'Options',
    optionList: [
      {
        name: 'mqttbroker',
        typeLabel: '{underline ipaddress:port}',
        alias: 'm',
        type: String,
        description: 'MQTT broker IP address/hostname and port'
      },
      {
        name: 'logfile',
        typeLabel: '{underline file}',
        alias: 'f',
        type: String,
        description: 'Log file used for simulation'
      },
      {
        name: 'verbose',
        alias: 'v',
        description: 'Enable verbose logging.'
      },
      {
        name: 'help',
        alias: 'h',
        description: 'Print this usage guide.'
      }
    ]
  }
]

const options = commandLineArgs(optionDefinitions);

const valid =
  options.help ||
  (
    options.mqttbroker &&
    options.logfile 
  );

if (!valid) {
  console.log(getUsage(sections));
  process.exit(-1);
}

const logDataFile = options.logfile
    , MQTTBROKER  = options.mqttbroker
    , WAITFORMQTT = 1000
    , dummyDate   = "01/01/2000 "
    , REGEX = { groups: 4, regex: /(\d{4}-\d{2}-\d{2}) (\d{2}:\d{2}:\d{2}.\d{3}) verb MQTT Message received with topic '(.*)' and data: (.*)/}
;

log.level = (options.verbose) ? 'verbose' : 'info';
log.timestamp = true;

if (!fs.existsSync(logDataFile)) {
  log.error(PROCESS, "File %s does not exist or is not readable", logDataFile);
  process.exit(-1);
}

var mqttClient = _.noop();

async.series( {
  splash: (callbackMainSeries) => {
    log.info(PROCESS, "%s - %s", PROCESSNAME, VERSION);
    log.info(PROCESS, "Author - %s", AUTHOR);
    callbackMainSeries(null, true);
  },
  mqtt: (callbackMainSeries) => {
    // Connect to MQTT broker
    log.info(MQTT, "Connecting to MQTT broker at %s", MQTTBROKER);
    mqttClient = mqtt.connect(MQTTBROKER, { username: "iot", password: "welcome1", reconnectPeriod: 1000, connectTimeout: 30000 });
    mqttClient.connected = false;
    // Common event handlers
    mqttClient.on('connect', () => {
      log.info(MQTT, "Successfully connected to MQTT broker at %s", MQTTBROKER);
      mqttClient.connected = true;
    });

    mqttClient.on('error', err => {
      log.error(MQTT, "Error: ", err);
    });

    mqttClient.on('reconnect', () => {
      log.verbose(MQTT, "Client trying to reconnect...");
    });

    mqttClient.on('offline', () => {
      mqttClient.connected = false;
      log.warn(MQTT, "Client went offline!");
    });

    mqttClient.on('end', () => {
      mqttClient.connected = false;
      log.info(MQTT, "Client ended");
    });
    callbackMainSeries(null, true);
  },
  file: (callbackMainSeries) => {
    // Read and process log file
    var contents = fs.readFileSync(logDataFile,'utf8');
    var lines = contents.split("\n");
    log.info(PROCESS, "Processing log file: %s (%d entries)", logDataFile, lines.length);
    var t1 = undefined;
    async.eachOfSeries(lines, (line, index, nextLineInLog) => {
      let l = _.split(line, REGEX.regex).filter(a=>a);
      if (l.length != REGEX.groups) {
        // No matching, go to next line
        nextLineInLog();
        return;
      }

      if (t1 === undefined) {
        // Very first line in log
        publishMQTT(l[2], l[3]).then(() => {
          t1 = l[1];
          nextLineInLog();
        });
      } else {
        let d1 = new Date(dummyDate + t1);
        let d2 = new Date(dummyDate + l[1]);
        let wait = d2 - d1;
        setTimeout(() => {
          publishMQTT(l[2], l[3]).then(() => {
            t1 = l[1];
            nextLineInLog();
          });
        }, wait);
      }
    }, function(err) {
      if (!options.verbose) {
        process.stdout.write("\n");
      }
      log.info("", "Processing completed");
      callbackMainSeries();
    });
  }
}, (err, results) => {
  if (err) {
    log.error(PROCESS, "Aborting.Severe error: " + err);
  }
  process.exit(0);
});

function publishMQTT(topic, payload) {
  return new Promise((resolve, reject) => {
    async.until(
    (cb) => {
      cb(null, mqttClient.connected);
    }, 
    (cb) => {
      setTimeout(() => {
        log.verbose(MQTT, `Waiting for MQTT connection established...`);
        cb();
      }, WAITFORMQTT);
    }, 
    () => {
      log.verbose(MQTT, `Publishing on topic "${topic}": ${payload}`);
      mqttClient.publish(topic, payload);
      resolve();
    });
  });
}

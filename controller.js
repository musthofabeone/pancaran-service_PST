const nodeFetch = require('node-fetch')
const fetch = require('fetch-cookie')(nodeFetch);
const connection = require('./conn');
const base64 = require('base-64');
const cron = require('node-cron');
const crypto = require('crypto');
var https = require('follow-redirects').https;
var fs = require('fs');
const privateKey = fs.readFileSync('./pancaran-pg-sap-dev.key');
var jwt = require('jsonwebtoken');
var moment = require('moment'); // untuk definisi tanggal
const config = require('./config');
const { Pool } = require('pg') // untuk postgresql
var retry = require('promise-fn-retry'); // untik retry & interval
const { base64encode, base64decode } = require('nodejs-base64');
const fastJson = require('fast-json-stringify')
const { AbortController } = require('node-abort-controller');
const fetchRetry = require('node-fetch-retry-timeout');
const { resolve } = require('path');
const { match } = require('assert');
const logger = require('./logger');
const { job } = require('cron');
const { response, query } = require('express');
const { Logger } = require('winston');
const { time } = require('console');
const CronJob = require('cron').CronJob;
const start = new Date();
let reffnumber = ""

// create pool
const pool = new Pool({
  name: 'service_payment',
  user: 'h2hadm',
  host: '10.10.16.58',
  database: 'saph2h',
  password: 'susujahe',
  port: 5090,
});
// SSL (Only if needed)
process.env['NODE_TLS_REJECT_UNAUTHORIZED'] = 0


// PostgreSql Query Adapter
async function executeQuery(query, queryparams) {
  return new Promise((resolve, reject) => {
    connection.query(query, queryparams,
      function (error, rows, fields) {
        if (error) {
          logger.info(moment().format('YYYY-MM-DD h:mm:ss a') + ": ");
          logger.error("PostgreSql Query failed");
          logger.error(error);
          reject(error);
        } else {
          // logger.info(this.sql); // Debug
          resolve(rows);
        }
      });
  })
}

async function getConfigDB() {
  // let _dbName =  await fs.readFile('configDB.txt', 'utf8', (err, data) => {
  //   if (err) {
  //     console.error(err);
  //     return;
  //   } else {
  //     _dbName = data
  //     logger.info("Ambil nama DB 1111111: " + data)
  //   }
  // })

  const data = await fs.readFileSync("configDB.txt", "utf8");
  return Buffer.from(data);

}

const getDBName = () => {
  return new Promise((resolve, reject) => {
    const data = fs.readFileSync("configDB.txt", "utf8");
    resolve(Buffer.from(data));
  });
};



const testing = () => {
  return new Promise((resolve, reject) => {
    fetch("https://10.10.16.21:4300/PaymentService/getEntryOut.xsjs?docnum=OUT/206/1102&dbName=PSTTESTING2022", {
      method: 'GET',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': "Basic " + config.auth_basic,
        'Cookie': 'sapxslb=0F2F5CCBA8D5854A9C26CB7CAD284FD1; xsSecureId1E75392804D765CC05AC285536FA9A62=14C4F843E2445E48A692A2D219C5C748'
      },
      'maxRedirects': 20
    })
      .then(res => res.json())
      .then(async data => {
        logger.debug(data);
        var result = []
        var detail = []
        var rows = {}
        Object.keys(data.ENTRY).forEach(function (key) {
          logger.debug(data.ENTRY[key].DocEntry)
          //data = 

          result.push({
            DocEntry: data.ENTRY[key].DocEntry,
            LineId: data.ENTRY[key].LineId,
            TrxId: "",
            Auth: 'Basic ' + config.auth_basic,
            DbName: "",
            Referency: "referency",
            OutType: data.ENTRY[key].OUTYPE,
            HasError: "error"
          })
        })

        resolve(result)
      })
      .catch(err => {
        logger.error("Get Error: " + err.message)
      });
  });
};

async function Outgoing(dbname) {

  logger.info("Nama DB: " + dbname)

  let result = await connection.query('SELECT DISTINCT X0."trxId", X0."customerReffNumber", CAST(X1."AMOUNT" AS DECIMAL(19,2)) "AMOUNT", CAST(X1."BANKCHARGE" AS DECIMAL(19,2)) "BANKCHARGE", X1."DB", X0."auth", X1."PAYMENTOUTTYPE"  FROM paymenth2h."BOS_DO_STATUS" X0 ' +
    'INNER JOIN paymenth2h."BOS_TRANSACTIONS" X1 ON X0."customerReffNumber" = X1."REFERENCY" ' +
    'INNER JOIN paymenth2h."BOS_LOG_TRANSACTIONS" X2 ON X0."trxId" = X2."TRXID" AND X0."customerReffNumber" = X2."REFERENCY" ' +
    'WHERE X0."flag" = \'0\' AND X1."DB" = $1 ORDER BY X0."customerReffNumber" ASC LIMIT 1', [dbname]);

  return result;
}






var resultRowCount = 0
async function main() {
  try {
    let _dbName = await getDBName();
    logger.info('Service Create Outgoing Payment ' + _dbName);


    let result = await connection.query('SELECT DISTINCT X1."VENDOR", X0."trxId", X0."customerReffNumber", CAST(X1."AMOUNT" AS DECIMAL(19,2)) "AMOUNT" ' +
      ', CAST(X1."BANKCHARGE" AS DECIMAL(19,2)) "BANKCHARGE", X1."DB", X0."auth", X0."successDate" ' +
      ', X2."PAYMENTOUTTYPE", X2."PAYMENTNO"  FROM paymenth2h."BOS_DO_STATUS" X0  ' +
      'INNER JOIN paymenth2h."BOS_TRANSACTIONS" X1 ON X0."customerReffNumber" = X1."REFERENCY"  ' +
      'INNER JOIN (SELECT MAX("ROWNUMBER") "NUMBER", "TRXID", "REFERENCY" ,"PAYMENTOUTTYPE", "PAYMENTNO" FROM ( ' +
      '			SELECT ROW_NUMBER () OVER (PARTITION BY "REFERENCY" ORDER BY "TRANSDATE", "TRANSTIME" DESC) "ROWNUMBER", "TRXID", "REFERENCY" ' +
      '		, "PAYMENTOUTTYPE", "PAYMENTNO", "TRANSTIME" FROM  paymenth2h."BOS_LOG_TRANSACTIONS"  ' +
      '			WHERE "TRXID" != \'\'  ORDER BY "TRANSDATE", "TRANSTIME" DESC) X0 ' +
      '			GROUP BY X0."TRXID", X0."REFERENCY", X0."PAYMENTOUTTYPE", X0."PAYMENTNO") X2 ON   ' +
      '		 X0."customerReffNumber" = X2."REFERENCY"  ' +
      '		WHERE X0."flag" = \'0\' AND X1."DB" = $1' +
      'ORDER BY X0."customerReffNumber"', [_dbName]);

    if (result.rowCount > 0) {
      resultRowCount = 0
      config.inproses = true
      jobs.stop();
    }

    const timer = ms => new Promise(res => setTimeout(res, ms))
    for await (var data of result.rows) {
      resultRowCount++;
      logger.info("Create Outgoing Payment (" + data.DB + "), Reff: " + data.customerReffNumber)
      let Login = await LoginSL(data.DB)
      if (Login !== "LoginError") {
        var Outgoing = await post_Incoming(data.trxId, data.customerReffNumber, data.AMOUNT, data.auth, data.DB, data.VENDOR, data.successDate)

        if (Outgoing === "Success") {
          var detail = await getDetailUdo(data.trxId, data.DB, data.auth, data.customerReffNumber, false, data.PAYMENTOUTTYPE);

          if (Object.keys(detail).length > 0) {
            await Object.keys(detail).forEach(async function (key) {
              let updateUdoDetails = await updateUdo(detail[key].Type, detail[key].LineId, detail[key].DocEntry, detail[key].DocNum
                , detail[key].ProcessType, detail[key].PaymentMeans, detail[key].BankPaymentMeans, detail[key].SourcePayment
                , detail[key].Trxid, detail[key].CustomerReffNumber, data.DB, false, data.PAYMENTOUTTYPE, data.PAYMENTNO);

              if (updateUdoDetails === "Update Success" && data.PAYMENTOUTTYPE === "OUTSTATUS") {
                var detailSecond = await getDetailUdo(data.trxId, data.DB, data.auth, data.customerReffNumber, false, "OUT");
                await Object.keys(detailSecond).forEach(async function (key) {
                  let updateUdoDetails = await updateUdo(detailSecond[key].Type, detailSecond[key].LineId, detailSecond[key].DocEntry, detailSecond[key].DocNum
                    , detailSecond[key].ProcessType, detailSecond[key].PaymentMeans, detailSecond[key].BankPaymentMeans, detailSecond[key].SourcePayment
                    , detailSecond[key].Trxid, detailSecond[key].CustomerReffNumber, data.DB, false, "OUTSTATUS", data.PAYMENTNO);
                })
              }
            })

            await connection.query('UPDATE paymenth2h."BOS_DO_STATUS" SET "flag" = \'1\' WHERE "trxId" = $1 AND "customerReffNumber" = $2;', [data.trxId, data.customerReffNumber], async function (error, result, fields) {
              if (error) {
                logger.error(error)
              }
            })
          }

        } else if (Outgoing == "error") {
          var detail = await getDetailUdo(data.trxId, data.DB, data.auth, data.customerReffNumber, true, data.PAYMENTOUTTYPE);
          if (Object.keys(detail).length > 0) {
            var updateUdoDetails = await updateUdo(detail.Type, detail.LineId, detail.DocEntry, detail.DocNum, detail.ProcessType
              , detail.PaymentMeans, detail.BankPaymentMeans, detail.SourcePayment, detail.Trxid, detail.CustomerReffNumber, data.DB, true, data.PAYMENTOUTTYPE, data.PAYMENTNO);
            logger.info(updateUdoDetails)
          }
        } else {
          await executeQuery('INSERT INTO paymenth2h."BOS_LOG_TRANSACTIONS"("PAYMENTOUTTYPE", "PAYMENTNO", "TRXID", "REFERENCY", "VENDOR", "ACCOUNT", "AMOUNT", "TRANSDATE", "TRANSTIME", "STATUS", "REASON", "BANKCHARGE", "FLAGUDO", "SOURCEACCOUNT", "TRANSFERTYPE", "CLIENTID", "ERRORCODE")' +
            'SELECT "PAYMENTOUTTYPE", "PAYMENTNO","TRXID", "REFERENCY", "VENDOR", "ACCOUNT", "AMOUNT", $3, $4, $1, $6, "BANKCHARGE", "FLAGUDO", "SOURCEACCOUNT", "TRANSFERTYPE", "CLIENTID", $5' +
            'FROM paymenth2h."BOS_TRANSACTIONS" WHERE "REFERENCY" = $2 limit 1', ['5', data.customerReffNumber, moment(new Date()).format("yyyyMMDD"), moment(new Date()).format("HH:mm:ss"), 'SAP -1', 'SAP - ' + Outgoing], async function (error, result, fields) {
              if (error) {
                logger.info(error)
              } else {
                var detail = await getDetailUdo(data.trxId, data.DB, data.auth, data.customerReffNumber, true, data.PAYMENTOUTTYPE);
                if (Object.keys(detail).length > 0) {
                  var updateUdoDetails = await updateUdo(detail.Type, detail.LineId, detail.DocEntry, detail.DocNum, detail.ProcessType
                    , detail.PaymentMeans, detail.BankPaymentMeans, detail.SourcePayment, detail.Trxid, detail.CustomerReffNumber, data.DB, true, data.PAYMENTOUTTYPE, data.PAYMENTNO);
                  logger.info(updateUdoDetails)
                }
              }
            });
        }
      }
      await timer(5000)
    }

    if (result.rowCount === resultRowCount) {
      logger.info("Interfacing Finished")
      resultRowCount = 0
      config.inproses = false
    }

    jobs.start();
  }
  catch (error) {
    logger.error("Interfacing Error: " + error.message)
    resultRowCount = 0
    config.inproses = false
    jobs.start();
  }

}

const jobs = new CronJob({
  cronTime: '*/5 * * * * *',
  onTick: async () => {
    try {
      main()
    } catch (err) {
      logger.error("Interfacing Error: " + err.message)
      resultRowCount = 0
      config.inproses = false
      jobs.start();
    }

  },
  start: true,
  timeZone: 'UTC'
})

jobs.start()


const LoginSL = async (dbName) => {
  return new Promise((resolve, reject) => {
    var myHeaders = new nodeFetch.Headers();
    //var retries = 3;
    var backoff = 300;
    myHeaders.append("Authorization", "Basic " + config.auth_basic);
    myHeaders.append("Content-Type", "application/json");
    logger.info("(OUT) Middleware - Service Layer (" + dbName + "), Login : " + config.base_url_SL + "/b1s/v2/Login")
    var raw = JSON.stringify({
      "CompanyDB": dbName,
      "UserName": config.userName,
      "Password": config.Password
    });

    var requestOptions = {
      method: 'POST',
      headers: myHeaders,
      body: raw,
      redirect: 'follow'
    };

    const fetchWithTimeout = (input, init, timeout) => {
      const controller = new AbortController();
      setTimeout(() => {
        controller.abort();
      }, timeout)

      return fetch(input, { signal: controller.signal, ...init });
    }

    const wait = (timeout) =>
      new Promise((resolve) => {
        setTimeout(() => resolve(), timeout);
      })

    const fetchWithRetry = async (input, init, timeout, retries) => {
      let increseTimeOut = config.interval_retries;
      let count = retries;

      while (count > 0) {
        try {
          return await fetchWithTimeout(input, init, timeout);
        } catch (e) {
          if (e.name !== "AbortError") throw e;
          count--;
          logger.error(
            `fetch Login, retrying login in ${increseTimeOut}s, ${count} retries left`
          );
          await wait(increseTimeOut)
        }
      }
    }

    var Session = ""
    var url = config.base_url_SL + "/b1s/v2/Login";
    const retryCodes = [408, 500, 502, 503, 504, 522, 524]
    return fetchWithRetry(url, requestOptions, config.timeOut, config.max_retries)
      .then(res => {
        //logger.debug(res.text())

        if (res.status == 200) return res.text()

      })
      .then(result => {
        if (result !== null) {
          const resultBody = JSON.parse(result)
          if (!resultBody.error) {
            Session = resultBody.SessionId
            logger.info("Login Service Layer: (" + dbName + ") Success")
            resolve(Session);
          } else {
            logger.info("Error Login (" + dbName + ")", result);
            resolve("LoginError")
          }
        }

      })
      .catch(error => {
        logger.info('Error Login : ' + error)
        resolve("LoginError")
      });
  });
};


// async function LoginV2(dbName) {
//   //jobs.stop();
//   /* 1 */
//   var myHeaders = new nodeFetch.Headers();
//   var retries = 3;
//   var backoff = 300;
//   myHeaders.append("Authorization", "Basic " + config.auth_basic);
//   myHeaders.append("Content-Type", "application/json");
//   //myHeaders.append("Cookie", "B1SESSION=01d87ac4-3379-11ed-8000-000c297b22e1");
//   await logger.info("(OUT) Middleware - Service Layer (" + dbName + "), Login : " + config.base_url_SL + "/b1s/v2/Login")
//   var raw = JSON.stringify({
//     "CompanyDB": dbName,
//     "UserName": config.userName,
//     "Password": config.Password
//   });

//   var requestOptions = {
//     method: 'POST',
//     headers: myHeaders,
//     body: raw,
//     redirect: 'follow'
//   };

//   const fetchWithTimeout = (input, init, timeout) => {
//     const controller = new AbortController();
//     setTimeout(() => {
//       controller.abort();
//     }, timeout)

//     return fetch(input, { signal: controller.signal, ...init });
//   }

//   const wait = (timeout) =>
//     new Promise((resolve) => {
//       setTimeout(() => resolve(), timeout);
//     })

//   const fetchWithRetry = async (input, init, timeout, retries) => {
//     let increseTimeOut = config.interval_retries;
//     let count = retries;

//     while (count > 0) {
//       try {
//         return await fetchWithTimeout(input, init, timeout);
//       } catch (e) {
//         logger.info(e.name)
//         if (e.name !== "AbortError") throw e;
//         count--;
//         logger.error(
//           `fetch Login, retrying login in ${increseTimeOut}s, ${count} retries left`
//         );
//         await wait(increseTimeOut)
//       }
//     }
//   }

//   var Session = ""
//   var url = config.base_url_SL + "/b1s/v2/Login";
//   const retryCodes = [408, 500, 502, 503, 504, 522, 524]
//   return await fetchWithRetry(url, requestOptions, config.timeOut, config.max_retries)
//     .then(async res => {
//       //logger.info("Login Session ("+ dbName +"): " , res.status);
//       if (res.status == 200) return res.text()

//       if (config.max_retries > config.min_retries && (res.status != 200 || res.status == "")) {
//         setTimeout(() => {
//           logger.info("Retry Login Session (" + dbName + "): Count: " + config.min_retries++ + " From " + config.max_retries);
//           return LoginV2(dbName) /* 3 */
//         }, config.interval_retries) /* 2 */
//       } else {
//         throw new Error(res.text())
//       }
//     })
//     .then(result => {
//       if (result != "") {
//         const resultBody = JSON.parse(result)
//         if (!resultBody.error) {
//           Session = resultBody.SessionId
//           logger.info("Login Service Layer: (" + dbName + ") Success")
//           return Session;
//         } else {
//           logger.info("Error Login (" + dbName + ")", result);
//         }
//       }
//     })
//     .catch(error => {
//       logger.info('Error Login', error)
//     });
// }

const getHeader = async (trxid, customerReffNumber, valueAmount, auth, dbName, invoice, cfl, _bankCharge, vendor, _successDate) => {
  return new Promise((resolve, reject) => {
    var start = new Date();
    var date = moment(start).format('YYYYMMDD')
    fetch(config.base_url_xsjs + "/PaymentService/get_Header.xsjs?trxid=" + customerReffNumber + "&dbName=" + dbName, {
      method: 'GET',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': auth,
        'Cookie': 'sapxslb=0F2F5CCBA8D5854A9C26CB7CAD284FD1; xsSecureId1E75392804D765CC05AC285536FA9A62=14C4F843E2445E48A692A2D219C5C748'
      },
      'maxRedirects': 20
    })
      .then(res => res.json())
      .then(data => {
        logger.info("(OUT) Middleware - XSJS (" + dbName + "): Header Outgoing " + customerReffNumber + " , Body: " + config.base_url_xsjs + "/PaymentService/get_Header.xsjs?trxid=" + trxid + "&dbName=" + dbName)

        var _Series = "";
        var _CardCode = "";
        var _Remarks = "";
        var _TransferAccount = "";

        let bodyPayment = {};
        var _amount = 0;
        Object.keys(data.HEADER).forEach(function (key) {
          var d = data.HEADER[key];
          _Series = d.Series;

          _CardCode = d.CardCode;
          _Remarks = d.Remarks;
          _TransferAccount = d.Account;
          _amount = parseFloat(d.Amount) + parseFloat(_bankCharge);
          logger.info(_CardCode)


          logger.info("Body Outgoing (" + dbName + ") : " + JSON.stringify({
            Series: _Series,
            DocObjectCode: 'bopot_OutgoingPayments',
            DocType: 'rSupplier',
            CardCode: _CardCode,
            DocDate: _successDate,
            DueDate: _successDate,
            VatDate: _successDate,
            Remarks: _Remarks,
            TransferAccount: _TransferAccount, // no. account transfer
            TransferSum: _amount,
            BankChargeAmount: _bankCharge, // bank charge
            U_PYMNO: customerReffNumber, // no ref dari pay out/out status
            PaymentInvoices: invoice, // Detail AP Invoice/APDP
            CashFlowAssignments: cfl // CashFlow
          }))


          bodyPayment = JSON.stringify({
            Series: _Series,
            DocObjectCode: 'bopot_OutgoingPayments',
            DocType: 'rSupplier',
            CardCode: _CardCode,
            DocDate: _successDate,
            DueDate: _successDate,
            VatDate: _successDate,
            Remarks: _Remarks,
            TransferAccount: _TransferAccount,
            TransferSum: _amount,
            BankChargeAmount: _bankCharge,
            U_PYMNO: customerReffNumber,
            PaymentInvoices: invoice,
            CashFlowAssignments: cfl
          })
        })

        if (bodyPayment === null) {
          resolve("HeaderEmpty")
        } else {
          resolve(bodyPayment)
        }
      })
      .catch(err => {
        logger.error(err)
      });

  })
}

const createOutgoing = async (trxid, customerReffNumber, valueAmount, auth, dbName, bodyPayment, vendor) => {
  return new Promise((resolve, reject) => {
    fetch(config.base_url_SL + '/b1s/v2/VendorPayments', {
      method: 'POST',
      body: bodyPayment,
      headers: {
        'Accept': 'application/json',
        'Content-Type': 'application/json'
      },
      crossDomain: true,
      credentials: "include",
      withCredentials: true,
      xhrFields: {
        'withCredentials': true
      },
    }).then(res3 => {
      return res3.json()
    }).then(async data => {
      logger.info("(OUT) Middleware - SAP (" + dbName + "): Create Outgoing " + customerReffNumber + " : " + config.base_url_SL + '/b1s/v2/VendorPayments')
      //logger.info(data)

      var dateOutgoing = new Date()
      if (!data.error) {

        logger.info("(OUT) Middleware - SAP (" + dbName + "): Create Outgoing " + customerReffNumber + " Success")

        await connection.query('INSERT INTO paymenth2h."BOS_LOG_TRANSACTIONS"("PAYMENTOUTTYPE", "PAYMENTNO", "TRXID", "REFERENCY", "VENDOR", "ACCOUNT", "AMOUNT", "TRANSDATE", "TRANSTIME", "STATUS", "REASON", "BANKCHARGE", "FLAGUDO", "SOURCEACCOUNT", "TRANSFERTYPE", "CLIENTID","ERRORCODE")' +
          'SELECT "PAYMENTOUTTYPE", "PAYMENTNO", $1, "REFERENCY", "VENDOR", "ACCOUNT", "AMOUNT", $3, $4, 0, $6, "BANKCHARGE", "FLAGUDO", "SOURCEACCOUNT", "TRANSFERTYPE", "CLIENTID", $5' +
          'FROM paymenth2h."BOS_TRANSACTIONS" WHERE "REFERENCY" = $2 limit 1', [trxid, customerReffNumber, moment(new Date()).format("yyyyMMDD"), moment(new Date()).format("HH:mm:ss"), '', 'SAP - Success Generate Outgoing Payment']);

        await connection.query('UPDATE paymenth2h."BOS_DO_STATUS" SET "flag" = \'1\' WHERE "trxId" = $1 AND "customerReffNumber" = $2;', [trxid, customerReffNumber])
        resolve("Success")

      } else {
        logger.error("(OUT) Middleware - SAP (" + dbName + "): Create Outgoing Error " + customerReffNumber + " : " + data.error.message.value)

        // cek error code agar tidak dobel saat log
        var errorCode = data.error.code
        var errorMsg = data.error.message.value
        if (parseFloat(data.error.code) === -1) {
          await connection.query('UPDATE paymenth2h."BOS_DO_STATUS" SET "flag" = \'1\' WHERE "customerReffNumber" = $1;', [customerReffNumber])
        }

        let result = await executeQuery('SELECT COUNT(*) "errorCount" FROM paymenth2h."BOS_LOG_TRANSACTIONS"  WHERE "REFERENCY" = $1 AND "ERRORCODE" = $2', [customerReffNumber, 'SAP ' + errorCode])
        if (result.rowCount > 0) {
          for (var data of result.rows) {
            if (data.errorCount < 1) {
              await executeQuery('INSERT INTO paymenth2h."BOS_LOG_TRANSACTIONS"("PAYMENTOUTTYPE", "PAYMENTNO", "TRXID", "REFERENCY", "VENDOR", "ACCOUNT", "AMOUNT", "TRANSDATE", "TRANSTIME", "STATUS", "REASON", "BANKCHARGE", "FLAGUDO", "SOURCEACCOUNT", "TRANSFERTYPE", "CLIENTID", "ERRORCODE")' +
                'SELECT "PAYMENTOUTTYPE", "PAYMENTNO","TRXID", "REFERENCY", "VENDOR", "ACCOUNT", "AMOUNT", $3, $4, $1, $6, "BANKCHARGE", "FLAGUDO", "SOURCEACCOUNT", "TRANSFERTYPE", "CLIENTID", $5' +
                'FROM paymenth2h."BOS_TRANSACTIONS" WHERE "REFERENCY" = $2 limit 1', ['5', customerReffNumber, moment(new Date()).format("yyyyMMDD"), moment(new Date()).format("HH:mm:ss"), 'SAP ' + errorCode, 'SAP - ' + errorMsg], async function (error, result, fields) {
                  if (error) {
                    logger.info(error)
                  } else {
                    resolve("error")
                  }
                });
            }
          }
        }

        resolve("error :" + errorMsg)
      }
    }).catch(err => {

      logger.error(err.message)
      resolve("error :" + err.message)
    });

  })
}

const getInvoice = async (trxid, customerReffNumber, valueAmount, auth, dbName, vendor) => {
  return new Promise((resolve, reject) => {
    fetch(config.base_url_xsjs + "/PaymentService/get_invDetail.xsjs?trxid=" + customerReffNumber + "&vendor=" + vendor + "&dbName=" + dbName, {
      method: 'GET',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': auth,
        'Cookie': 'sapxslb=0F2F5CCBA8D5854A9C26CB7CAD284FD1; xsSecureId1E75392804D765CC05AC285536FA9A62=14C4F843E2445E48A692A2D219C5C748'
      },
      'maxRedirects': 20
    })
      .then(res => res.json())
      .then(dataInvoices => {
        var PaymentInvoices = []
        var rowdata = {}
        // print log
        logger.info("(OUT) Middleware - XSJS (" + dbName + "): Details invoice " + customerReffNumber + " :" + config.base_url_xsjs + "/PaymentService/get_invDetail.xsjs?trxid=" + customerReffNumber + "&vendor=" + vendor + "&dbName=" + dbName)

        var line = 0
        logger.info(dataInvoices)
        Object.keys(dataInvoices.DETAIL).forEach(function (key) {
          var d = dataInvoices.DETAIL[key];

          rowdata = {
            "LineNum": line,
            "DocEntry": d.DocEntry,
            "SumApplied": d.U_REQTOTAL,
            "InvoiceType": d.U_DOCTYPE,
            "InstallmentId": d.U_INSTALLMENT
          };
          PaymentInvoices.push(rowdata);
          line++;

        })

        if (rowdata === null) {
          resolve("InvoiceEmpty")
        } else {
          resolve(PaymentInvoices)
        }

      })
      .catch(err => {

        logger.error("Get Error: " + err.message)
      });


  })
}

const getCashFlow = async (trxid, customerReffNumber, valueAmount, auth, dbName, _bankCharge) => {
  return new Promise((resolve, reject) => {
    // casfhflow line item
    fetch(config.base_url_xsjs + "/PaymentService/get_CFL.xsjs?trxid=" + customerReffNumber + "&dbName=" + dbName, {
      method: 'GET',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': auth,
        'Cookie': 'sapxslb=0F2F5CCBA8D5854A9C26CB7CAD284FD1; xsSecureId1E75392804D765CC05AC285536FA9A62=14C4F843E2445E48A692A2D219C5C748'
      },
      'maxRedirects': 20
    })
      .then(res => res.json())
      .then(_cfl => {
        // print log cashflow
        logger.info("(OUT) Middleware - XSJS (" + dbName + "): Cashflow Line Item " + customerReffNumber + " , Body: " + config.base_url_xsjs + "/PaymentService/get_CFL.xsjs?trxid=" + customerReffNumber + "&dbName=" + dbName)

        //logger.info(_cfl)

        var linesCashfLOW = []
        var rowdata = {}
        var amount_cf = 0;
        Object.keys(_cfl.CFL).forEach(function (key) {
         
          var d = _cfl.CFL[key];
          
          if (parseFloat(d.Amount) > 0) {
            if (d.Row == "1") {
              amount_cf = parseFloat(d.Amount) + parseFloat(_bankCharge)
            } else {
              amount_cf = parseFloat(d.Amount)
            }

          } else if (parseFloat(d.Amount) < 0) {
            amount_cf = amount_cf - (parseFloat(d.Amount) * -1)
          }

          rowdata = {
            'CashFlowLineItemID': d.CFWId,
            'PaymentMeans': 'pmtBankTransfer',
            'AmountLC': amount_cf
          };

          linesCashfLOW.push(rowdata);
        })
        logger.info(linesCashfLOW)

        if (linesCashfLOW === null) {
          resolve("CFLEmpty")
        } else {
          resolve(linesCashfLOW)
        }



      })
      .catch(err => {
        logger.error("Get Error: " + err.message)
        resolve(err.message)
      });


  })
}

const getCount = async (dbName, customerReffNumber, auth) => {
  return new Promise((resolve, reject) => {
    fetch(config.base_url_xsjs + "/PaymentService/check_reff.xsjs?dbName=" + dbName + "&reff=" + customerReffNumber, {
      method: 'GET',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': auth,
        'Cookie': 'sapxslb=0F2F5CCBA8D5854A9C26CB7CAD284FD1; xsSecureId1E75392804D765CC05AC285536FA9A62=14C4F843E2445E48A692A2D219C5C748'
      },
      'maxRedirects': 20
    })
      .then(res => res.json())
      .then(dataOvpm => {
        //logger.info(dataOvpm)

        var _bankCharge = 0;
        var _TotalAmount = 0;

        Object.keys(dataOvpm.COUNTCHEK).forEach(async function (key) {
          var d = dataOvpm.COUNTCHEK[key];
          resolve(d.Counting)
        })
      })
      .catch(err => {
        logger.error("Get Error: " + err.message)
      });

  })
}

const post_Incoming = async (trxid, customerReffNumber, valueAmount, auth, dbName, vendor, successDate) => {
  return new Promise(async (resolve, reject) => {

    var start = new Date();
    var date = moment(start).format('YYYYMMDD')

    try {


      // header
      // mendaptakand Bank Charge
      let bankCharge = await connection.query('SELECT DISTINCT CASE WHEN "CHMODELID" = \'BEN\' THEN 0 ELSE CAST("BANKCHARGE" AS DECIMAL(19,2)) END AS "BANKCHARGE", CAST("AMOUNT" AS DECIMAL(19,2)) "AMOUNT"  FROM paymenth2h."BOS_TRANSACTIONS" WHERE "TRXID" = $1 AND "REFERENCY" = $2', [trxid, customerReffNumber])
      if (bankCharge.rowCount > 0) {
        for (let data of bankCharge.rows) {
          _bankCharge = parseFloat(data.BANKCHARGE)
          _TotalAmount = parseFloat(data.AMOUNT)
        }
      }

      var InvDetails = await getInvoice(trxid, customerReffNumber, valueAmount, auth, dbName, vendor)
      if (InvDetails === "InvoiceEmpty") {
        reject("Invoice Empty")
      }
      var CflDetails = await getCashFlow(trxid, customerReffNumber, valueAmount, auth, dbName, _bankCharge)
      //logger.debug(CflDetails)
      if (CflDetails === "CFLEmpty") {
        reject("Cashflow Empty")
      }

      var getHeaderOP = await getHeader(trxid, customerReffNumber, valueAmount, auth, dbName, InvDetails, CflDetails, _bankCharge, successDate)
      //logger.debug(getHeaderOP)
      if (getHeaderOP === "HeaderEmpty") {
        reject("Header Empty")
      } else {
        let counting = await getCount(dbName, customerReffNumber, auth)
        if (parseFloat(counting) < 1) {
          let createOP = await createOutgoing(trxid, customerReffNumber, valueAmount, auth, dbName, getHeaderOP)
          logger.info(createOP)
          resolve(createOP)
        } else {
          connection.query('UPDATE paymenth2h."BOS_DO_STATUS" SET "flag" = \'1\' WHERE "trxId" = $1 AND "customerReffNumber" = $2;', [trxid, customerReffNumber], async function (error, result, fields) {
            if (error) {
              logger.error(error)
            }
          })
        }
      }



    } catch (error) {
      logger.error("Create Outoing Payment Error, Msg: " + error.message)
      config.inproses = false
      jobs.start()
    }

  });
};

const getDetailUdo = async (trxid, dbName, auth, customerReffNumber, error, type) => {
  return new Promise((resolve, reject) => {
    fetch(config.base_url_xsjs + "/PaymentService/get_detail_lines.xsjs?trxid=" + customerReffNumber + "&dbName=" + dbName, {
      method: 'GET',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': auth,
        'Cookie': 'sapxslb=0F2F5CCBA8D5854A9C26CB7CAD284FD1; xsSecureId1E75392804D765CC05AC285536FA9A62=14C4F843E2445E48A692A2D219C5C748'
      },
      'maxRedirects': 20
    })
      .then(res => res.json())
      .then(data => {

        var detail = []
        var rows = {}
        Object.keys(data.LINES).forEach(async function (key) {
          if (type === data.LINES[key].Type) {
            detail.push({
              Type: data.LINES[key].Type,
              LineId: data.LINES[key].LineId,
              DocEntry: data.LINES[key].DocEntry,
              DocNum: data.LINES[key].DocNum,
              ProcessType: data.LINES[key].ProcessType,
              PaymentMeans: data.LINES[key].PaymentMeans,
              BankPaymentMeans: data.LINES[key].BankPaymentMeans,
              SourcePayment: data.LINES[key].SourcePayment,
              Trxid: trxid,
              CustomerReffNumber: customerReffNumber,
              DbName: dbName,
              Error: error
            })
            resolve(detail)
          }
        })

      })
      .catch(err => {
        logger.error("Get Error: " + err.message)
      });

  });
};

const updateUdo = async (Type, Lines, DocEntry, DocNum, ProcessType, PaymentMeans, BankPaymentMeans, SourcePayment, trxid, customerReffNumber, dbName, error, typePayment, docnumOut) => {
  return new Promise((resolve, reject) => {

    var msg = "";
    logger.info("Tipe dokumen: " + Type)
    if (Type == "OUT") // jika payment out
    {
      var row = {}
      var detailsOut = []
      if (typePayment === "OUT") {
        if (error === true) {
          row = {
            'LineId': Lines,
            'U_STATUS': 'Failed',
            'U_TRXID_SAP': trxid,
            'U_PYMOUT_STATUS': 'H2H'
          }

          msg = 'Failed'
        } else {
          row = {
            'LineId': Lines,
            'U_STATUS': 'Success',
            'U_TRXID_SAP': trxid,
            'U_PYMOUT_STATUS': 'H2H'
          }

          msg = 'Success'
        }

        detailsOut.push(row)
      } else {
        if (error === true) {
          row = {
            'LineId': Lines,
            'U_STATUS': 'Failed',
            'U_TRXID_SAP': trxid,
            'U_PYMOUT_STATUS': docnumOut + ' - Open'
          }

          msg = 'Failed'
        } else {
          row = {
            'LineId': Lines,
            'U_STATUS': 'Success',
            'U_TRXID_SAP': trxid,
            'U_PYMOUT_STATUS': docnumOut + ' - H2H'
          }
          msg = 'Success'
        }
        detailsOut.push(row)
      }



      logger.info("Details Row, Payment OUT (" + dbName + "): " + detailsOut)

      var myHeaders = new nodeFetch.Headers();
      myHeaders.append("Content-Type", "application/json");

      var requestOptions = {
        method: 'PATCH',
        headers: myHeaders,
        body: JSON.stringify({ BOS_PYM_OUT1Collection: detailsOut }),
        redirect: 'follow'
      };

      const fetchWithTimeout = (input, init, timeout) => {
        const controller = new AbortController();
        setTimeout(() => {
          controller.abort();
        }, timeout)

        return fetch(input, { signal: controller.signal, ...init });
      }

      const wait = (timeout) =>
        new Promise((resolve) => {
          setTimeout(() => resolve(), timeout);
        })

      const fetchWithRetry = async (input, init, timeout, retries) => {
        let increseTimeOut = config.interval_retries;
        let count = retries;

        while (count > 0) {
          try {
            return await fetchWithTimeout(input, init, timeout);
          } catch (e) {
            if (e.name !== "AbortError") throw e;
            count--;
            logger.error(
              `fetch patch, retrying patch in ${increseTimeOut}s, ${count} retries left`
            );
            await wait(increseTimeOut)
          }
        }
      }

      logger.info("(OUT) Mobopay - SAP  (" + dbName + "): Update Flag UDO Payment Out" + config.base_url_SL + "/b1s/v2/PYM_OUT(" + parseFloat(DocEntry) + ")")

      return fetchWithRetry(config.base_url_SL + "/b1s/v2/PYM_OUT(" + parseFloat(DocEntry) + ")", requestOptions, 60000, config.max_retries)
        .then(response => {
          response.text()
        }).then(result => {
          console.log(result)
          let _getStatus = getStatus(dbName, parseFloat(DocEntry), Lines, Type, msg, customerReffNumber)
          resolve("Update Success")
        }).catch(error => logger.error("Error Update UDO (" + dbName + ") Ref: " + customerReffNumber, error));

    } else if (Type == "OUTSTATUS") // jika payment out STATUS
    {
      var details = []
      if (error === true) {
        var row = {
          'LineId': Lines,
          'U_STATUS': 'Failed',
          'U_TRXID_SAP': trxid,
          'U_PYMOUT_STATUS': docnumOut + ' - Open'
        }
        msg = 'Failed'
        details.push(row)
      } else {
        var row = {
          'LineId': Lines,
          'U_STATUS': 'Success',
          'U_TRXID_SAP': trxid,
          'U_PYMOUT_STATUS': docnumOut + ' - H2H'
        }
        msg = 'Success'
        details.push(row)
      }


      logger.info(details)
      // update UDO
      logger.info("Details Row, Payment OUT Status (" + dbName + "): " + details)

      var myHeaders = new nodeFetch.Headers();
      myHeaders.append("Content-Type", "application/json");

      var requestOptions = {
        method: 'PATCH',
        headers: myHeaders,
        body: JSON.stringify({ BOS_PYMOUT_STATUS1Collection: details }),
        redirect: 'follow'
      };

      const fetchWithTimeout = (input, init, timeout) => {
        const controller = new AbortController();
        setTimeout(() => {
          controller.abort();
        }, timeout)

        return fetch(input, { signal: controller.signal, ...init });
      }

      const wait = (timeout) =>
        new Promise((resolve) => {
          setTimeout(() => resolve(), timeout);
        })

      const fetchWithRetry = async (input, init, timeout, retries) => {
        let increseTimeOut = config.interval_retries;
        let count = retries;

        while (count > 0) {
          try {
            return await fetchWithTimeout(input, init, timeout);
          } catch (e) {
            logger.info(e.name)
            if (e.name !== "AbortError") throw e;
            count--;
            logger.error(
              `fetch patch, retrying patch in ${increseTimeOut}s, ${count} retries left`
            );
            await wait(increseTimeOut)
          }
        }
      }

      logger.info("(OUT) Mobopay - SAP  (" + dbName + "): Update Flag UDO Payment Out Status" + config.base_url_SL + "/b1s/v2/PYMOUT_STATUS(" + parseFloat(DocEntry) + ")")
      return fetchWithRetry(config.base_url_SL + "/b1s/v2/PYMOUT_STATUS(" + parseFloat(DocEntry) + ")", requestOptions, 60000, config.max_retries)
        .then(response => {
          response.text()
        }).then(async result => {
          console.log(result)
          let _getStatus = getStatus(dbName, parseFloat(DocEntry), Lines, Type, msg, customerReffNumber)
          let getcount = getCountFail(parseFloat(DocEntry), dbName)
          logger.info(getcount)
          resolve("Update Success")

        }).catch(error => logger.error("Error Update UDO (" + dbName + ") Ref: " + customerReffNumber, error));

    } else {
      resolve("Update Failed")
    }
  });
};

const getStatus = async (dbName, Entry, LineId, DocType, Error, Reff) => {
  return new Promise((resolve, reject) => {
    if (DocType == "OUT") {
      logger.info("get Line Status: PYM OUT - " + config.base_url_xsjs + "/PaymentService/get_status_pymout.xsjs?dbName=" + dbName + "&DocEntry=" + parseFloat(Entry) + "&LineId=" + parseFloat(LineId))
      fetch(config.base_url_xsjs + "/PaymentService/get_status_pymout.xsjs?dbName=" + dbName + "&DocEntry=" + parseFloat(Entry) + "&LineId=" + parseFloat(LineId), {
        method: 'GET',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': 'Basic ' + config.auth_basic,
          'Cookie': 'sapxslb=0F2F5CCBA8D5854A9C26CB7CAD284FD1; xsSecureId1E75392804D765CC05AC285536FA9A62=14C4F843E2445E48A692A2D219C5C748'
        },
        'maxRedirects': 20
      })
        .then(res => res.json())
        .then(async _status => {
          logger.info(_status)

          Object.keys(_status.MSGSTATUS).forEach(function (key) {
            //var d = _status.CFL[key];
            logger.info("Status UDO: " + Error)
            if (_status.MSGSTATUS[key].STATUS != Error) {
              // JIKA HASIL NYA TIDAK SAMA, MAKA DISIMPAN DITABEL
              executeQuery('INSERT INTO paymenth2h."BOS_LOG_STATUS_UDO" VALUES ($1, $2, $3, $4, $5, $6, $7)',
                [dbName, Entry, DocType, LineId, Error, '0', Reff], async function (error, result, fields) {
                  if (error) {
                    logger.error(error)
                  } else {
                    resolve("insert success")
                  }
                });
            }


          })
        })
        .catch(err => {
          logger.error("Get Error: " + err)
        });
    } else {
      logger.info("get Line Status: PYM OUT STATUS - " + config.base_url_xsjs + "/PaymentService/get_status_pymout_status.xsjs?dbName=" + dbName + "&DocEntry=" + parseFloat(Entry) + "&LineId=" + parseFloat(LineId))
      fetch(config.base_url_xsjs + "/PaymentService/get_status_pymout_status.xsjs?dbName=" + dbName + "&DocEntry=" + parseFloat(Entry) + "&LineId=" + parseFloat(LineId), {
        method: 'GET',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': 'Basic ' + config.auth_basic,
          'Cookie': 'sapxslb=0F2F5CCBA8D5854A9C26CB7CAD284FD1; xsSecureId1E75392804D765CC05AC285536FA9A62=14C4F843E2445E48A692A2D219C5C748'
        },
        'maxRedirects': 20
      })
        .then(res => res.json())
        .then(async status => {
          logger.info(status)
          Object.keys(status.MSGSTATUS).forEach(function (key) {
            if (status.MSGSTATUS[key].STATUS != Error) {
              // JIKA HASIL NYA TIDAK SAMA, MAKA DISIMPAN DITABEL
              executeQuery('INSERT INTO paymenth2h."BOS_LOG_STATUS_UDO" VALUES ($1, $2, $3, $4, $5, $6, $7);',
                [dbName, Entry, DocType, LineId, Error, '0', Reff], async function (error, result, fields) {
                  if (error) {
                    logger.error(error)
                  } else {
                    resolve("insert success")
                  }
                });
            }
          })
        })
        .catch(err => {
          logger.error("Get Error: " + err.message)
        });
    }

  });
};

const getCountFail = async (Entry, dbName) => {
  return new Promise((resolve, reject) => {
    fetch(config.base_url_xsjs + "/PaymentService/getCountFail.xsjs?Entry=" + Entry + "&dbName=" + dbName, {
      method: 'GET',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': 'Basic U1lTVEVNOlBuQyRnUlBAMjAxOA==',
        'Cookie': 'sapxslb=0F2F5CCBA8D5854A9C26CB7CAD284FD1; xsSecureId1E75392804D765CC05AC285536FA9A62=14C4F843E2445E48A692A2D219C5C748'
      },
      'maxRedirects': 20
    })
      .then(res => res.json())
      .then(data => {
        Object.keys(data.FAIL).forEach(async function (key) {

          if (parseFloat(data.FAIL[key].CountFail) == 0) {

            var myHeaders = new nodeFetch.Headers();
            myHeaders.append("Content-Type", "application/json");

            var requestOptions = {
              method: 'PATCH',
              headers: myHeaders,
              body: JSON.stringify({ "U_DOCSTATUS": "C" }),
              redirect: 'follow'
            };

            const fetchWithTimeout = (input, init, timeout) => {
              const controller = new AbortController();
              setTimeout(() => {
                controller.abort();
              }, timeout)

              return fetch(input, { signal: controller.signal, ...init });
            }

            const wait = (timeout) =>
              new Promise((resolve) => {
                setTimeout(() => resolve(), timeout);
              })

            const fetchWithRetry = async (input, init, timeout, retries) => {
              let increseTimeOut = config.interval_retries;
              let count = retries;

              while (count > 0) {
                try {
                  return await fetchWithTimeout(input, init, timeout);
                } catch (e) {
                  logger.error(e.name)
                  if (e.name !== "AbortError") throw e;
                  count--;
                  console.warn(
                    `fetch Login, retrying login in ${increseTimeOut}s, ${count} retries left`
                  );
                  await wait(increseTimeOut)
                }
              }
            }
            var Session = ""
            return fetchWithRetry(config.base_url_SL + "/b1s/v2/PYMOUT_STATUS(" + parseFloat(Entry) + ")", requestOptions, 60000, config.max_retries)
              .then(response => {
                if (response.status == 204) {
                  response.text()
                } else {
                  fetchWithRetry();
                }
              })
              .then(result => {
                resolve("Success")
              })
              .catch(error => logger.error('error', error));
          }

        })
      })
      .catch(err => {
        logger.error("Get Error Update Status: " + err.message)
      });
  });
};

exports.get_Signature = function (req, res) {
  logger.info('Body: ' + req.body.message)
  var Response_ = signature(req.body.message)
  timeout = true;
  var result = {
    'Signature': Response_
  }
  res.json(result);
  logger.info('Signature: ' + Response_)
}

function signature(message) {
  let _signature
  var Response_ = generateMessageBodySignature(message, privateKey)
  timeout = true;
  _signature = Response_
  return _signature
}

function generateMessageBodySignature(message, privateKey) {
  try {
    const sign = crypto.createSign('RSA-SHA1');
    sign.update(message);
    sign.end();
    const signature = sign.sign(privateKey);
    return signature.toString('base64')
  } catch (error) {
    logger.error(error);
  }
}

exports.get_Token = function (req, res) {
  var token = jwt.sign({ foo: 'bar' }, 'shhhhh');
  var result = {
    'access_token': token,
    'Timeout': '5 minutes'
  }
  res.json(result);
  logger.info('Token: ' + result)
}


exports.get_Verification = async function (req, res) {
  var start = new Date();
  var date = moment(start).format('YYYYMMDD')

  logger.info("(IN) Moboay -> Middleware: Proses Validation Payment, Body ", req.body);
  await connection.query('SELECT "PAYMENTOUTTYPE", "PAYMENTNO", "TRXID", "REFERENCY", "VENDOR", "ACCOUNT", CAST("AMOUNT" AS DECIMAL(18,2)) "AMOUNT"' +
    ', TO_CHAR("TRANSDATE", \'YYYYMMDD\') "TRANSDATE", TO_CHAR("TRANSTIME", \'HH24MIss\') "TRANSTIME", "STATUS", "REASON", CAST("BANKCHARGE" AS DECIMAL(18,2)) "BANKCHARGE", "FLAGUDO"' +
    ', "SOURCEACCOUNT", "TRANSFERTYPE", "CLIENTID"' +
    'FROM paymenth2h."BOS_LOG_TRANSACTIONS"  WHERE "TRXID" = $1 limit 1', [req.body.trxId], async function (error, result, fields) {
      if (error) {
        logger.info(error)
        var rowdata = {
          'result_code': '1',
          'Result_msg': 'OK',
          'Message': error.message,
          'date': moment(start).format('YYYY-MM-DD'),
          'time': moment(start).format('HH:mm:DD')
        };
        logger.info("Error: " + rowdata)
        res.json(rowdata);
      } else {
        //logger.info(result)
        rowdata = {};
        dbkode = "";
        for (var data of result.rows) {
          rowdata = {
            'paymentOutType': data.PAYMENTOUTTYPE,
            'paymentNo': data.PAYMENTNO,
            'vendor': data.VENDOR,
            'cusRef': data.REFERENCY,
            'trxId': data.TRXID,
            'clientId': data.CLIENTID,
            'preferedMethodTransferId': data.TRANSFERTYPE,
            'sourceAccount': data.SOURCEACCOUNT,
            'targetAccount': data.ACCOUNT,
            'amount': data.AMOUNT
          }
        }

        logger.info(rowdata)

        if (rowdata != "") {
          res.json(rowdata);
        } else {
          await connection.query('SELECT "PAYMENTOUTTYPE", "PAYMENTNO", "TRXID", "REFERENCY", "VENDOR", "ACCOUNT", CAST("AMOUNT" AS DECIMAL(18,2)) "AMOUNT"' +
            ', TO_CHAR("TRANSDATE", \'YYYYMMDD\') "TRANSDATE", TO_CHAR("TRANSTIME", \'HH24MIss\') "TRANSTIME", "STATUS", "REASON", CAST("BANKCHARGE" AS DECIMAL(18,2)) "BANKCHARGE", "FLAGUDO"' +
            ', "SOURCEACCOUNT", "TRANSFERTYPE", "CLIENTID"' +
            'FROM paymenth2h."BOS_LOG_TRANSACTIONS"  WHERE "TRXID" = $1 limit 1', [req.body.trxId], function (error, result, fields) {
              if (error) {
                logger.info(error)
                var rowdata = {
                  'result_code': '1',
                  'Result_msg': 'OK',
                  'Message': error.message,
                  'date': moment(start).format('YYYY-MM-DD'),
                  'time': moment(start).format('HH:mm:DD')
                };
                logger.info("Error: " + rowdata)
                res.json(rowdata);
              } else {
                //logger.info(result)
                rowdata = {};
                dbkode = "";
                for (var data of result.rows) {
                  rowdata = {
                    'paymentOutType': data.PAYMENTOUTTYPE,
                    'paymentNo': data.PAYMENTNO,
                    'vendor': data.VENDOR,
                    'cusRef': data.REFERENCY,
                    'trxId': data.TRXID,
                    'clientId': data.CLIENTID,
                    'preferedMethodTransferId': data.TRANSFERTYPE,
                    'sourceAccount': data.SOURCEACCOUNT,
                    'targetAccount': data.ACCOUNT,
                    'amount': data.AMOUNT
                  }
                }
                logger.info(rowdata)
                res.json(rowdata);
              }
            })
        }
      }
    });
}

exports.post_statusPayment = async function (req, res) {
  var start = new Date();
  var date = moment(start).format('YYYYMMDD')

  logger.info("(IN) Moboay -> Middleware: Payment Status, Body ", JSON.stringify(req.body));
  //logger.info("Response Transaksi.......!!!!")
  var rowdata = {
    'resultCode': '0',
    'resultMsg': 'OK',
    'message': 'Transaction Success',
    'date': moment(start).format('YYYY-MM-DD'),
    'time': moment(start).format('HH:mm:DD')
  };
  res.json(rowdata);
  var data_transaction = [];
  data_transaction = [
    req.body.trxId
    , req.body.resultCode
    , req.body.message
    , req.body.errorMessage
    , req.body.debitAccount
    , req.body.creditAccount
    , req.body.valueCurrency
    , parseFloat(req.body.valueAmount)
    , req.body.customerReffNumber
    , '0'
    , req.headers.authorization
    , moment(new Date()).format("yyyyMMDD")
  ]


  //logger.info(data_transaction)


  if (req.body.resultCode == "1") {
    var data_transaction = [];
    data_transaction = [
      req.body.trxId
      , req.body.resultCode
      , req.body.message
      , req.body.errorMessage
      , req.body.debitAccount
      , req.body.creditAccount
      , req.body.valueCurrency
      , parseFloat(req.body.valueAmount)
      , req.body.customerReffNumber
      , '0'
      , req.headers.authorization
      , moment(new Date()).format("yyyyMMDD")
    ]


    // INSERT KE LOG, JIKA SUDAH MENDAPATAKN STATUS PEMBAYARAN DARI MOBOPAY
    await executeQuery('INSERT INTO paymenth2h."BOS_LOG_TRANSACTIONS"("PAYMENTOUTTYPE", "PAYMENTNO", "TRXID", "REFERENCY", "VENDOR", "ACCOUNT", "AMOUNT", "TRANSDATE", "TRANSTIME", "STATUS", "REASON", "BANKCHARGE", "FLAGUDO", "SOURCEACCOUNT", "TRANSFERTYPE", "CLIENTID", "ERRORCODE")' +
      'SELECT "PAYMENTOUTTYPE", "PAYMENTNO",$6, "REFERENCY", "VENDOR", "ACCOUNT", "AMOUNT", $3, $4, 0, $1, "BANKCHARGE", "FLAGUDO", "SOURCEACCOUNT", "TRANSFERTYPE", "CLIENTID", $5' +
      'FROM paymenth2h."BOS_TRANSACTIONS" WHERE "REFERENCY" = $2 limit 1', ['Mobopay ' + req.body.message + ' - Payment Success', req.body.customerReffNumber, moment(start).format("yyyyMMDD"), moment(start).format("HH:mm:ss"), '', req.body.trxId], async function (error, result, fields) {
        if (error) {
          logger.error(error)
        }
      });

    //logger.info(data_transaction)
    // cek payment di log
    await connection.query('SELECT COUNT(*) "COUNTING" FROM paymenth2h."BOS_DO_STATUS" WHERE "trxId" = $1', [req.body.trxId], async function (error, result, fields) {
      if (error) {
        logger.error(error)
      } else {
        for (var data of result.rows) {
          if (data.COUNTING == "0") // jika ada baka tidak diinsert lagi
          {
            let CompanyDb = await executeQuery('SELECT "DB", "PAYMENTOUTTYPE" FROM paymenth2h."BOS_TRANSACTIONS" WHERE "REFERENCY" = $1 limit 1', [req.body.customerReffNumber])
            for await (var db of CompanyDb.rows) {
              await getEntryUdoOut(req.body.customerReffNumber, db.DB, req.body.trxId, db.PAYMENTOUTTYPE, false)
            }
            await executeQuery('INSERT INTO paymenth2h."BOS_DO_STATUS" VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)', data_transaction, function (error, result1, fields) {
              if (error) {

              } else {

              }
            });
          } else // jika belom ada maka diinsett
          {
            var rowdata = {
              'resultCode': '1',
              'resultMsg': 'FAIL',
              'message': 'Transaction has already exist',
              'date': moment(start).format('YYYY-MM-DD'),
              'time': moment(start).format('HH:mm:DD')
            };
            logger.info(rowdata)
            res.json(rowdata);
          }
        }
      }
    });
  }
  if (req.body.resultCode != "1") {

    var data_transaction = [];
    data_transaction = [
      req.body.trxId
      , req.body.resultCode
      , req.body.message
      , req.body.errorMessage
      , req.body.debitAccount
      , req.body.creditAccount
      , req.body.valueCurrency
      , parseFloat(req.body.valueAmount)
      , req.body.customerReffNumber
      , '0'
      , req.headers.authorization
      , moment(new Date()).format("yyyyMMDD")
    ]

    //logger.info(data_transaction)

    let cekError = await executeQuery('SELECT COUNT(*) "errorCount" FROM paymenth2h."BOS_LOG_TRANSACTIONS" WHERE "REFERENCY" = $1 AND "ERRORCODE" = $2', [req.body.customerReffNumber, req.body.resultCode])
    if (cekError.rowCount > 0) {
      for (var data of cekError.rows) {
        if (data.errorCount == "0") {
          await executeQuery('INSERT INTO paymenth2h."BOS_LOG_TRANSACTIONS"("PAYMENTOUTTYPE", "PAYMENTNO", "TRXID", "REFERENCY", "VENDOR", "ACCOUNT", "AMOUNT", "TRANSDATE", "TRANSTIME", "STATUS", "REASON", "BANKCHARGE", "FLAGUDO", "SOURCEACCOUNT", "TRANSFERTYPE", "CLIENTID", "ERRORCODE")' +
            'SELECT "PAYMENTOUTTYPE", "PAYMENTNO",$6, "REFERENCY", "VENDOR", "ACCOUNT", "AMOUNT", $3, $4, 4, $1, "BANKCHARGE", "FLAGUDO", "SOURCEACCOUNT", "TRANSFERTYPE", "CLIENTID", $5' +
            'FROM paymenth2h."BOS_TRANSACTIONS" WHERE "REFERENCY" = $2 limit 1', [req.body.errorMessage, req.body.customerReffNumber, moment(start).format("yyyyMMDD"), moment(start).format("HH:mm:ss"), req.body.resultCode, req.body.trxId], async function (error, result, fields) {
              if (error) {
                logger.error(error)
              }
            });
          var trx_id = "";
          if (req.body.trxId != "") {
            trx_id = req.body.trxId;
            await executeQuery('UPDATE paymenth2h."BOS_TRANSACTIONS" SET "TRXID" = $2, "SUCCESS" = \'N\' WHERE "REFERENCY" = $1', [req.body.customerReffNumber, req.body.trxId])
          }
          await executeQuery('UPDATE paymenth2h."BOS_TRANSACTIONS" SET "INTERFACING" = \'1\', "SUCCESS" = \'N\' WHERE "REFERENCY" = $1', [req.body.customerReffNumber])
          let CompanyDb = await executeQuery('SELECT "DB", "PAYMENTOUTTYPE" FROM paymenth2h."BOS_TRANSACTIONS" WHERE "REFERENCY" = $1 limit 1', [req.body.customerReffNumber])
          for await (var db of CompanyDb.rows) {
            await getEntryUdoOut(req.body.customerReffNumber, db.DB, trx_id, db.PAYMENTOUTTYPE, true)
          }
        } else {
          await executeQuery('INSERT INTO paymenth2h."BOS_LOG_TRANSACTIONS"("PAYMENTOUTTYPE", "PAYMENTNO", "TRXID", "REFERENCY", "VENDOR", "ACCOUNT", "AMOUNT", "TRANSDATE", "TRANSTIME", "STATUS", "REASON", "BANKCHARGE", "FLAGUDO", "SOURCEACCOUNT", "TRANSFERTYPE", "CLIENTID", "ERRORCODE")' +
            'SELECT "PAYMENTOUTTYPE", "PAYMENTNO",$6, "REFERENCY", "VENDOR", "ACCOUNT", "AMOUNT", $3, $4, 4, $1, "BANKCHARGE", "FLAGUDO", "SOURCEACCOUNT", "TRANSFERTYPE", "CLIENTID", $5' +
            'FROM paymenth2h."BOS_TRANSACTIONS" WHERE "REFERENCY" = $2 limit 1', [req.body.errorMessage, req.body.customerReffNumber, moment(start).format("yyyyMMDD"), moment(start).format("HH:mm:ss"), req.body.resultCode, req.body.trxId], async function (error, result, fields) {
              if (error) {
                logger.error(error)
              }
            });
          var trx_id = "";
          if (req.body.trxId != "") {
            trx_id = req.body.trxId;
            await executeQuery('UPDATE paymenth2h."BOS_TRANSACTIONS" SET "TRXID" = $2, "SUCCESS" = \'N\' WHERE "REFERENCY" = $1', [req.body.customerReffNumber, req.body.trxId])
          }
          await executeQuery('UPDATE paymenth2h."BOS_TRANSACTIONS" SET "INTERFACING" = \'1\', "SUCCESS" = \'N\' WHERE "REFERENCY" = $1', [req.body.customerReffNumber])
          let CompanyDb = await executeQuery('SELECT "DB", "PAYMENTOUTTYPE" FROM paymenth2h."BOS_TRANSACTIONS" WHERE "REFERENCY" = $1 limit 1', [req.body.customerReffNumber])
          for await (var db of CompanyDb.rows) {
            await getEntryUdoOut(req.body.customerReffNumber, db.DB, trx_id, db.PAYMENTOUTTYPE, true)
          }
        }
      }
    }
  }
}

exports.post_closePayment = async function (req, res) {
  var start = new Date();
  var date = moment(start).format('YYYYMMDD')

  logger.info("(IN) SAP -> Middleware: Close Transaksi, Body" + req.body);
  //logger.info("Close Transaksi.......!!!!")
  var rowdata = {
    'result': '0',
    'msgCode': 'OK',
    'msgDscriptions': 'Close Transaction Success'
  };
  res.json(rowdata);

  // cek data jika ada
  let result = await connection.query('SELECT COUNT(*) "COUNTING" FROM paymenth2h."BOS_TRANSACTIONS" WHERE "REFERENCY" = $1', [req.body.referency])
  for (var data of result.rows) {
    if (data.COUNTING != "0") // jika ada baka tidak diinsert lagi
    {
      // await executeQuery('INSERT INTO paymenth2h."BOS_LOG_TRANSACTIONS"("PAYMENTOUTTYPE", "PAYMENTNO", "TRXID", "REFERENCY", "VENDOR", "ACCOUNT", "AMOUNT", "TRANSDATE", "TRANSTIME", "STATUS", "REASON", "BANKCHARGE", "FLAGUDO", "SOURCEACCOUNT", "TRANSFERTYPE", "CLIENTID")' +
      //   'SELECT "PAYMENTOUTTYPE", "PAYMENTNO", "TRXID", "REFERENCY", "VENDOR", "ACCOUNT", "AMOUNT", $3, $4, 0, $1, "BANKCHARGE", "FLAGUDO", "SOURCEACCOUNT", "TRANSFERTYPE", "CLIENTID"' +
      //   'FROM paymenth2h."BOS_TRANSACTIONS" WHERE "REFERENCY" = $2 limit 1', ['Generate Outgoing by user', req.body.referency, moment(start).format("yyyyMMDD"), moment(start).format("HH:mm:ss")], async function (error, result, fields) {
      //     if (error) {
      //       logger.error(error)
      //     }
      //   });

      let update = await executeQuery('UPDATE paymenth2h."BOS_TRANSACTIONS" SET "INTERFACING" = \'1\', "SUCCESS" = $2 WHERE "REFERENCY" = $1', [req.body.referency, 'Y'])
      await executeQuery('UPDATE paymenth2h."BOS_DO_STATUS" SET "flag" = \'1\' WHERE "customerReffNumber" = $1', [req.body.referency], async function (error, result, fields) {
        if (error) {
          logger.error(error)
        }
      });
    }
  }
}

exports.get_testing = function (req, res) {
  pool.query('select "ID_DB","NM_DB"  from paymenth2h."BOS_DB_LIST"', function (error, result, fields) {
    if (error) {
      logger.error(error)
    } else {
      for (var data of result.rows) {
        logger.info(data.ID_DB)
      }
    }
  });
}

const getValidationAmount = (custReff, dbName, auth) => {
  return new Promise((resolve, reject) => {
    fetch(config.base_url_xsjs + "/PaymentService/get_validation_amount.xsjs?reff=" + custReff + "&dbName=" + dbName, {
      method: 'GET',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': auth,
        'Cookie': 'sapxslb=0F2F5CCBA8D5854A9C26CB7CAD284FD1; xsSecureId1E75392804D765CC05AC285536FA9A62=14C4F843E2445E48A692A2D219C5C748'
      },
      'maxRedirects': 20
    })
      .then(res => res.json())
      .then(valAmount => {
        var rowdata = {}
        // print log
        logger.info("(OUT) Middleware - XSJS (" + dbName + "): GET Validation Amount " + custReff + " :" + config.base_url_xsjs + "/PaymentService/get_validation_amount.xsjs?reff=" + custReff + "&dbName=" + dbName)

        Object.keys(valAmount.VALAMOUNT).forEach(function (key) {
          var d = valAmount.VALAMOUNT[key];

          rowdata = {
            "CustomerReff": d.CustomerReff,
            "Amount": d.TotalAmount
          };
        })

        if (rowdata === null) {
          resolve("AmountNull")
        } else {
          resolve(rowdata)
        }

      })
      .catch(err => {
        logger.error("Get Error: " + err.message)
      });
  })
}

exports.post_logTransactions = async function (req, res) {

  try {
    var start = new Date();
    var date = moment(start).format('YYYYMMDD')
    var amountDetail = 0;
    var reffDetail = "";
    //jobs.stop();

    //logger.info(req.body) 

    logger.info("(IN) SAP - Middleware: Insert Data Transaksi, Body: ", req.body)

    var Db = await getConfigDB(req.body.db);

    let custreff = ""
    //await getConfigDB(req.body.db);
    custreff = req.body.referency
    var data_transaction = [];
    var dataDetail = [];
    data_transaction = [req.body.paymentOutType
      , req.body.paymentNo
      , req.body.trxId
      , req.body.referency
      , req.body.vendor
      , req.body.account
      , req.body.amount
      , req.body.transDate
      , req.body.transTime
      , req.body.status
      , req.body.reason
      , req.body.bankCharge
      , req.body.flagUdo
      , req.body.sourceAccount
      , req.body.transferType
      , req.body.clientId
      , req.body.db
      , '0'
      , ''
      , req.body.chargingModelId
      , req.body.action
      , "0"
      , "N"
    ]



    // untuk pengecekan data jika payment sudah berhasil di mobopay
    for await (let index of req.body.details) {

      amountDetail = index.debitAmount;
      reffDetail = index.customerReferenceNumber;
      dataDetail = [
        index.preferredTransferMethodId,
        index.debitAccount,
        index.creditAccount,
        index.customerReferenceNumber,
        index.chargingModelId,
        index.defaultCurrencyCode,
        index.debitCurrency,
        index.creditCurrency,
        index.chargesCurrency,
        index.remark1,
        index.remark2,
        index.remark3,
        index.remark4,
        index.paymentMethod,
        index.extendedPaymentDetail,
        index.preferredCurrencyDealId,
        index.destinationBankCode,
        index.beneficiaryBankName,
        index.switcher,
        index.beneficiaryBankAddress1,
        index.beneficiaryBankAddress2,
        index.beneficiaryBankAddress3,
        index.valueDate,
        index.debitAmount,
        index.beneficiaryName,
        index.beneficiaryEmailAddress,
        index.beneficiaryAddress1,
        index.beneficiaryAddress2,
        index.beneficiaryAddress3,
        index.debitAccount2,
        index.debitAmount2,
        index.debitAccount3,
        index.debitAmount3,
        index.creditAccount2,
        index.creditAccount3,
        index.creditAccount4,
        index.instructionCode1,
        index.instructionRemark1,
        index.instructionCode2,
        index.instructionRemark2,
        index.instructionCode3,
        index.instructionRemark3,
        index.paymentRemark1,
        index.paymentRemark2,
        index.paymentRemark3,
        index.paymentRemark4,
        index.reservedAccount1,
        index.reservedAmount1,
        index.reservedAccount2,
        index.reservedAmount2,
        index.reservedAccount3,
        index.reservedAmount3,
        index.reservedField1,
        index.reservedField2,
        index.reservedField3,
        index.reservedField4,
        index.reservedField5,
        index.reservedField6,
        index.reservedField7,
        index.reservedField8,
        index.reservedField9,
        index.reservedField10,
        index.reservedField11,
        index.reservedField12,
        index.reservedField13,
        index.reservedField14,
        index.reservedField15,
        index.reservedField16,
        index.reservedField17,
        index.reservedField18,
        index.reservedField19,
        index.reservedField20,
      ]

    }

    if (req.body.referency != reffDetail) {
      res.json({
        'result': "OK",
        'msgCode': "1",
        'msgDscriptions': 'Transaction is Invalid'
      });
    } else {
      var valAmount = await getValidationAmount(req.body.referency, req.body.db, "Basic " + config.auth_basic)
      if ((parseFloat(valAmount.Amount) === parseFloat(req.body.amount)) && (parseFloat(amountDetail) === parseFloat(valAmount.Amount))) {
        //untuk pengecekan data jika payment sudah berhasil di mobopay
        await connection.query('SELECT COUNT(*) AS "Counting"  FROM paymenth2h."BOS_DO_STATUS" WHERE "customerReffNumber" = $1', [req.body.referency], async function (errorCheck, resultCheck, fieldCheck) {
          if (errorCheck) {
            logger.error(errorCheck)
          } else {
            for (var data of resultCheck.rows) {
              if (data.Counting == "0") {
                if (req.body.paymentOutType == "OUT") {
                  await connection.query('SELECT COUNT(*) AS "Counting"  FROM paymenth2h."BOS_TRANSACTIONS" WHERE "REFERENCY" = $1', [req.body.referency], async function (error, result, fields) {
                    if (error) {
                      logger.error(error)
                    } else {
                      for (var data of result.rows) {
                        if (data.Counting == "0") {
                          await connection.query('INSERT INTO paymenth2h."BOS_TRANSACTIONS" VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23)', data_transaction, async function (error, result1, fields) {
                            if (error) {
                              logger.error(error)
                            } else {
                              await connection.query('INSERT INTO paymenth2h."BOS_TRANSACTIONS_D" VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38, $39, $40, $41, $42, $43, $44, $45, $46, $47, $48, $49, $50, $51, $52, $53, $54, $55, $56, $57, $58, $59, $60, $61, $62, $63, $64, $65, $66, $67, $68, $69, $70, $71, $72);', dataDetail, async function (error, result, fields) {
                                if (error) {
                                  logger.error(error)
                                } else {
                                  // jika berhasil di add maka insert ke log
                                  await connection.query('INSERT INTO paymenth2h."BOS_LOG_TRANSACTIONS"("PAYMENTOUTTYPE", "PAYMENTNO", "TRXID", "REFERENCY", "VENDOR", "ACCOUNT", "AMOUNT", "TRANSDATE", "TRANSTIME", "STATUS", "REASON", "BANKCHARGE", "FLAGUDO", "SOURCEACCOUNT", "TRANSFERTYPE", "CLIENTID", "ERRORCODE")' +
                                    'SELECT "PAYMENTOUTTYPE", "PAYMENTNO","TRXID", "REFERENCY", "VENDOR", "ACCOUNT", "AMOUNT", $3, $4, $1, $6, "BANKCHARGE", "FLAGUDO", "SOURCEACCOUNT", "TRANSFERTYPE", "CLIENTID", $5' +
                                    'FROM paymenth2h."BOS_TRANSACTIONS" WHERE "REFERENCY" = $2 limit 1', [1, req.body.referency, moment(start).format("yyyyMMDD"), moment(start).format("HH:mm:ss"), '', ''], async function (error, result, fields) {
                                      if (error) {
                                        logger.error(error)
                                      } else {
                                        var data1 = {
                                          'result': "0",
                                          'msgCode': "0",
                                          'msgDscriptions': "Success"
                                        }
                                        res.json(data1);
                                      }
                                      //await connection.query('UPDATE paymenth2h."BOS_TRANSACTIONS" SET "INTERFACING" = \'1\', "SUCCESS" = \'N\' WHERE "REFERENCY" = $1', [referency])
                                    });
                                }
                              });

                            }
                          });
                        } else {
                          var data1 = {
                            'result': "0",
                            'msgCode': "0",
                            'msgDscriptions': "Success"
                          }
                          res.json(data1);
                          logger.info(data)
                        }
                      }
                    }
                  });

                } else { // jika dari payment outstatus
                  let cekError = await connection.query('SELECT CAST(coalesce(MAX("RESENDCOUNT"),\'0\') as INT) + 1 as "RESENDCOUNT" FROM  paymenth2h."BOS_TRANSACTIONS"  WHERE "REFERENCY"=$1', [req.body.referency])
                  if (cekError.rowCount > 0) {
                    for (var data of cekError.rows) {
                      logger.info("check resend count (" + req.body.db + "): " + req.body.referency + ", " + parseFloat(data.RESENDCOUNT))
                      if (parseFloat(data.RESENDCOUNT) <= config.resend_payment_retries) {
                        //await connection.query('UPDATE  paymenth2h."BOS_TRANSACTIONS_D" SET "preferredTransferMethodId" = $1  WHERE "customerReferenceNumber" = $2', [req.body.chargingModelId, req.body.referency])


                        await connection.query('DELETE FROM paymenth2h."BOS_TRANSACTIONS" WHERE "REFERENCY" = $1', [req.body.referency], async function (error, result, fields) {
                          if (error) {
                            logger.error(error)
                          } else {

                            var data1 = {
                              'result': "0",
                              'msgCode': "0",
                              'msgDscriptions': "Success"
                            }
                            res.json(data1);

                            await connection.query('INSERT INTO paymenth2h."BOS_TRANSACTIONS" VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23)', data_transaction, async function (error1, result1, fields) {
                              if (error1) {
                                logger.error(error1)
                              } else {
                                await connection.query('DELETE FROM paymenth2h."BOS_TRANSACTIONS_D" WHERE "customerReferenceNumber" = $1', [req.body.referency])
                                await connection.query('INSERT INTO paymenth2h."BOS_TRANSACTIONS_D" VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38, $39, $40, $41, $42, $43, $44, $45, $46, $47, $48, $49, $50, $51, $52, $53, $54, $55, $56, $57, $58, $59, $60, $61, $62, $63, $64, $65, $66, $67, $68, $69, $70, $71, $72);', dataDetail, async function (error, result, fields) {
                                  if (error) {
                                    logger.error(error)
                                  }
                                });
                                await connection.query('UPDATE  paymenth2h."BOS_TRANSACTIONS" SET "RESENDCOUNT" = $1  WHERE "REFERENCY" = $2', [data.RESENDCOUNT, req.body.referency])

                                await connection.query('INSERT INTO paymenth2h."BOS_LOG_TRANSACTIONS"("PAYMENTOUTTYPE", "PAYMENTNO", "TRXID", "REFERENCY", "VENDOR", "ACCOUNT", "AMOUNT", "TRANSDATE", "TRANSTIME", "STATUS", "REASON", "BANKCHARGE", "FLAGUDO", "SOURCEACCOUNT", "TRANSFERTYPE", "CLIENTID", "ERRORCODE")' +
                                  'SELECT "PAYMENTOUTTYPE", "PAYMENTNO","TRXID", "REFERENCY", "VENDOR", "ACCOUNT", "AMOUNT", $3, $4, $1, $6, "BANKCHARGE", "FLAGUDO", "SOURCEACCOUNT", "TRANSFERTYPE", "CLIENTID", $5' +
                                  'FROM paymenth2h."BOS_TRANSACTIONS" WHERE "REFERENCY" = $2 limit 1', [3, req.body.referency, moment(start).format("yyyyMMDD"), moment(start).format("HH:mm:ss"), '', ''], async function (error, result, fields) {
                                    if (error) {
                                      logger.error(error)
                                    }
                                    //await connection.query('UPDATE paymenth2h."BOS_TRANSACTIONS" SET "INTERFACING" = \'1\', "SUCCESS" = \'N\' WHERE "REFERENCY" = $1', [referency])
                                  });
                              }
                            });
                          }
                        });
                      } else {

                        //CEK JIKA MASIH ERROR CODE MAX RESED
                        await connection.query('UPDATE  paymenth2h."BOS_TRANSACTIONS_D" SET "preferredTransferMethodId" = $1  WHERE "customerReferenceNumber" = $2', [req.body.chargingModelId, req.body.referency])

                        let cekError = await connection.query('SELECT COUNT(*) "errorCount", "TRXID","ERRORCODE"  FROM paymenth2h."BOS_LOG_TRANSACTIONS"  WHERE "REFERENCY" = $1 ' +
                        'AND "PAYMENTOUTTYPE" = \'OUTSTATUS\' ' +
                        'AND "STATUS" = \'3\' ' +
                        'GROUP BY "TRXID", "ERRORCODE"', [req.body.referency])

                        var trxid;
                        if (cekError.rowCount >= 0 || cekError.rowCount == "") {
                          for (var data of cekError.rows) {

                            trxid = data.TRXID

                            if (data.errorCount == "3" || data.errorCount == "") {
                              await connection.query('INSERT INTO paymenth2h."BOS_LOG_TRANSACTIONS"("PAYMENTOUTTYPE", "PAYMENTNO", "TRXID", "REFERENCY", "VENDOR", "ACCOUNT", "AMOUNT", "TRANSDATE", "TRANSTIME", "STATUS", "REASON", "BANKCHARGE", "FLAGUDO", "SOURCEACCOUNT", "TRANSFERTYPE", "CLIENTID", "ERRORCODE")' +
                                'SELECT "PAYMENTOUTTYPE", "PAYMENTNO","TRXID", "REFERENCY", "VENDOR", "ACCOUNT", "AMOUNT", $3, $4, $1, $5, "BANKCHARGE", "FLAGUDO", "SOURCEACCOUNT", "TRANSFERTYPE", "CLIENTID", $6' +
                                'FROM paymenth2h."BOS_TRANSACTIONS" WHERE "REFERENCY" = $2 limit 1', [4, req.body.referency, moment(start).format("yyyyMMDD"), moment(start).format("HH:mm:ss"), 'Maximum Resend 3x', 'SAP-MR01'], async function (error, result, fields) {
                                  if (error) {
                                    logger.error(error)
                                  }
                                });
                            }
                            
                            if(data.errorCount == "3"){
                              res.json({
                                'result': "OK",
                                'msgCode': "1",
                                'msgDscriptions': 'Maximum Resend 3x'
                                // 'result': "0",
                                // 'msgCode': "0",
                                // 'msgDscriptions': "Success"
                              });
                            }
                          }
                        } 

                        
                        await getDetailUdo(req.body.referency, req.body.db, trxid, req.body.paymentOutType, "Y")
                      }
                    }
                  }
                }
              } else {
                await connection.query('UPDATE paymenth2h."BOS_DO_STATUS" SET "flag" = \'1\' WHERE "customerReffNumber" = $1', [req.body.referency], async function (error, result, fields) {
                  if (error) {
                  }
                });
                await connection.query('INSERT INTO paymenth2h."BOS_LOG_TRANSACTIONS"("PAYMENTOUTTYPE", "PAYMENTNO", "TRXID", "REFERENCY", "VENDOR", "ACCOUNT", "AMOUNT", "TRANSDATE", "TRANSTIME", "STATUS", "REASON", "BANKCHARGE", "FLAGUDO", "SOURCEACCOUNT", "TRANSFERTYPE", "CLIENTID", "ERRORCODE")' +
                  'SELECT "PAYMENTOUTTYPE", "PAYMENTNO","TRXID", "REFERENCY", "VENDOR", "ACCOUNT", "AMOUNT", $3, $4, $1, $5, "BANKCHARGE", "FLAGUDO", "SOURCEACCOUNT", "TRANSFERTYPE", "CLIENTID", $6' +
                  'FROM paymenth2h."BOS_TRANSACTIONS" WHERE "REFERENCY" = $2 limit 1', [4, req.body.referency, moment(start).format("yyyyMMDD"), moment(start).format("HH:mm:ss"), "Can't Reproses this payment, Payment has been Success", 'SAP-MR02'], async function (error, result, fields) {
                    if (error) {
                      logger.info(error)
                    }
                  });

                res.json({
                  'result': "OK",
                  'msgCode': "2",
                  'msgDscriptions': 'Payment Success'
                });
              }
            }
          }
        })

        // res.json({
        //   'result': "OK",
        //   'msgCode': "0",
        //   'msgDscriptions': 'Transaction Valid'
        // });
      } else {
        logger.info('Payment (' + req.body.db + ') Interfacing, Ref: ' + req.body.referency + ', Valisation Amount Failed');

        // update interfacingnya mendjadi 1
        await connection.query('UPDATE paymenth2h."BOS_TRANSACTIONS" SET "INTERFACING" = \'1\' WHERE "REFERENCY" = $1', [req.body.referency])

        // insert ke log, jika nilai tidak sama
        await connection.query('INSERT INTO paymenth2h."BOS_LOG_TRANSACTIONS"("PAYMENTOUTTYPE", "PAYMENTNO", "TRXID", "REFERENCY", "VENDOR", "ACCOUNT", "AMOUNT", "TRANSDATE", "TRANSTIME", "STATUS", "REASON", "BANKCHARGE", "FLAGUDO", "SOURCEACCOUNT", "TRANSFERTYPE", "CLIENTID", "ERRORCODE")' +
          'SELECT "PAYMENTOUTTYPE", "PAYMENTNO","TRXID", "REFERENCY", "VENDOR", "ACCOUNT", "AMOUNT", $3, $4, 4, $1, "BANKCHARGE", "FLAGUDO", "SOURCEACCOUNT", "TRANSFERTYPE", "CLIENTID",  $5' +
          'FROM paymenth2h."BOS_TRANSACTIONS" WHERE "REFERENCY" = $2 limit 1', ["SAP - Validation Amount Failed", req.body.referency, moment(start).format("yyyyMMDD"), moment(start).format("HH:mm:ss"), "EOD"]);


        // update status di payment
        var getEntry = await getDetailUdo(req.body.referency, req.body.db, "", req.body.paymentOutType, "Y")
        if (Object.keys(getEntry).length > 0) {
          let Login = await LoginSL(req.body.db)
          if (Login !== "LoginError") {
            let _updateUdo = await updateUdo(getEntry.DocEntry, getEntry.LineId, getEntry.TrxId, getEntry.Auth
              , getEntry.DbName, getEntry.Referency, getEntry.OutType, "Y", req.body.paymentNo, req.body.paymentOutType, req.body.paymentNo)
          } else {
            logger.error(Login);
          }

        } else {
          logger.error("Get DocEntry Error (" + req.body.db + "): " + req.body.referency);
        }

        res.json({
          'result': "OK",
          'msgCode': "1",
          'msgDscriptions': 'Transaction is Invalid'
        });
      }
    }
  } catch (error) {
    res.status(300).json({
      'result': "OK",
      'msgCode': "1",
      'msgDscriptions': 'Transaction is Invalid'
    });
  }


}

// function detail transaksi
exports.post_detailTrasactions = async function (req, res) {
  var start = new Date();
  var date = moment(start).format('YYYYMMDD')

  logger.info("(IN) SAP - Middleware: Details Payment, Body: ", req.body)
  var data = [];
  data = [req.body.preferredTransferMethodId,
  req.body.debitAccount,
  req.body.creditAccount,
  req.body.customerReferenceNumber,
  req.body.chargingModelId,
  req.body.defaultCurrencyCode,
  req.body.debitCurrency,
  req.body.creditCurrency,
  req.body.chargesCurrency,
  req.body.remark1,
  req.body.remark2,
  req.body.remark3,
  req.body.remark4,
  req.body.paymentMethod,
  req.body.extendedPaymentDetail,
  req.body.preferredCurrencyDealId,
  req.body.destinationBankCode,
  req.body.beneficiaryBankName,
  req.body.switcher,
  req.body.beneficiaryBankAddress1,
  req.body.beneficiaryBankAddress2,
  req.body.beneficiaryBankAddress3,
  req.body.valueDate,
  req.body.debitAmount,
  req.body.beneficiaryName,
  req.body.beneficiaryEmailAddress,
  req.body.beneficiaryAddress1,
  req.body.beneficiaryAddress2,
  req.body.beneficiaryAddress3,
  req.body.debitAccount2,
  req.body.debitAmount2,
  req.body.debitAccount3,
  req.body.debitAmount3,
  req.body.creditAccount2,
  req.body.creditAccount3,
  req.body.creditAccount4,
  req.body.instructionCode1,
  req.body.instructionRemark1,
  req.body.instructionCode2,
  req.body.instructionRemark2,
  req.body.instructionCode3,
  req.body.instructionRemark3,
  req.body.paymentRemark1,
  req.body.paymentRemark2,
  req.body.paymentRemark3,
  req.body.paymentRemark4,
  req.body.reservedAccount1,
  req.body.reservedAmount1,
  req.body.reservedAccount2,
  req.body.reservedAmount2,
  req.body.reservedAccount3,
  req.body.reservedAmount3,
  req.body.reservedField1,
  req.body.reservedField2,
  req.body.reservedField3,
  req.body.reservedField4,
  req.body.reservedField5,
  req.body.reservedField6,
  req.body.reservedField7,
  req.body.reservedField8,
  req.body.reservedField9,
  req.body.reservedField10,
  req.body.reservedField11,
  req.body.reservedField12,
  req.body.reservedField13,
  req.body.reservedField14,
  req.body.reservedField15,
  req.body.reservedField16,
  req.body.reservedField17,
  req.body.reservedField18,
  req.body.reservedField19,
  req.body.reservedField20
  ]

  await executeQuery('DELETE FROM paymenth2h."BOS_TRANSACTIONS_D" WHERE "customerReferenceNumber" = $1', [req.body.customerReferenceNumber])

  await connection.query('INSERT INTO paymenth2h."BOS_TRANSACTIONS_D" VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38, $39, $40, $41, $42, $43, $44, $45, $46, $47, $48, $49, $50, $51, $52, $53, $54, $55, $56, $57, $58, $59, $60, $61, $62, $63, $64, $65, $66, $67, $68, $69, $70, $71, $72);', data, async function (error, result, fields) {
    if (error) {
      logger.error(error)
      data = {
        'result': "Failed",
        'msgCode': "1",
        'msgDscriptions': error.message
      }
      res.json(data);
    } else {
      data = {
        'result': "OK",
        'msgCode': "0",
        'msgDscriptions': "Success"
      }
      res.json(data);
      //await executeQuery('UPDATE  paymenth2h."BOS_TRANSACTIONS_D" SET "preferredTransferMethodId" = $1  WHERE "customerReferenceNumber" = $2', [req.body.preferredTransferMethodId, req.body.customerReferenceNumber])
    }
  });
}

exports.get_kodeDb = async function (req, res) {
  connection.query('select * from paymenth2h."BOS_DB_LIST" WHERE "NM_DB" = $1', [req.body.dbName], function (error, result, fields) {
    if (error) {
      logger.error(error)
      //pool.end()
    } else {
      // pool.end()
      rowdata = {};
      dbkode = "";
      for (var data of result.rows) {
        rowdata = {
          'dbName': data.KODE_DB,
          'clientId': data.ID_DB,
          'clientSecret': data.CLIENT_SECRET
        }

        dbkode = data.KODE_DB
        logger.info("Kode Database: " + dbkode)

        var fs = require('fs');
        fs.writeFile('configDB.txt', req.body.dbName, function (err) {
          if (err) throw err;
          console.log('Saved!');
        });
      }
      res.json(rowdata);
    }
  });


  // pool.end()
}

exports.get_History = async function (req, res) {
  logger.info("Get History, Ref: " + req.body.referency);


  connection.query('SELECT DISTINCT "PAYMENTOUTTYPE", "PAYMENTNO", "TRXID", "REFERENCY", "VENDOR", "ACCOUNT", CAST("AMOUNT" AS DECIMAL(18,2)) "AMOUNT"' +
    ', TO_CHAR("TRANSDATE", \'YYYYMMDD\') "TRANSDATE", TO_CHAR("TRANSTIME", \'HH24MIss\') "TRANSTIME", "STATUS", "REASON", CAST("BANKCHARGE" AS DECIMAL(18,2)) "BANKCHARGE", "FLAGUDO"' +
    ', "SOURCEACCOUNT", "TRANSFERTYPE", "CLIENTID", "ERRORCODE"' +
    'FROM paymenth2h."BOS_LOG_TRANSACTIONS" WHERE "REFERENCY" = $1', [req.body.referency], async function (error, result, fields) {
      if (error) {
        logger.error(error)
        //pool.end()
      } else {
        rowdata = {};
        resultrow = []
        dbkode = "";
        for (var data of result.rows) {
          rowdata = {
            'paymentOutType': data.PAYMENTOUTTYPE,
            'paymentNo': data.PAYMENTNO,
            'trxId': data.TRXID,
            'referency': data.REFERENCY,
            'vendor': data.VENDOR,
            'account': data.ACCOUNT,
            'amount': data.AMOUNT,
            'transDate': data.TRANSDATE,
            'transTime': data.TRANSTIME,
            'status': data.STATUS,
            'reason': data.REASON,
            'bankCharge': data.BANKCHARGE,
            'flagUdo': data.FLAGUDO,
            'sourceAccount': data.SOURCEACCOUNT,
            'transferType': data.TRANSFERTYPE,
            'clientId': data.CLIENTID,
            'errorCode': data.ERRORCODE
          }

          // jika kalau ada time out maka update flag
          if (data.ERRORCODE == "EOD") {
            //await updateStatusCutOff(data.REFERENCY, data.DB)
          }


          resultrow.push(rowdata)
        }

        res.json(resultrow);
      }
    });
  // pool.end()
  if (!config.inproses) {
    jobs.start();
  }

}

// function detail transaksi
exports.post_configDb = async function (req, res) {
  var start = new Date();
  var date = moment(start).format('YYYYMMDD')

  var dbName = req.body.dbName

  await connection.query('select COUNT(*) "dbCount" from paymenth2h."BOS_DB_LIST" WHERE "NM_DB" = $1', [dbName], function (error, result, fields) {
    if (error) {
      logger.error(error)
    } else {
      for (var data of result.rows) {
        if (data.dbCount == "0") {

        } else {
          // await executeQuery('INSERT INTO paymenth2h."BOS_LOG_TRANSACTIONS"("PAYMENTOUTTYPE", "PAYMENTNO", "TRXID", "REFERENCY", "VENDOR", "ACCOUNT", "AMOUNT", "TRANSDATE", "TRANSTIME", "STATUS", "REASON", "BANKCHARGE", "FLAGUDO", "SOURCEACCOUNT", "TRANSFERTYPE", "CLIENTID", "ERRORCODE")' +
          //                 'SELECT "PAYMENTOUTTYPE", "PAYMENTNO","TRXID", "REFERENCY", "VENDOR", "ACCOUNT", "AMOUNT", $3, $4, $1, $5, "BANKCHARGE", "FLAGUDO", "SOURCEACCOUNT", "TRANSFERTYPE", "CLIENTID", $6' +
          //                 'FROM paymenth2h."BOS_TRANSACTIONS" WHERE "REFERENCY" = $2 limit 1', [4, req.body.referency, moment(start).format("yyyyMMDD"), moment(start).format("HH:mm:ss"), 'Maximum Resend', 'SAP-MR01'], async function (error, result, fields) {
          //                   if (error) {
          //                     logger.info(error)
          //                   }
          //                 });
        }
      }
    }
  });
}

exports.post_pdateFlag = function (req, res) {
  connection.query('CALL paymenth2h.Update_Flag($1)', [req.body.trxId], async function (error, result, fields) {
    if (error) {
      logger.info(error)
      //pool.end()
    } else {
      // pool.end()
      rowdata = {};
      dbkode = "";
      for (var data of result.rows) {
        rowdata = {
          'dbName': data.KODE_DB,
          'clientId': data.ID_DB,
          'clientSecret': data.CLIENT_SECRET
        }

        dbkode = data.KODE_DB
      }
      logger.info("Kode Database: " + dbkode)
      res.json(rowdata);
    }
  });
  // pool.end()
}
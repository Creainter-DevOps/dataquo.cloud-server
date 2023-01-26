import csv from 'csv-parser';
import readline from 'readline';
import express from 'express';
import fs from 'fs';
import http from 'http';
import https from 'https';
import {MongoClient} from 'mongodb';
import pg from 'pg';
import {exec} from 'child_process';


import { PathExist, __rootDir, IsDir, EnumDir, ParseToHtml } from './utility.js';


const hash     = 'ef499bd1-891e-41aa-aa64-72b93a75dee7';
const filepath = "datasets/" + hash + ".csv";
//"5a24370e-da9f-4519-87e7-a9565c08670f.csv";
const recurso_id = 43;

var xloader_id = null;

console.log('pg', pg.Pool);

const pool = new pg.Pool({
  user: 'postgres',
  database: 'creainter',
  password: 'meteLPBDo0gmsc3d',
  port: 5432,
  host: '10.21.32.3',
});


async function xloader_inicio() {
  var xloader = await pool.query("SELECT * FROM datosabiertos.fn_xloader_inicio($1)", [recurso_id]);
  return xloader.rows[0];
}
async function xloader_fin(params) {
  var xloader = await pool.query("SELECT * FROM datosabiertos.fn_xloader_fin($1, $2, $3, $4)", params);
  return xloader.rows[0];
}
var xloader = await xloader_inicio();
xloader_id = xloader.xloader_id;

console.log('xloader_id', xloader_id);

function db(cb) {
  let url = "mongodb://localhost:27017/";
  return MongoClient.connect(
      url,
      { useNewUrlParser: true, useUnifiedTopology: true },
      (err, client) => {
        if (err) throw err;
        cb(client);
      });

}
var columns = {};
var cantidad = 0;

db(function(client) {
  var ccc = 0;

  let readStream = fs.createReadStream(filepath, {
    autoClose: true,
    encoding: 'utf8'
  })
  .on('error', () => {
    console.log('ERRROR');
  })
  .pipe(csv({
    delimiter: ",",
    quote: '"',
  }))
  .on('data', function(line) {
    cantidad++;
    for(var key in line) {
      if(line.hasOwnProperty(key)) {
        if(typeof columns[key] === 'undefined') {
          columns[key] = {
            selectable: true,
            data: {}  
          };
        }
        if(typeof columns[key][line[key]] === 'undefined') {
          columns[key].data[line[key]] = 0;
        }
        if(columns[key].selectable) {
          columns[key].data[line[key]]++;
          if(Object.keys(columns[key].data) > 1000) {
            columns[key].selectable = false;
          }
        }
      }
    }
    client
      .db('datosabiertos')
      .collection(hash)
      .insertOne(line);
      console.log(cantidad);
  })
  .on('end', () => {
    console.log("read done");
    let columnas = Object.keys(columns);
    console.log('columnas', columnas);
    for(var index in columnas) {
      if(columnas.hasOwnProperty(index)) {
        pool.query("SELECT datosabiertos.fn_xloader_diccionario($1, $2)", [xloader_id, columnas[index]]);
      }
    }
    var xloader = xloader_fin([xloader_id, columnas.length, cantidad, 'csv']);
    console.log('fin', xloader);
  });
});

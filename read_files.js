import csv from 'csv-parser';
import readline from 'readline';
import process from 'node:process';
//import express from 'express';
import fs from 'fs';
import http from 'http';
import https from 'https';
import {MongoClient} from 'mongodb';
import pg from 'pg';
import {exec} from 'child_process';
import { PathExist, __rootDir, IsDir, EnumDir, ParseToHtml } from './utility.js';

const pool = new pg.Pool({
  user: 'postgres',
  database: 'creainter',
  password: 'meteLPBDo0gmsc3d',
  port: 5432,
  host: '35.202.192.69',
});

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

console.log('pg', pg.Pool);

const args = process.argv.slice(2); 
console.log(args);

const action = args[0];

if ( action == 'read' ){

  const filepath = parseInt(args[1]); 

  let readStream = fs.createReadStream(filepath, {
    autoClose: true,
    encoding: 'utf8'
  })
  .on('error',async (err) => {

    await xloader_log(xloader_id,"Error en la carga");	
    console.log('ERRROR');

	  console.log(err);
	  process.exit();
  })
  .on('data', function(line) {
	  console.log(data);
  })

    //console.log('xloader_id', xloader_id);
  }

console.log( {'status' : true } );



async function xloader_inicio(recurso_id) {
  var xloader = await pool.query("SELECT * FROM datosabiertos.fn_xloader_inicio($1)", [recurso_id]);
  
  return xloader.rows[0];
}

async function xloader_fin(params) {
  var xloader = await pool.query("SELECT * FROM datosabiertos.fn_xloader_fin($1, $2, $3, $4)", params);
  //process.exit();
  return xloader.rows[0];
}

async function xloader_log(xloader_id, message ){
   var log = await pool.query("insert into datosabiertos.xloader_log( loader_id, mensaje, fecha ) values( $1, $2, CURRENT_TIMESTAMP)", [xloader_id,message ]);
   //return xloader.row[0];	   
}


var columns = {};
var cantidad = 0;

async function load( xloader_id, filepath, hash ){
 
  await xloader_log(xloader_id,"Inciando carga dataset");	

  await xloader_log(xloader_id,"Cargando dataset...");	
  console.log(xloader_id, filepath );

db(async function(client) {
  var ccc = 0;
  
  let readStream = fs.createReadStream(filepath, {
    autoClose: true,
    encoding: 'utf8'
  })
  .on('error',async (err) => {

    await xloader_log(xloader_id,"Error en la carga");	
    console.log('ERRROR');

	  console.log(err);
	  process.exit();
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
  .on('end', async () => {
    
    await xloader_log(xloader_id,"Culminando carga de dataset");	
    await  xloader_log(xloader_id,"Conteo de columnas y registros");	

    console.log("read done");

    let columnas = Object.keys(columns);

    console.log('columnas', columnas);
    for(var index in columnas) {
      if(columnas.hasOwnProperty(index)) {
        pool.query("SELECT datosabiertos.fn_xloader_diccionario($1, $2)", [xloader_id, columnas[index]]);
      }
    }

    await xloader_log(xloader_id,"Diccionario generado");	
    await xloader_fin([xloader_id, columnas.length, cantidad, 'csv']);
    await xloader_log(xloader_id,"Proceso culminado");	

    process.exit();

  });
});
}

//process.exit()

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

const pool = new pg.Pool({
  user: 'postgres',
  database: 'creainter',
  password: 'meteLPBDo0gmsc3d',
  port: 5432,
  host: '35.202.192.69',
});


const server = 'pc-01';
const app = express();


app.get('/api/xloader/start/:resource_id', async (req,res) => {

  const recurso_id = req.params.resource_id; 

  console.log("recurso  => " + recurso_id );
  var xloader = await xloader_puedo_iniciar(recurso_id);
  if(!xloader.iniciar) {
      return res.json({'status' : false, 'message': 'El recurso ya esta en proceso o tiene un proceso pendiente'});
  }
  xloader = await xloader_inicio(recurso_id);
  return res.json({'status' : true, 'message': 'Carga Programada', 'xloader_id' : xloader.xloader_id });
});

async function start_process(){
  var loader = await process_queue();

  if(loader.success) {
      console.log("Ejecuntado procesos en espera. => ", loader );
      var res = await pool.query("SELECT * FROM datosabiertos.fn_xloader_running($1,$2);",[ loader.xloader_id, server ]);
      if(res.rowCount != 1) {
          return false;
      }
      if(res.rows[0].ejecutar) {
        exec("node xloader.js submit " + loader.xloader_id + " " + loader.recurso_id, { maxBuffer: 1024 * 5000000 }, (error, stdout, stderr) => {
          if ( error ) {
              console.log(`error: ${error.message}`);
              return;
          }
          if ( stderr ) {
              console.log(`stderr: ${stderr}`);
              return;
          }
          console.log(`stdout: ${stdout}`);
          start_process();
        });
      }
  } else if(typeof loader.xloader_id !== 'undefined') {
      if(!pidIsRunning(loader.pid)) {
          console.log('Proceso Killed', loader);
          await pool.query("SELECT * FROM datosabiertos.fn_xloader_errado($1,$2);",[ loader.xloader_id, 'pid killed']);
      }
      console.log("Procesos en espera culminados");   
  }
}


setInterval(start_process, 2000);

async function process_queue(xloader_id = 0){

  var loaderQueue  = await pool.query("SELECT * FROM datosabiertos.fn_xloader_en_proceso($1);", [server]);

  if ( loaderQueue.rowCount == 1 ){
      var loader = loaderQueue.rows[0];
      if( loader.estado == 'procesando') {
        return { success: false, xloader_id: loader.id, recurso_id : loader.recurso_id };

      } else if ( loader.estado == 'pendiente' || xloader_id == loader.id) {
        return { success: true, xloader_id: loader.id, recurso_id : loader.recurso_id };     

      } else {
         return { success: false };
      }
  } else {
    return { success: false};
  }  
}

function pidIsRunning(pid) {
  try {
    process.kill(pid, 0);
    return true;
  } catch(e) {
    return false;
  }
}
app.get('/api/xloader/stop/:xloader', async (req,res) => {
  const xloader_id =  parseInt(req.params.xloader);
  await xloader_detener(xloader_id);

  const xloaderResult =  await pool.query("SELECT * FROM datosabiertos.xloader where id = $1",[xloader_id ]);
  console.log(xloaderResult)   
  if ( xloaderResult.rowCount == 1 ){

    const  xloader = xloaderResult.rows[0] ;
    	
    exec("sudo kill -9  " + xloader.pid, (error, stdout, stderr) => {

    if (error) {
        console.log(`error: ${error.message}`);
        return;
    }
    if (stderr) {
        console.log(`stderr: ${stderr}`);
        return;
    }
    console.log(`stdout: ${stdout}`);
  });

  }
  res.json({"status":true ,"message":"Proceso cancelado","xloader_id": xloader_id  })  
})

app.get('/api/xloader/status/:xloader_id' ,async (req,res) => {
  const xloader_id = req.params.xloader_id	
  const logResult = await pool.query("select * from datosabiertos.xloader_log where loader_id = $1 order by fecha desc",[ xloader_id  ])  
  const logs = logResult.rows;	
  res.json({ 'status':true, 'logs' : logs });
}); 

async function xloader_puedo_iniciar(recurso_id) {
  var xloader = await pool.query("SELECT * FROM datosabiertos.fn_xloader_puedo_iniciar($1)", [recurso_id]);
  return xloader.rows[0];
}
async function xloader_detener(xloader_id) {
  var xloader = await pool.query("SELECT * FROM datosabiertos.fn_xloader_detener($1)", [xloader_id]);
  return xloader.rows[0];
}
async function xloader_fin(params) {
  var xloader = await pool.query("SELECT * FROM datosabiertos.fn_xloader_fin($1, $2, $3, $4)", params);
  return xloader.rows[0];
}

app.listen(9080);
console.log( "LISTEN 9080" );

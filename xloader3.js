import csv from 'csv-parser';
import  Client  from 'ftp';
import readline from 'readline';
import process from 'node:process';
import fs from 'fs';
import {MongoClient} from 'mongodb';
import pg from 'pg';
import decompress from "decompress";
import {exec} from 'child_process';
import { PathExist, __rootDir, IsDir, EnumDir, ParseToHtml } from './utility.js';

const pool = new pg.Pool({
  user: 'postgres',
  database: 'creainter',
  password: 'meteLPBDo0gmsc3d',
  port: 5432,
  host: '35.202.192.69',
});


const ftp_client = new Client();

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

var xloader_id = 0;
var resource = {};
var item_credencial = {};
var xloader = {} ;
var columns = {};
var cantidad = 0;
var chunk = 0;
var docs = [];
const MAX_DISTINCT = 1000;
const MAX_ROWS     = 1000;


async function db_all(query, params) {
  var res = await pool.query(query, params);
  if(res.rowCount > 0) {
    return res.rows;
  }
  return null;
}
async function db_first(query, params) {
  var res = await pool.query(query, params);
  if(res.rowCount > 0) {
    return res.rows[0];
  }
  return null;
}
async function download_ftp(cfile, outfile) {
  ftp_client.on('ready', async function() {
    ftp_client.get(cfile.directorio + '/' + cfile.archivo, async(err, file) => {
      if( err ) {
        console.log(err);
        return;
      }
      console.log("Guardando Archivo");
     const ws = fs.createWriteStream(outfile, { encoding: 'utf8' });
     file.pipe(ws);
    });
  });
  ftp_client.connect( { host: cfile.host, port: cfile.puerto, user : cfile.usuario , password: cfile.clave });
}

async function fn_convertir_archivo_a_csv(recurso, cfile, outfile) {
    await fs.access(filepath_csv, fs.F_OK,async (err) => {
    if(cfile.formato == 'csv') {
        /* */
    } else if(cfile.formato == 'zip') {
        decompress(outfile, "./datasets/")
        .then((files) => {
          var file_name = "./datasets/" + files[0].path;
          filepath = "./datasets/" + hash + ".csv";
          fs.rename(file_name, filepath , async () => {
            await load(xloader_id,filepath, hash )
          })
        })
        .catch((error) => {
          console.log(error);
        });
    } else {
        return false;
    }
    });
}
async function fn_aterrizar_archivo(recurso, cfile) {
    var outfile = recurso.archivo_hash + '.' + cfile.formato;
    if(cfile.tipo == 'ftp') {
       await download_ftp(cfile, outfile);
    } else {
        return false;
    }
    return outfile;
}
async function xloader_inicio(recurso_id) {
  var xloader = await pool.query("SELECT * FROM datosabiertos.fn_xloader_inicio($1)", [recurso_id]);

  return xloader.rows[0];
}

async function xloader_fin(params) {
  var xloader = await pool.query("SELECT * FROM datosabiertos.fn_xloader_fin($1, $2, $3, $4)", params);
  process.exit();
}

async function xloader_log(xloader_id, message ){
   var log = await pool.query("insert into datosabiertos.xloader_log( loader_id, mensaje, fecha ) values( $1, $2, CURRENT_TIMESTAMP)", [xloader_id,message ]);
   return log.rows[0];
}

async function db_chunk(hash) {
  console.log("Guardando Bloque =>", chunk)
  await client.db('datosabiertos').collection(hash).insertMany(docs);
  docs = [];
  chunk = 0;
  return true;
}
async function fn_cargar_a_db( xloader_id, filepath, hash) {

  await xloader_log(xloader_id,"Inciando carga dataset");
  await xloader_log(xloader_id,"Cargando dataset...");
  console.log(xloader_id, filepath );

  db(async function(client) {
      let readStream = fs.createReadStream(filepath, {
        autoClose: true,
        encoding: 'utf8',
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
      .on('data',async function(line) {
        cantidad++;
        chunk++;
        for (var key in line) {
          if(line.hasOwnProperty(key)) {
            if(typeof columns[key] === 'undefined') {
              columns[key] = {
                selectable: false,
                data: {}
              };
            }
            if(columns[key].selectable) {
              if(typeof columns[key][line[key]] === 'undefined') {
                columns[key].data[line[key]] = 0;
              }
              if(columns[key].selectable) {
                columns[key].data[line[key]]++;
                if(Object.keys(columns[key].data) > MAX_DISTINCT) {
                  columns[key].selectable = false;
                }
              }
            }
          }
        }
        docs.push(line);
        if(chunk >= MAX_ROWS) {
         db_chunk(hash);
       }
  })
  .on('end', async () => {
      db_chunk(hash);

            console.log("Registros:" , cantidad);
            await xloader_log(xloader_id,"Culminando carga de dataset");
            await xloader_log(xloader_id,"Conteo de columnas y registros");

            console.log("read done");

            let columnas = Object.keys(columns);

            console.log('columnas', columnas);
            for(var index in columnas) {
              if(columnas.hasOwnProperty(index)) {
             var col =  columnas[index].replace("\"","" ).replace("\"","").trim() ;
                pool.query("SELECT datosabiertos.fn_xloader_diccionario($1, $2)", [xloader_id, col]);
              }
            }
            await xloader_log(xloader_id,"Diccionario generado");
            await xloader_log(xloader_id,"Proceso culminado");
            await xloader_fin([xloader_id, columnas.length, cantidad, 'csv']);

  });
});
}


if (action == 'submit') {

  xloader_id = parseInt(args[1]); 
  const resource_id = parseInt(args[2]); 

   console.log( 'PID = ' +   process.pid);
   console.log( 'PPID = ' +   process.ppid);


  const xloader = await db_first("SELECT * FROM datosabiertos.xloader where  id = $1", [xloader_id]);
  const recurso = await db_first("SELECT * FROM datosabiertos.recurso where  id = $1", [resource_id]);
  const cfile   = await db_first("SELECT CI.*,C.* FROM datosabiertos.credencial_item CI JOIN datosabiertos.credencial C on C.id = CI.credencial_id where CI.id = $1", [recurso.credencial_item]);
  console.log('R', xloader, recurso, cfile);

  var outfile = fn_aterrizar_archivo(cfile);

  fn_convertir_archivo_a_csv(recurso, cfile, outfile);

  fn_cargar_a_db(outfile);


} else if (action == 'stop') {
   pid = args[1]; 
   try {
    process.kill(pid, 0);
    await xloader_log(params.xloader_id, "Proceso ha sido cancelado");
    console.log( true) ; 
  } catch(e) {
    console.log(false) ;
  }
}


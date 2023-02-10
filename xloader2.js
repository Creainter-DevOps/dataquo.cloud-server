import csv from 'csv-parser';
import  Client  from 'ftp';
import readline from 'readline';
import process from 'node:process';
import fs from 'fs';
import {MongoClient} from 'mongodb';
import { createInterface } from "readline";
import { createReadStream } from "fs";
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


function readBytes(fd, buffer) {
    return new Promise((resolve, reject) => {
        fs.read(
            fd, 
            buffer,
            0,
            buffer.length,
            null,
            (err) => {
                if(err) { return reject(err); }
                resolve();
            }
        );
    });
}


console.log('pg', pg.Pool);

const args = process.argv.slice(2); 
console.log(args);

const action = args[0];

var  xloader_id = 0;
var  resource = {};
var  item_credencial = {};
var xloader = {} ;

if ( action == 'submit' ){

    xloader_id = parseInt(args[1]); 
    const resource_id = parseInt(args[2]); 

    console.log( 'PID = ' +   process.pid  );	
    console.log( 'PPID =  ' +   process.ppid  );	

    const xloaderResult =  await pool.query("SELECT * FROM datosabiertos.xloader where  id = $1",[xloader_id ]);

    if ( xloaderResult.rowCount == 1 ){

        xloader = xloaderResult.rows[0] ;

        const resourceResult = await pool.query("SELECT * FROM datosabiertos.recurso where  id = $1",[ resource_id ]);

        resource = resourceResult.rows[0] ;

        console.log(resource);	 

        const item_credencialResult =  await pool.query("SELECT CI.*,C.* FROM datosabiertos.credencial_item CI JOIN datosabiertos.credencial C on C.id = CI.credencial_id where CI.id = $1", [ resource.credencial_item ]);		  

        if(item_credencialResult.rowCount > 0 ){
            item_credencial = item_credencialResult.rows[0]
        }

        const hash = resource.archivo_hash;

        const extension = item_credencial.archivo.split(".")[1]; 	  
        const filename = hash + "." + extension;	   
        const filepath = "/var/www/repository/" + hash + "."+ extension ;
        const filepath_csv = "/var/www/repository/" + hash + ".csv" ;

        await pool.query("UPDATE datosabiertos.xloader SET pid = $1 , ppid = $2 where id = $3 ",[ process.pid, process.ppid , xloader_id ]); 

        await fs.access(filepath_csv, fs.F_OK,async (err) => {

            const filepath_ftp = item_credencial.directorio + '/' + item_credencial.archivo;
            var url_storage = resource.archivo;
            var archivo = url_storage.replace("https://datosabiertos.mef.gob.pe/storage", "https://storage.googleapis.com/mef-peru" )
            console.log(err);

            if (err && (extension == "zip" || extension == "csv" || extension == "CSV" ) ) {
                console.log("Descargando recurso");	  
                await download_ftp(archivo, hash ,xloader_id,filepath )		 
                //return
            }else{
                console.log("Procesando recurso");	  
                await load( xloader_id, filepath_csv, hash );	    
            }

            //file exists
        })	  

    }

    console.log( {'status' : true } );

}
if (action == 'stop' ){
    pid = args[1]; 
    try {
        process.kill(pid, 0);

        await xloader_log( params.xloader_id,"Proceso ha sido cancelado");	
        console.log( true) ; 
    } catch(e) {
        console.log(false) ;
    }
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

async function download_ftp(filepath_ftp,hash,xloader_id,filepath ){

    /*ftp_client.on('ready', async function() {

        var ext = item_credencial.archivo.split(".")[1]; 
        console.log(filepath_ftp,hash  )   
        ftp_client.get(filepath_ftp, async( err,file ) => {
            if( err ){
                console.log(err); 
                return;
            }
            console.log("Guardando Archivo");

            const ws = fs.createWriteStream(filepath , { encoding: 'utf8' });
            file.pipe(ws);
            await file.on('end',async ()=> {
                console.log('Archivo Guardado');
                if( ext  == "zip" ){
                    decompress(filepath, "./var.www.repository/")
                        .then((files) => {
                            var file_name = "./var.www.repository/" + files[0].path;
                            filepath = "./var.www.repository/" + hash + ".csv";   
                            fs.rename(file_name, filepath ,async () => {

                                await load(xloader_id,filepath, hash )
                            })  
                        })
                        .catch((error) => {
                            console.log(error);
                        })	           				
                }else if( ext == "csv"  ) {
                    await load(xloader_id,filepath, hash )
                }		

            } );
        })    
    });

    ftp_client.connect( { host: '200.60.146.34', port: 21, user : 'ftp-ugi03' , password: 'tKS#g*xYCq'});
    */
    console.log(filepath_ftp);
    exec("wget " + filepath_ftp , {maxBuffer: 1024 * 5000000 } , (err,stdout,sterr ) => {
        
        if(err){
            console.log(err)
            return;

        } 
            var file_name = filepath_ftp.split("/")[5] ;
            console.log("FILENAME", file_name )
            filepath = "/var/www/repository/" + hash + ".csv";   

            console.log("FILE PATH", filepath )

            fs.rename(file_name, filepath ,async ( err ) => {
                if(err){
                    console.log(err);
                    return ;
                }
                await load(xloader_id,filepath, hash )
            })  
    })
}

var columns = {};
var cantidad = 0;
var chunk = 0;


async function load( xloader_id, filepath, hash ){

    await xloader_log(xloader_id,"Inciando carga dataset");	

    await xloader_log(xloader_id,"Cargando dataset...");	
    console.log(xloader_id, filepath );

    db(async function(client) {

        var ccc = 0;

        var collect = await  client.db('datosabiertos').listCollections().toArray();

        var ext = collect.find((obj) => {
            return obj.name = hash
        })		

        if( ext.name === hash ){
            console.log("Existe => ", hash )
            await client.db('datosabiertos').collection(hash).drop( async(err,result)=> {
                if (err){
                    console.log("No se encontro la collecion", hash ); 
                };    
            });
        }
        exec("mongoimport --db datosabiertos --collection "+ hash + " --type csv --headerline --ignoreBlanks --file " + filepath ,async(error, stdout, sterr ) => {
            if(error){
                console.log(error);
                client.close();
                return;
            }
            console.log("stdout",stdout) ;
            var firsObj = await client.db("datosabiertos").collection(hash).findOne();
            console.log(firsObj )
            var filas = await client.db("datosabiertos").collection(hash).countDocuments();

            client.close();
            console.log(filas)
            var columnas = Object.keys(firsObj).length;
            await xloader_log(xloader_id,"Diccionario generado");	
            await xloader_log(xloader_id,"Proceso culminado");	
            await  xloader_fin([xloader_id, columnas , filas , 'CSV']);

        })

    });

}

async function process_queue(){

    var loaderQueue  = await pool.query("select * from datosabiertos.xloader where ( estado = 'pendiente' or  estado = 'procesando') and fecha_hasta is null  order by fecha_desde  asc  limit 1 ");

    if ( loaderQueue.rowCount == 1 ){

        var loader = loaderQueue.rows[0];
        console.log( loader  )
        if( loader.estado == 'procesando'){
            //return { success: e, xloader_id: loader.id, recurso_id : loader.recurso_id };
            return { success: false };
        } else if ( loaderQueue.estado == 'pendiente' || xloader_id == loader.id ){
            return { success: true, xloader_id: loaderQueue.id, recurso_id : loaderQueue.recurso_id };     
        } else {
            return { success: false };
        }
    } else {
        return { success: false};
    }  
}

async function start_process(){
    var loader = await process_queue();

    if(loader.success) {
        console.log("Ejecuntado procesos en espera. => ", loader );
        var res = await pool.query("SELECT * FROM datosabiertos.fn_xloader_running($1);",[ loader.xloader_id ]);
        if(res.rowCount != 1) {
            return false;
        }
        console.log(res);
        if(res.rows[0].ejecutar) {
            exec("node xloader3.js submit " + loader.xloader_id + " " + loader.recurso_id, { maxBuffer: 1024 * 5000000 }, (error, stdout, stderr) => {
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
  } else {
      console.log("Procesos en espera culminados");   
  }
}
//start_process();

//process.exit()

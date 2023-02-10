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
    const filepath = "./datasets/" + hash + "."+ extension ;
    const filepath_csv = "./datasets/" + hash + ".csv" ;

    await pool.query("UPDATE datosabiertos.xloader SET pid = $1 , ppid = $2 where id = $3 ",[ process.pid, process.ppid , xloader_id ]); 
   	
    await fs.access(filepath_csv, fs.F_OK,async (err) => {

      const filepath_ftp = item_credencial.directorio + '/' + item_credencial.archivo;
      console.log(err);

	  if (err && (extension == "zip" || extension == "csv" ) ) {
	        console.log("Descargando recurso");	  
            await download_ftp(filepath_ftp, hash ,xloader_id,filepath )		 
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
	
  ftp_client.on('ready', async function() {

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
			decompress(filepath, "./datasets/")
			  .then((files) => {
			       var file_name = "./datasets/" + files[0].path;
			    	filepath = "./datasets/" + hash + ".csv";   
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

}


var columns = {};
var cantidad = 0;
var chunk = 0;

async function load( xloader_id, filepath, hash ){
 
  await xloader_log(xloader_id,"Inciando carga dataset");	
  
  await xloader_log(xloader_id,"Cargando dataset...");	
  console.log(xloader_id, filepath );

const CHUNK_SIZE = 10000000; // 10MB
  db(async function(client) {

      var ccc = 0;
      var docs = [];
      let readStream = fs.createReadStream(filepath, {
        autoClose: true,
        encoding: 'utf8',
          highWaterMark: CHUNK_SIZE 

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
        for (var key in line) {
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
    
    //console.log(line);	  

     console.log(cantidad);
     docs.push(line);	  
    
     /*if( docs.length == 100 ){
         chunk++;
         console.log("Guardando Bloque =>", chunk   )
         await  client.db('datosabiertos').collection(hash).insertMany(docs);
         docs = [];
     }*/

  })
  .on('end', async () => {
	  
     var collect = await  client.db('datosabiertos').listCollections().toArray();
      console.log(collect);	  
      var ext = collect.find((obj) => {
      	return obj.name = hash
      })		

      if(ext){
        console.log("Existe",ext)
         await client.db('datosabiertos').collection(hash).drop( async(err,result)=> {
            if (err) throw err;    
            await  client.db('datosabiertos').collection(hash).insertMany(docs);
         });
      } else {
        await  client.db('datosabiertos').collection(hash).insertMany(docs);
      }
    
      try{
            var collect = await  client.db('datosabiertos').listCollections().toArray();

            var ext = collect.find((obj) => {
                return obj.name = hash
            })		

            if(ext.name === hash){
                console.log("Existe => ", hash )
                await client.db('datosabiertos').collection(hash).drop( async(err,result)=> {
                    if (err){
                       console.log("No se encontro la collecion", hash ); 
                    };    
                });
            }

            var inserts = await  client.db('datosabiertos').collection(hash).insertMany(docs);

            console.log("Registros:" , cantidad);
            await xloader_log(xloader_id,"Culminando carga de dataset");	
            await  xloader_log(xloader_id,"Conteo de columnas y registros");	

            console.log("read done");

            let columnas = Object.keys(columns);

            console.log('columnas', columnas);
            for(var index in columnas) {
              if(columnas.hasOwnProperty(index)) {
             var col =  columnas[index].replace("\"","" ).replace("\"","").trim() ;
                pool.query("SELECT datosabiertos.fn_xloader_diccionario($1, $2)", [xloader_id,col  ]);
                  
              }
            }

            await xloader_log(xloader_id,"Diccionario generado");	
            await xloader_log(xloader_id,"Proceso culminado");	
            await xloader_fin([xloader_id, columnas.length, cantidad, 'csv']);

      }catch(e){
        console.log(e);
      }
    //process.exit();

  });
});

}

//process.exit()

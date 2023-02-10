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


var  recurso_id = 0;
var  resource = {};
var  item_credencial = {};
var recurso = {} ;

async function download(){

var recursoQuery  = await process_queue();

    recurso  = recursoQuery[0];   
    recurso_id = recurso.id;

console.log(recurso);

    const item_credencialResult = await pool.query("SELECT CI.*,C.* FROM datosabiertos.credencial_item CI JOIN datosabiertos.credencial C on C.id = CI.credencial_id where CI.id = $1", [ recurso.credencial_item ]);		  

    if(item_credencialResult.rowCount > 0 ){
    	item_credencial = item_credencialResult.rows[0]
    }

    const hash = recurso.archivo_hash;

    const extension = item_credencial.archivo.split(".")[1]; 	  

    const filename = hash + "." + extension;	   

    const filepath = "/var/www/repository/" + hash + "."+ extension ;
    const filepath_csv = "/var/www/repository/" + hash + ".csv" ;

    await fs.access(filepath_csv, fs.F_OK,async (err) => {

      const filepath_ftp = item_credencial.directorio + '/' + item_credencial.archivo;

      console.log(err);

	  if (err && (extension == "zip" || extension == "csv" ) ) {
	        console.log("Descargando recurso");	  
            await download_ftp(filepath_ftp, hash ,recurso_id,filepath )		 
	    //return
	  }else{
	    console.log("Recurso Descargado");	  

        await pool.query("UPDATE datosabiertos.recurso set filas  = 0  where id = $1",[recurso_id ]);

        download();
        //await pool.query("UPDATE datosabiertos.recurso set estado = '--'  where id = $1",[recurso.id ]);
	  }
    })	

}

async function process_queue(recurso_id = 0){

  var loaderQueue  = await pool.query("select * from datosabiertos.recurso where credencial_item is not null  and filas is null and procesado_desde is null order by id asc ");
  return loaderQueue.rows; 
}

async function download_ftp(filepath_ftp,hash,recurso_id,filepath ){
	
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
			decompress(filepath, "/var/www/repository/")
			  .then((files) => {
			       var file_name = "/var/www/repository/" + files[0].path;
			    	filepath = "/var/www/repository/" + hash + ".csv";   
			        fs.rename(file_name, filepath ,async () => {
                    await pool.query("UPDATE datosabiertos.recurso set filas  = 0   where id = $1",[recurso_id ]);
                    download();
//	           		await load(recurso_id,filepath, hash )
			       })  
			  })
			  .catch((error) => {
			    console.log(error);
			  })	           				
		}else if( ext == "csv"  ) {
              await pool.query("UPDATE datosabiertos.recurso set filas = 0  where id = $1",[recurso_id ]);
            download();
	           //await load(recurso_id,filepath, hash )
		}		
	
	 } );
	})    
  });

  ftp_client.connect( { host: '200.60.146.34', port: 21, user : 'ftp-ugi03' , password: 'tKS#g*xYCq'});

}


setTimeout(download, 1000);

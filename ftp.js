import  express  from 'express' ;
import  Client  from 'ftp';

import pg from 'pg';

const pool = new pg.Pool({
  user: 'postgres',
  database: 'creainter',
  password: 'meteLPBDo0gmsc3d',
  port: 5432,
  host: '35.202.192.69',
});


console.log('pg', pg.Pool);

const app = express();
const ftp_client = new Client();

app.get('/ftp/refresh', async( req,res ) => {
  initFTP();  
  res.json({status:true});
})

function readFTP(dir, cb) {
    ftp_client.list(dir, false , async function(err,list ) {
      if(err) {
        console.log(err);
        return false;
      }

      for(var index in list) {
        if(list[index].type == 'd') {
	  readFTP(dir + '/' + list[index].name, cb);
	} else {
          list[index].directory = dir;
          cb(list[index]);
	}
      }
    });
}
function initFTP() {
  ftp_client.on('ready', async function() {
    await readFTP('/Automatico/DGPP/', function(f) {
      console.log('file', f.directory + '/' + f.name);
      save_file(f);
    });
  });
  ftp_client.connect( { host: '200.60.146.34', port: 21, user : 'ftp-ugi03' , password: 'tKS#g*xYCq', pasvTimeout : 20000 });
}
initFTP();

async function save_file(file) {
  var credencialResult = await pool.query("SELECT * from datosabiertos.credencial_item where directorio = $1 AND archivo = $2",[file.directory, file.name]);
  if( credencialResult.rowCount == 0) {
    await pool.query("INSERT INTO datosabiertos.credencial_item(archivo,directorio,tamano,created_on, updated_on ) values ($1,$2,$3,CURRENT_TIMESTAMP, CURRENT_TIMESTAMP )" ,[ file.name, file.directory, file.size]);
  }
}

app.listen(9080);

console.log("LISTEN 9080");

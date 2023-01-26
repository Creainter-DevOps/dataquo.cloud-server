import csv from 'csv-parser';
import readline from 'readline';
import express from 'express';
import fs from 'fs';
import http from 'http';
import https from 'https';
import { PathExist, __rootDir, IsDir, EnumDir, ParseToHtml } from './utility.js';
import {MongoClient} from 'mongodb';

console.log('CSV', csv);

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
const hash     = 'ef499bd1-891e-41aa-aa64-72b93a75dee7';

const credentials = {
  key: fs.readFileSync('/var/www/nginx/ssl/dataquo.cloud.key'),
  cert: fs.readFileSync('/var/www/nginx/ssl/dataquo.cloud.pem')
};

const httpApp = express();
httpApp.get('*', (req, res) => {
    res.redirect(`https://sorrow.live${req.baseUrl}`);
});

const app = express();


app.get('/mongodb', (req, res) => {
  res.status(200);
  res.setHeader("Content-Type", "application/json");
  res.write('[');
  db(function(client) {
    var stream = client
      .db('datosabiertos')
      .collection(hash)
      .find()
      .stream();
    stream.on('data', function(doc) {
      delete doc['_id'];
      res.write(JSON.stringify(doc) + ',');
    });
    stream.on('end', function() {
      res.write(']');
      res.send();
    });
  });
});
app.post('/api/3/action/datastore_search', (req, res) => {
  res.status(200).download('response.json');
});

app.get('/api/3/action/datastore_search2', (req, res) => {
  res.set({ 'content-type': 'application/json; charset=utf-8' });
  res.status(200).write('{"data":[');
  const filepath = "datasets/5a24370e-da9f-4519-87e7-a9565c08670f.csv";
  //datasets/Directorio_Invierte_diccionario.csv"

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
    columns: false, bom: true, trim: true,
    headers: true,
    skipLines: 50000
  }))
  .on('data', function(line) {
    console.log('DATA', line);
    res.write(JSON.stringify(line)+",");
    ccc++;
    if(ccc > 1) {
      console.log('CANCELAR');
      res.write("]}");
      res.send();
      readStream.end();
      readStream.destroy();
    }
  })
  .on('end', () => {
    console.log("read done");
    res.write("]}");
    res.send();
  })
});


app.get('*', (req, res) => {

    if (req.originalUrl.includes('/..')){
        res.status(403).send('FORBIDDEN');
        return;
    }
    if (!PathExist(req.originalUrl))
    {
        res.contentType('text/html');
        res.status(404).send("ERROR 404");
        return;
    }
    if (!IsDir(req.originalUrl))
    {
        res.status(200).sendFile(__rootDir + req.originalUrl);
        return;
    }
    res.send(ParseToHtml(EnumDir(req.originalUrl), req.originalUrl));
});



const httpServer = http.createServer(httpApp);
const httpsServer = https.createServer(app, credentials);

if (process.env.DEBUG || true) {
  console.log('MODO DEBUG');
  app.listen(8080);
} 
else
{
    console.log('MODO PRODUCCION');
    httpsServer.listen(443);
    httpServer.listen(80);
}
    

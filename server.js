import csv from 'csv-parser';
import readline from 'readline';
import express from 'express';
import bodyParser from 'body-parser';
import fs from 'fs';
import http from 'http';
import https from 'https';
import { AsyncParser } from '@json2csv/node';
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

const httpApp = express();
httpApp.get('*', (req, res) => {
    res.redirect(`https://sorrow.live${req.baseUrl}`);
});

const app = express();

var jsonparser = bodyParser.urlencoded({ extended: false }) 
//var jsonparser = bodyParser.json() 
    
app.set('view engine', 'ejs');
app.set('views', __rootDir);

app.get('/resource/:rid', function(req, res) {
  res.render('index', { resource_id: req.params.rid});
});


function sorts_db(sort){

   var f = {};        
    
   var values = Object.values(filters);

   Object.keys(filters).forEach( (key,index ) =>{
     f[key] = new RegExp(".*" + values[index] + ".*" ); 
   })
   return f; 
}


function filters_db(filters){

   var f = {};        

   var values = Object.values(filters);

   Object.keys(filters).forEach( (key,index ) =>{
     f[key] = new RegExp(".*" + values[index] + ".*" ); 
   })
   return f; 

}
app.get('/datastore/dump/:hash',async (req,res ) => {
  
  var filters = {};

  const hash = req.params.hash;  
  if(req.query.filters){
     var js = JSON.parse(req.query.filters);
    filters = await filters_db(js); 
  }    
  console.log("filters", filters );
  var registros = [];
  db(function(client) {
    var stream = client
      .db('datosabiertos')
      .collection(hash)
      .find( filters)
      .stream();
    stream.on('data', function(doc) {
      delete doc['_id'];
      registros.push( doc );	    
    });
    stream.on('err', function(err ){
        console.log(err);	
    })	  
    stream.on('end', async function() {
        if(registros.length === 0 ){
	        return;
        } else {
            console.log(registros);
            const opts = { withBOM:true};
            const transformOpts = {};
            const asyncOpts = {};
            const parser = new AsyncParser(opts, transformOpts, asyncOpts);
            const csv = await parser.parse(registros).promise();

            res.end(csv);
            //console.log(csv);
    }      
  })  
  })
})

app.post('/api/4/action/datastore_search',jsonparser,  (req, res) => {

   var body = {};	
    var filters = {};
    var sort = {};
    var offset = 0;
   Object.keys( req.body).forEach((item)=> {
   	body = JSON.parse(item);
   } ) ;  
  var hash  = body.resource_id;	
  console.log(body );
   if(body.filters){
     //var filters_d = filters_db( body.filters );
     //console.log(filters_d)
     filters = filters_db( body.filters );
     console.log(filters);
   } 

   if ( body.offset ) {
    offset = body.offset;
   }

   if(body.sort){
     var order = body.sort.split(" ");  
     sort[order[0]] = order[1] == "asc" ? 1 : -1 ;
   }

  let response = {
  	help:'https://mef.dataquo.cloud/api/3/action/help_show?name=datastore_search',
	result : {
		filters: filters,
		fields: {},
		include_total:true,
		limit:1000,
		offset: offset,
		records: [],
		recors_format: "objects",
		total: 0,
		q: "",
		total_estimation_threshold: 1000,
		total_was_estimated:false,
		links: {
			next: "/api/4/action/datastore_search?offset=1000",
			start: "/api/4/action/datastore_search"
		}
	},
	success:true   
  };  	
	
  db(function(client) {
    var stream = client
      .db('datosabiertos')
      .collection(hash)
      .find( filters)
      .sort(sort)
      .limit(1000)	  
      .skip(offset)
      .stream();
    stream.on('data', function(doc) {
      delete doc['_id'];
      response.result.records.push( doc );	    
    });
    stream.on('err', function(err ){
        console.log(err);	
    })	  
    stream.on('end', async function() {
        if(response.result.records.length === 0 ){
      	    response.success = false;
            res.status(200).json(response);	    
	        return;
        } else {
            response.result.total = await  client.db("datosabiertos").collection(hash).countDocuments(filters )

	        response.result.fields = Object.keys( response.result.records[0]).map( (item) => ({
	            id: item,
	            type:'text'      
	        })); 		
	        res.status(200).json(response);	    
        } 	    
    });
  });
});




/*app.post('/api/4/action/datastore_search', (req, res) => {
  res.status(200).download('response.json');
});*/
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
  	res.json({'status': true } ); 	
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


if (process.env.DEBUG || true) {
  console.log('MODO DEBUG');
  app.listen(7090);
} 

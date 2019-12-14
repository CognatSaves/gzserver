const express = require('express');
const app = express();
const bodyParser = require('body-parser');
const fs = require('fs'); 
//const multer = require('multer');
const port = 3000;


const MongoClient = require("mongodb").MongoClient;
const mongoClient = new MongoClient("mongodb://localhost:27017/", {useNewUrlParser: true});


let dbClient;

mongoClient.connect(function(err,client){

        // const db = client.db('archivedb');
        // const collection = db.collection('archivecoll');
        // collection.insertOne(archive,function(err,result){
        //     if(err){
        //         return console.log(err);
        //     }
        //     client.close();
        // }) 
        if(err) throw err;
        dbClient = client;
        app.locals.archivecoll = client.db('archivedb').collection('archivecoll');
        
        app.listen(port, (err)=>{
            if(err){
                return console.log('Error ', err);
            }
            console.log(`Server is listening on ${port}`);
        })
});



let temporaryArchiveSave = [];

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({extended: false}));
//app.use(multer({dest:"uploads"}).single('archive'));
const jsonParser = express.json();
var multipart = require('connect-multiparty');
var multipartMiddleware = multipart();
app.get('/', (req,res)=>{
    res.send('Get request');
})

app.post('/upload', multipartMiddleware, (req,res)=>{
    console.log('call upload');
    //console.log(req.body);
    //console.log(req.files);
    let archive = req.files;
    //console.log(archive);
    

    try{
        let originalFilename = req.files.archive.originalFilename;
        let filenameParts = originalFilename.toString().split('.');
        let type = filenameParts[filenameParts.length-1];
        let name = './uploads/file'+Date.now()+'.'+type;
        console.log('try to open');
        fs.open(req.files.archive.path, 'r', function(err, fdRead){
            console.log('open first')
            
                fs.fstat(fdRead, function(err,stats){
                    let bufferSize = stats.size;
                    let chunkSize = 512;
                    let buffer = new Buffer(bufferSize);
                    let bytesRead = 0;
                    console.log('readStart');
                    let i=0;
                    while(bytesRead < bufferSize){
                        if((bytesRead + chunkSize) > bufferSize){
                            chunkSize = (bufferSize - bytesRead);
                        }
                        fs.read(fdRead, buffer, bytesRead, chunkSize, bytesRead,
                            ()=>{});   
                        bytesRead +=chunkSize;
                        if(i*10000000<bytesRead){
                            //console.log(i+"&&&&");
                            i++;
                        }
                        
                    }
                    //console.log(buffer.toString('utf8',0, 500));
                    fs.close(fdRead,()=>{});


                    fs.open(name,'w',function(err,fdWrite){
                        //console.log(stats, buffer);
                        let bufferSize = stats.size;
                        let chunkSize = 512;
                        let bytesWrite = 0;
                        let j=0;
                        while(bytesWrite < bufferSize){
                            if((bytesWrite + chunkSize) > bufferSize){
                                chunkSize = (bufferSize - bytesWrite);
                            }
                            fs.write(fdWrite,buffer, bytesWrite, chunkSize, bytesWrite, ()=>{});
                            bytesWrite +=chunkSize;
                            if(j*10000000<bytesWrite){
                                //console.log(j+'****');
                                j++;
                            }
                        }
                        fs.close(fdWrite,()=>{});
                        buffer=""; 
                        console.log('create archive');
                        let archive = {
                            title: req.body.title,
                            description: req.body.description,
                            expire: req.body.expire,
                            archive: name
                        }
                        const collection = req.app.locals.archivecoll;
                        collection.insertOne(archive,function(err,result){
                            if(err){
                                console.log('some error in mongo');
                                return console.log(err);
                            }
                            console.log('send answer');
                            res.send(archive);
                        })
                        
                    })
                    
                })  
        })
    }
    catch(error){
        console.log('some error');
        console.log(error);
        res.send(error);
    }
    
})

app.get('/archivesListPage', (req,res)=>{
    console.log(req.query);
    let page = parseInt(req.query.page);
    let step = parseInt(req.query.step);
    if(!page){
        page=1;
    }
    if(!step){
        step=10;
    }
    const collection = req.app.locals.archivecoll;
    //collection.count().then((value)=>{console.log('all count='+value)})
    collection.find({}).limit(step).skip(step*(page-1))
    .toArray(function(err, result){
        console.log(result);
        res.send(result);
    });
})

process.on("SIGINT", () => {
    dbClient.close();
    process.exit();
});
// app.get('/archivesList',)

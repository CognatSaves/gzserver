const express = require('express');
const app = express();
const bodyParser = require('body-parser');
const fs = require('fs'); 
//const multer = require('multer');
//const gunzip = require('gunzip-file');
const zlib = require('zlib');
// const {gzip, ungzip} = require('node-gzip');
// const readline = require('linebyline'); 
// const stream = require('stream');
// const byline = require('byline');
const port = 3000;


const MongoClient = require("mongodb").MongoClient;
const mongoClient = new MongoClient("mongodb://localhost:27017/", {useNewUrlParser: true});

const ObjectID = require('mongodb').ObjectID;
let dbClient;


let nextArchiveToDelete = '';//should be = _id
let deleteTimeoutId = '';
let nextDeleteExpiresDate = null;//should be Date object

function resetArchiveDeleteTimeout(object){
    if(!object){
        //any problem - call basic cycle function
        deleteArchiveCycleStarter();
        return;
    }
    console.log('resetArchiveDeleteTimeout');
    let now = new Date();
    let expiresDate = new Date(object.expire);
    nextDeleteExpiresDate=expiresDate;
    nextArchiveToDelete = object._id;
    let delta = nextDeleteExpiresDate-now;
    let standart = true;
    //next one is 
    if(delta>2000000000){//i know that integer = 2 147 483 647 but set 2kkk 
        delta=2000000000;
        standart=false;
    }
    console.log('is standart call='+standart);
    console.log('delta=',delta);
    setTimeout(()=>{ 
        nextArchiveToDelete='';
        nextDeleteExpiresDate=null;
        standart ? deleteOneArchive(nextArchiveToDelete, true) : deleteArchiveCycleStarter();
    }, Math.max(delta,0));
}
function deleteOneArchive(_id, callStarter){
    console.log('call deleteOneArchive');
    const collection = app.locals.archivecoll;
    collection.find({_id: ObjectID(_id)}).toArray(function(err,result){
        if(err) throw err;
        if(result.length===0) {deleteArchiveCycleStarter(); return};//if array is empty - recall stard cycle func
        let obj = result[0];
        let fileAddress = obj.archive;
        fs.unlink(fileAddress, (err) => {
            if (err) {console.log('Error in file deleting, continue. Error=',err);}
        });
        
        collection.deleteOne({_id: ObjectID(_id)});
        if(callStarter){
            deleteArchiveCycleStarter();
        }
        return;
    })
}
function deleteArchiveCycleStarter(){
    console.log('deleteArchiveCycleStarter');

    const collection = app.locals.archivecoll;
    //console.log('collection=',collection);
    collection.find({}).toArray(function(err,result){
        let now = new Date();let nextDeleteOne=null;let objectsToRemove = [];
        for(let i=0; i<result.length; i++){
            let expireDate = new Date(result[i].expire);
            if(expireDate<now){
                objectsToRemove.push(result[i]._id);
                //console.log('kill');
            }
            else{
                if(!nextDeleteExpiresDate){
                    nextDeleteExpiresDate = expireDate;
                    //nextArchiveToDelete = result[i]._id;
                    nextDeleteOne=result[i];
                }
                else{
                    if(nextDeleteExpiresDate>expireDate){
                        nextDeleteExpiresDate = expireDate;
                        //nextArchiveToDelete = result[i]._id;
                        nextDeleteOne=result[i];
                    }
                }
            }
        }

        console.log('lastBreathe='+nextDeleteExpiresDate);
        console.log('objectsToRemove.length='+objectsToRemove.length);
        for(let i=0; i<objectsToRemove.length; i++){
            deleteOneArchive(objectsToRemove[i]);
        }
        if(nextDeleteOne){
            resetArchiveDeleteTimeout(nextDeleteOne);
        }
        
    })
}
mongoClient.connect(function(err,client){

        if(err) throw err;
        dbClient = client;
        app.locals.archivecoll = client.db('archivedb').collection('archivecoll');
        
        app.listen(port, (err)=>{
            if(err){
                return console.log('Error ', err);
            }
            console.log(`Server is listening on ${port}`);
            deleteArchiveCycleStarter();
        })
});



let temporaryArchiveSave = [];

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({extended: false}));

var multipart = require('connect-multiparty');
var multipartMiddleware = multipart();
app.get('/', (req,res)=>{
    res.send('Get request');
})

app.post('/upload', multipartMiddleware, (req,res)=>{
    console.log('call upload');
    try{
        let originalFilename = req.files.archive.originalFilename;
        let filenameParts = originalFilename.toString().split('.');
        let type = filenameParts[filenameParts.length-1];
        let name = './uploads/'+originalFilename;
        console.log('try to open');
        // console.log(originalFilename);
        fs.open(req.files.archive.path, 'r', function(err, fdRead){
            console.log('open first')
            
                fs.fstat(fdRead, function(err,stats){
                    let bufferSize = stats.size;
                    let chunkSize = 512;
                    let buffer = new Buffer(bufferSize);
                    let bytesRead = 0;
                    console.log('readStart');
                    while(bytesRead < bufferSize){
                        if((bytesRead + chunkSize) > bufferSize){
                            chunkSize = (bufferSize - bytesRead);
                        }
                        fs.read(fdRead, buffer, bytesRead, chunkSize, bytesRead,
                            ()=>{});   
                        bytesRead +=chunkSize;            
                    }
                    fs.close(fdRead,()=>{});
                    fs.open(name,'w',function(err,fdWrite){
                        let bufferSize = stats.size;
                        let chunkSize = 512;
                        let bytesWrite = 0;
                        while(bytesWrite < bufferSize){
                            if((bytesWrite + chunkSize) > bufferSize){
                                chunkSize = (bufferSize - bytesWrite);
                            }
                            fs.write(fdWrite,buffer, bytesWrite, chunkSize, bytesWrite, ()=>{});
                            bytesWrite +=chunkSize;
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

                            let expiresDate = new Date(archive.expire);
                            if(!nextDeleteExpiresDate){
                                resetArchiveDeleteTimeout(archive)
                            }
                            else{
                                if(expiresDate<nextDeleteExpiresDate){
                                    resetArchiveDeleteTimeout(archive)
                                }
                            }                           
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
    console.log('archivesListPage call');
    let page = parseInt(req.query.page);
    let step = parseInt(req.query.step);
    if(!page){
        page=1;
    }
    if(!step){
        step=10;
    }
    const collection = req.app.locals.archivecoll;
    collection.find({}).limit(step).skip(step*(page-1))
    .toArray(function(err, result){
        res.send(result);
    });
})

app.get('/archivesList', (req,res)=>{
    console.log('archivesList call');
    let step = parseInt(req.query.step);
    if(!step || step<1){
        step=10;
    }
    const collection = req.app.locals.archivecoll;
    collection.find({}).toArray(function(err,result){
        let resArray = [];
        let index = 0; let selectedSubArray=0; let numberOfElements=0;
        while(index!==result.length){
            if(numberOfElements===0){
                resArray[selectedSubArray]=[];
            }
            resArray[selectedSubArray].push(result[index]);
            index++;numberOfElements++;
            if(numberOfElements===step){
                numberOfElements=0;
                selectedSubArray++;
            }
        }
        return res.send(resArray);
    })
})

app.get('/archiveFileLines', (req,res)=>{
    console.log('archiveFileLines call');
    let _id = req.query._id;
    let lineNumber = parseInt(req.query.lineNumber);
    if(!_id){
        return res.sendStatus(400);
    }
    if(!lineNumber){
        lineNumber=1;
    }
    const collection = req.app.locals.archivecoll;
    collection.find({_id: ObjectID(_id)}).toArray(function(err,result){
        if(result.length>0){
            let archiveAddress =result[0].archive; 
            let readStream = fs.createReadStream(archiveAddress).pipe(zlib.createGunzip());

            getLines(readStream,lineNumber, (error, lines)=>{
                if(!error){
                    console.log(lines);
                    return res.send({
                        lines:lines
                    });
                }
                else{
                    //console.log(error);
                    return res.sendStatus(500);
                }
            })
        }
        else{
            return res.sendStatus(400);
        }
    })
})
process.on("SIGINT", () => {
    dbClient.close();
    process.exit();
});


function getLines(stream, lineNumber, callback) {
    console.log('getLines call');
    var fileData = '';
    stream.on('data', function(data){
        fileData += data;

        // The next lines should be improved
        var lines = fileData.split(/\r\n|\r|\n/);//\n\\\r

        if(lines.length >= +lineNumber){
        stream.destroy();
        callback(null, lines.slice(0,lineNumber));
        }
    });

    stream.on('error', function(){
      callback('Error', null);
    });

    stream.on('end', function(){
      callback('File end reached without finding line', null);
    });

}


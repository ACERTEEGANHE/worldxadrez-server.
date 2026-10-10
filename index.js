import express from 'express';
import http from 'node:http';
import cors from 'cors';
import { Server } from 'socket.io';
import { Chess } from 'chess.js';

const PORT = Number(process.env.PORT || 3001);
const CLIENT_ORIGINS = (process.env.CLIENT_ORIGINS || 'http://localhost:5173').split(',').map(s=>s.trim()).filter(Boolean);
const originAllowed = origin => !origin || CLIENT_ORIGINS.includes('*') || CLIENT_ORIGINS.includes(origin);
const eventWindows = new Map();
function rateAllowed(socket, limit=30, windowMs=10000) {
  const now=Date.now(), entry=eventWindows.get(socket.id) || {start:now,count:0};
  if(now-entry.start>windowMs){entry.start=now;entry.count=0;}
  entry.count++;eventWindows.set(socket.id,entry);
  return entry.count<=limit;
}
const app = express();
app.use(cors({origin(origin,callback){callback(null,originAllowed(origin));}}));
app.use(express.json({limit:'16kb'}));
app.get('/', (_req,res)=>res.json({name:'WorldXadrez Game Server',status:'online',protocol:'socket.io',version:'1.0.0'}));
app.get('/health', (_req,res)=>res.json({ok:true,rooms:rooms.size,uptime:Math.round(process.uptime())}));
const server = http.createServer(app);
const io = new Server(server,{cors:{origin(origin,callback){callback(null,originAllowed(origin));},methods:['GET','POST']},pingInterval:25000,pingTimeout:20000, maxHttpBufferSize:16_384, connectTimeout:10000});
const rooms = new Map();
const waitingPlayers = [];
const ROOM_TTL_MS = 6 * 60 * 60 * 1000;
setInterval(()=>{const now=Date.now();for(const [code,room] of rooms){if(room.players.length===0 && now-room.updatedAt>ROOM_TTL_MS)rooms.delete(code);}},60_000).unref();

function cleanCode(value){return String(value||'').toUpperCase().replace(/[^A-Z0-9]/g,'').slice(0,8);}
function roomSummary(room){return {roomCode:room.code,fen:room.game.fen(),players:room.players.length};}
function emitError(socket,message){socket.emit('room:error',{message});}
function leaveCurrentRoom(socket){
  const code=socket.data.roomCode;if(!code)return;
  const room=rooms.get(code);socket.leave(code);socket.data.roomCode=null;socket.data.color=null;
  if(room){room.players=room.players.filter(p=>p.id!==socket.id);room.updatedAt=Date.now();if(room.players.length===0)rooms.delete(code);else{io.to(code).emit('room:player-left');io.to(code).emit('game:ready',{fen:room.game.fen()});}}
}
function enterRoom(socket,room,color){
  socket.join(room.code);socket.data.roomCode=room.code;socket.data.color=color;
  room.players.push({id:socket.id,color});room.updatedAt=Date.now();
}
io.on('connection',socket=>{
  socket.onAny((event)=>{if(!rateAllowed(socket,event==='chat:message'?12:60,event==='chat:message'?10000:10000)){socket.emit('room:error',{message:'Muitas ações em pouco tempo. Aguarde alguns segundos.'});socket.disconnect(true);}});
  socket.emit('server:hello',{id:socket.id,rooms:rooms.size});
  socket.on('room:create',({roomCode}={})=>{
    leaveCurrentRoom(socket);let code=cleanCode(roomCode);
    if(!code)code='WX'+Math.random().toString(36).slice(2,6).toUpperCase();
    if(rooms.has(code)){emitError(socket,'Esse código já está em uso. Gere outro.');return;}
    const room={code,game:new Chess(),players:[],updatedAt:Date.now()};rooms.set(code,room);enterRoom(socket,room,'w');
    socket.emit('room:created',{roomCode:code,color:'w',fen:room.game.fen()});
  });
  socket.on('room:join',({roomCode}={})=>{
    const code=cleanCode(roomCode),room=rooms.get(code);
    if(!room){emitError(socket,'Sala não encontrada. Confira o código.');return;}
    if(room.players.length>=2){emitError(socket,'Essa sala já está cheia.');return;}
    if(room.players.some(p=>p.id===socket.id)){socket.emit('room:joined',{roomCode:code,color:socket.data.color,fen:room.game.fen()});return;}
    leaveCurrentRoom(socket);const color='b';enterRoom(socket,room,color);
    socket.emit('room:joined',{roomCode:code,color,fen:room.game.fen()});
    io.to(code).emit('game:ready',{fen:room.game.fen(),players:room.players.length});
  });
  socket.on('match:quick',()=>{
    const existing=waitingPlayers.find(p=>p.id!==socket.id&&p.connected);
    if(existing){
      waitingPlayers.splice(waitingPlayers.indexOf(existing),1);
      leaveCurrentRoom(existing);
      const code='Q'+Math.random().toString(36).slice(2,7).toUpperCase();
      const room={code,game:new Chess(),players:[],updatedAt:Date.now()};rooms.set(code,room);
      enterRoom(existing,room,'w');enterRoom(socket,room,'b');
      existing.emit('room:joined',{roomCode:code,color:'w',fen:room.game.fen()});
      socket.emit('room:joined',{roomCode:code,color:'b',fen:room.game.fen()});
      io.to(code).emit('game:ready',{fen:room.game.fen(),players:2});
    }else{
      if(!waitingPlayers.some(p=>p.id===socket.id))waitingPlayers.push(socket);
      socket.emit('match:waiting');
    }
  });
  socket.on('game:move',({roomCode,move}={})=>{
    const room=rooms.get(cleanCode(roomCode||socket.data.roomCode));
    if(!room||socket.data.roomCode!==room.code){emitError(socket,'Você não está nessa sala.');return;}
    if(room.players.length!==2){emitError(socket,'Aguardando o segundo jogador.');return;}
    if(socket.data.color!==room.game.turn()){emitError(socket,'Não é sua vez.');return;}
    try{
      const applied=room.game.move({from:move?.from,to:move?.to,promotion:move?.promotion||'q'});
      if(!applied){emitError(socket,'Movimento inválido.');return;}
      room.updatedAt=Date.now();
    io.to(room.code).emit('game:move',{fen:room.game.fen(),lastMove:applied.san,gameOver:room.game.isGameOver(),status:room.game.isCheckmate()?'checkmate':room.game.isDraw()?'draw':room.game.isCheck()?'check':'playing'});
    }catch{emitError(socket,'Movimento inválido.');}
  });
  socket.on('chat:message',({roomCode,message}={})=>{
    const room=rooms.get(cleanCode(roomCode||socket.data.roomCode));
    if(!room||socket.data.roomCode!==room.code)return;
    const safe=String(message||'').replace(/[\u0000-\u001F\u007F]/g,' ').trim().slice(0,180);if(!safe)return;
    io.to(room.code).emit('chat:message',{name:socket.data.color==='w'?'Brancas':'Pretas',message:safe,at:Date.now()});
  });
  socket.on('disconnect',()=>{
    eventWindows.delete(socket.id);
    const i=waitingPlayers.findIndex(p=>p.id===socket.id);if(i>=0)waitingPlayers.splice(i,1);
    leaveCurrentRoom(socket);
  });
});
server.listen(PORT,'0.0.0.0',()=>console.log(`WorldXadrez server listening on ${PORT}`));

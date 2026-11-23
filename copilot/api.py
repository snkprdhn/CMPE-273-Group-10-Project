"""Independent authenticated API; grid services never depend on this process."""
import asyncio
import json
from contextlib import asynccontextmanager
from uuid import uuid4
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse
from jsonschema import ValidationError
from identity.auth import Denied, require
from copilot.contracts import validate


def create_app(service, identity):
    @asynccontextmanager
    async def lifespan(app):
        yield
        tasks=list(service.tasks.values())
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        service.db.close()
    app=FastAPI(docs_url=None,redoc_url=None,openapi_url=None,lifespan=lifespan)

    @app.middleware('http')
    async def guard(request, next):
        try:
            response=await next(request)
        except Exception:
            response=JSONResponse({'error':dict(code='DEPENDENCY_UNAVAILABLE',message='Investigator unavailable',request_id=uuid4().hex,details=[])},status_code=503)
        response.headers['Cache-Control']='no-store'
        return response

    @app.exception_handler(Exception)
    @app.exception_handler(Denied)
    @app.exception_handler(ValueError)
    @app.exception_handler(ValidationError)
    @app.exception_handler(PermissionError)
    @app.exception_handler(LookupError)
    @app.exception_handler(BlockingIOError)
    @app.exception_handler(FileExistsError)
    async def error(request, exc):
        status,code=400,'INVALID_ARGUMENT'
        if isinstance(exc,Denied): status,code=exc.status,'UNAUTHENTICATED' if exc.status==401 else 'FORBIDDEN'
        elif isinstance(exc,PermissionError): status,code=403,'FORBIDDEN'
        elif isinstance(exc,LookupError): status,code=404,'NOT_FOUND'
        elif isinstance(exc,BlockingIOError): status,code=429,'RATE_LIMITED'
        elif isinstance(exc,FileExistsError): status,code=409,'CONFLICT'
        return JSONResponse({'error':dict(code=code,message='Investigation request rejected: '+code,request_id=uuid4().hex,details=[])},status_code=status)

    def actor(request):
        header=request.headers.get('authorization','')
        token=header[7:] if header.startswith('Bearer ') else ''
        return require(identity.authenticate(token),'Operator'),token

    @app.get('/healthz')
    def health():
        return {'status':'ok'}

    @app.post('/api/v1/investigations',status_code=202)
    async def start(request:Request):
        who,token=actor(request)
        raw=bytearray()
        async for chunk in request.stream():
            raw.extend(chunk)
            if len(raw)>8192: raise ValueError('Request too large')
        try:
            body=json.loads(raw)
            id=service.start(who,token,body,request.headers.get('idempotency-key'))
        except RecursionError as exc:
            raise ValueError('Nested input') from exc
        return dict(id=id,status='QUEUED')

    @app.get('/api/v1/investigations/{id}')
    async def get(id:str,request:Request):
        validate('Id',id)
        who,_=actor(request)
        return service.get(who,id)

    @app.get('/api/v1/investigations/{id}/events')
    async def events(id:str,request:Request):
        who,_=actor(request)
        cursor=request.headers.get('last-event-id')
        initial=service.events(who,id,cursor)
        async def stream():
            nonlocal cursor,initial
            heartbeat=asyncio.get_running_loop().time()
            while not await request.is_disconnected():
                if __import__('time').time() >= who.get('expires_at',float('inf')):
                    return
                for event in initial:
                    yield 'id: '+event['cursor']+'\nevent: '+event['event_type']+'\ndata: '+json.dumps(event)+'\n\n'
                    cursor=event['cursor']
                    if event['event_type']=='snapshot.required':
                        return
                if service.get(who,id)['status'] not in ('QUEUED','RUNNING'):
                    return
                if asyncio.get_running_loop().time()-heartbeat>=15:
                    yield ': heartbeat\n\n'
                    heartbeat=asyncio.get_running_loop().time()
                await asyncio.sleep(.2)
                initial=service.events(who,id,cursor)
        return StreamingResponse(stream(),media_type='text/event-stream')

    @app.post('/api/v1/investigations/{id}/cancel',status_code=202)
    async def cancel(id:str,request:Request):
        validate('Id',request.headers.get('idempotency-key'))
        who,_=actor(request)
        service.cancel(who,id)
        return dict(id=id,status='CANCELLED')
    return app
